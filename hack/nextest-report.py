#!/usr/bin/env python3
"""Parse nextest libtest-json output into structured reports.

Reads one or more libtest-json JSONL files (produced by nextest with
--message-format libtest-json), produces:

  - Human-readable markdown (printed to stdout)
  - Machine-parseable JSON (written to a file if --output-json is given)

Expected JSONL line shapes (nextest libtest-json):
  - {"type":"suite_start","num_tests":N}
  - {"type":"test_suite_start","root":"crate_name"}
  - {"type":"test","name":"test_name","status":"passed","stdout":"..."}
  - {"type":"test","name":"test_name","status":"failed","stdout":"...","stdout_data":"..."}
  - {"type":"test","name":"test_name","status":"ignored"}
  - {"type":"test","name":"test_name","status":"errored","stdout":"...","stderr":"..."}
  - {"type":"suite_end","status":"success"|"failure"}

This script handles both shapes.
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone


def parse_jsonl_files(paths):
    """Parse multiple JSONL files into a flat list of test events."""
    tests = []
    suite_start = None

    for path in paths:
        with open(path, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if event.get("type") == "suite_start":
                    suite_start = event
                elif event.get("type") == "test":
                    # Normalize: add root crate if not present
                    if "root" not in event:
                        event["root"] = "may_minihttp"
                    tests.append(event)
                elif event.get("type") == "suite_end":
                    pass

    return tests, suite_start


def analyze_tests(tests):
    """Analyze test events into structured summary."""
    summary = {
        "total": len(tests),
        "passed": 0,
        "failed": 0,
        "errored": 0,
        "ignored": 0,
        "skipped": 0,
        "by_crate": defaultdict(lambda: {"passed": 0, "failed": 0, "errored": 0, "ignored": 0, "skipped": 0}),
        "by_status": defaultdict(list),
        "slowest": [],
    }

    for t in tests:
        status = t.get("status", "unknown")
        name = t.get("name", "")
        root = t.get("root", "unknown")

        # Categorize
        if status == "passed":
            summary["passed"] += 1
            summary["by_crate"][root]["passed"] += 1
        elif status == "failed":
            summary["failed"] += 1
            summary["by_crate"][root]["failed"] += 1
        elif status == "errored":
            summary["errored"] += 1
            summary["by_crate"][root]["errored"] += 1
        elif status == "ignored":
            summary["ignored"] += 1
            summary["by_crate"][root]["ignored"] += 1
        else:
            summary["skipped"] += 1
            summary["by_crate"][root]["skipped"] += 1

        summary["by_status"][status].append(name)

        # Duration — try from stdout if present, else skip
        duration = None
        if "stdout" in t and isinstance(t["stdout"], str):
            try:
                # Some outputs include duration info in stdout
                import re
                m = re.search(r'(\d+\.\d+)(s|ms)', t["stdout"])
                if m:
                    val = float(m.group(1))
                    unit = m.group(2)
                    duration = val if unit == "s" else val / 1000.0
            except (ValueError, AttributeError):
                pass

        if duration is not None:
            summary["slowest"].append({
                "name": name,
                "duration": duration,
                "root": root,
            })

    summary["slowest"].sort(key=lambda x: x["duration"], reverse=True)

    # Convert defaultdicts to regular dicts for JSON serialization
    summary["by_crate"] = dict(summary["by_crate"])
    summary["by_status"] = {k: v for k, v in summary["by_status"].items()}

    return summary


def format_markdown(summary, title="Test Report"):
    """Render summary as markdown."""
    lines = []
    lines.append(f"## {title}")
    lines.append("")

    # Overall
    lines.append("### Overall")
    lines.append("")
    total = summary["total"]
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| **Total** | **{total}** |")
    lines.append(f"| ✅ Passed | {summary['passed']} |")
    lines.append(f"| ❌ Failed | {summary['failed']} |")
    lines.append(f"| ⚠️  Errored | {summary['errored']} |")
    lines.append(f"| 🔇 Ignored | {summary['ignored']} |")
    lines.append(f"| ⏭️  Skipped | {summary['skipped']} |")
    lines.append("")

    # By crate
    if summary["by_crate"]:
        lines.append("### By Crate")
        lines.append("")
        lines.append("| Crate | Passed | Failed | Errored | Ignored | Skipped |")
        lines.append("|-------|--------|--------|---------|---------|---------|")
        for crate, counts in sorted(summary["by_crate"].items()):
            lines.append(
                f"| {crate} | {counts['passed']} | {counts['failed']} | "
                f"{counts['errored']} | {counts['ignored']} | {counts['skipped']} |"
            )
        lines.append("")

    # Slowest tests (if we captured durations)
    if summary["slowest"]:
        top_slow = summary["slowest"][:5]
        lines.append("### Slowest Tests")
        lines.append("")
        lines.append("| Rank | Test | Duration |")
        lines.append("|------|------|----------|")
        for i, t in enumerate(top_slow, 1):
            # Truncate long test names
            name = t["name"]
            if len(name) > 80:
                name = name[:77] + "..."
            lines.append(f"| {i} | `{name}` | {t['duration']:.3f}s |")
        lines.append("")

    # Failed tests (if any)
    if summary["by_status"].get("failed"):
        lines.append("### Failed Tests")
        lines.append("")
        for name in summary["by_status"]["failed"][:20]:
            lines.append(f"- `{name}`")
        if len(summary["by_status"]["failed"]) > 20:
            lines.append(f"... and {len(summary['by_status']['failed']) - 20} more")
        lines.append("")

    # Errored tests (if any)
    if summary["by_status"].get("errored"):
        lines.append("### Errored Tests")
        lines.append("")
        for name in summary["by_status"]["errored"][:20]:
            lines.append(f"- `{name}`")
        if len(summary["by_status"]["errored"]) > 20:
            lines.append(f"... and {len(summary['by_status']['errored']) - 20} more")
        lines.append("")

    return "\n".join(lines)


def format_json(summary):
    """Return summary as serializable dict."""
    return {
        "total": summary["total"],
        "passed": summary["passed"],
        "failed": summary["failed"],
        "errored": summary["errored"],
        "ignored": summary["ignored"],
        "skipped": summary["skipped"],
        "by_crate": summary["by_crate"],
        "slowest": summary["slowest"][:10],
        "failed_tests": summary["by_status"].get("failed", []),
        "errored_tests": summary["by_status"].get("errored", []),
    }


def main():
    parser = argparse.ArgumentParser(description="Parse nextest libtest-json output")
    parser.add_argument("files", nargs="+", help="JSONL files to parse")
    parser.add_argument("--output-json", "-o", help="Write JSON report to this file")
    parser.add_argument("--title", default="Test Report", help="Title for markdown output")
    args = parser.parse_args()

    tests, suite_start = parse_jsonl_files(args.files)
    summary = analyze_tests(tests)

    # Print markdown to stdout (for CI capture / PR comment)
    md = format_markdown(summary, args.title)
    print(md)

    # Write JSON if requested
    if args.output_json:
        report = format_json(summary)
        with open(args.output_json, "w") as f:
            json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
