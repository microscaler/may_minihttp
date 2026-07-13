//! Multipart/form-data request encoding.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_BOUNDARY: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct Part {
    name: String,
    filename: Option<String>,
    content_type: Option<String>,
    data: Vec<u8>,
}

/// A deterministic-length multipart/form-data body.
///
/// Text and byte parts are retained once and streamed directly into the outgoing request. Encoding
/// does not create a second body-sized allocation. Field metadata is validated before any bytes are
/// written so CR/LF header injection cannot produce malformed MIME headers.
#[derive(Debug, Clone)]
pub struct MultipartForm {
    boundary: String,
    parts: Vec<Part>,
}

impl MultipartForm {
    /// Create an empty form with a process-unique boundary.
    pub fn new() -> Self {
        let sequence = NEXT_BOUNDARY.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            boundary: format!(
                "may-minihttp-{:x}-{nanos:x}-{sequence:x}",
                std::process::id()
            ),
            parts: Vec::new(),
        }
    }

    #[cfg(test)]
    fn with_boundary(boundary: &str) -> Self {
        Self {
            boundary: boundary.to_string(),
            parts: Vec::new(),
        }
    }

    /// Add a UTF-8 text field.
    pub fn text(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parts.push(Part {
            name: name.into(),
            filename: None,
            content_type: Some("text/plain; charset=utf-8".to_string()),
            data: value.into().into_bytes(),
        });
        self
    }

    /// Add an in-memory byte field, optionally carrying a filename and media type.
    pub fn bytes(
        mut self,
        name: impl Into<String>,
        filename: Option<impl Into<String>>,
        content_type: Option<impl Into<String>>,
        data: impl Into<Vec<u8>>,
    ) -> Self {
        self.parts.push(Part {
            name: name.into(),
            filename: filename.map(Into::into),
            content_type: content_type.map(Into::into),
            data: data.into(),
        });
        self
    }

    /// Eagerly read a bounded part at an explicit blocking boundary.
    ///
    /// Call this outside a may scheduler worker when `reader` performs blocking I/O. The retained
    /// bytes make the eventual HTTP request coroutine-safe and replayable.
    pub fn blocking_reader(
        mut self,
        name: impl Into<String>,
        filename: Option<String>,
        content_type: Option<String>,
        reader: impl Read,
        max_bytes: usize,
    ) -> io::Result<Self> {
        let mut data = Vec::new();
        reader
            .take((max_bytes as u64).saturating_add(1))
            .read_to_end(&mut data)?;
        if data.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("multipart part exceeds configured {max_bytes}-byte preload limit"),
            ));
        }
        self.parts.push(Part {
            name: name.into(),
            filename,
            content_type,
            data,
        });
        Ok(self)
    }

    /// Open and eagerly preload a bounded file part.
    ///
    /// This method is intentionally named `blocking_file`: `std::fs` has no may-aware API. Invoke
    /// it before entering latency-sensitive coroutines, or perform file loading in an explicit
    /// application-owned blocking executor and pass the resulting bytes to [`Self::bytes`].
    pub fn blocking_file(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        content_type: Option<String>,
        max_bytes: usize,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "multipart file path has no UTF-8 filename",
                )
            })?
            .to_string();
        let file = std::fs::File::open(path)?;
        self.blocking_reader(name, Some(filename), content_type, file, max_bytes)
    }

    /// Boundary token used by this form.
    pub fn boundary(&self) -> &str {
        &self.boundary
    }

    /// Value for the HTTP `Content-Type` header.
    pub fn content_type(&self) -> String {
        format!("multipart/form-data; boundary={}", self.boundary)
    }

    /// Compute the exact encoded body length without materializing the encoded body.
    pub fn content_length(&self) -> io::Result<usize> {
        let mut length = 0_usize;
        for part in &self.parts {
            let head = self.part_head(part)?;
            length = checked_add(length, head.len())?;
            length = checked_add(length, part.data.len())?;
            length = checked_add(length, 2)?; // trailing CRLF
        }
        checked_add(length, self.final_boundary().len())
    }

    /// Stream the encoded body to a writer.
    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        for part in &self.parts {
            writer.write_all(self.part_head(part)?.as_bytes())?;
            writer.write_all(&part.data)?;
            writer.write_all(b"\r\n")?;
        }
        writer.write_all(self.final_boundary().as_bytes())
    }

    /// Encode the complete body into a byte vector.
    ///
    /// Prefer [`Self::write_to`] for network requests; this helper is useful for signing, fixtures,
    /// or callers that explicitly require a contiguous representation.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut encoded = Vec::with_capacity(self.content_length()?);
        self.write_to(&mut encoded)?;
        Ok(encoded)
    }

    fn part_head(&self, part: &Part) -> io::Result<String> {
        validate_metadata("field name", &part.name)?;
        let mut head = format!(
            "--{}\r\nContent-Disposition: form-data; name=\"{}\"",
            self.boundary,
            escape_quoted(&part.name)
        );
        if let Some(filename) = &part.filename {
            validate_metadata("filename", filename)?;
            head.push_str(&format!("; filename=\"{}\"", escape_quoted(filename)));
        }
        head.push_str("\r\n");
        if let Some(content_type) = &part.content_type {
            validate_metadata("content type", content_type)?;
            head.push_str("Content-Type: ");
            head.push_str(content_type);
            head.push_str("\r\n");
        }
        head.push_str("\r\n");
        Ok(head)
    }

    fn final_boundary(&self) -> String {
        format!("--{}--\r\n", self.boundary)
    }
}

impl Default for MultipartForm {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_metadata(kind: &str, value: &str) -> io::Result<()> {
    if value.contains(['\r', '\n']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("multipart {kind} must not contain CR or LF"),
        ));
    }
    Ok(())
}

fn escape_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn checked_add(left: usize, right: usize) -> io::Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "multipart body length exceeds usize",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_length_matches_streamed_encoding() {
        let form = MultipartForm::with_boundary("test-boundary")
            .text("note", "hello")
            .bytes(
                "file",
                Some("image.png"),
                Some("image/png"),
                vec![0x89, b'P', b'N', b'G'],
            );

        let encoded = form.encode().unwrap();
        assert_eq!(form.content_length().unwrap(), encoded.len());
        let text = String::from_utf8_lossy(&encoded);
        assert!(text.contains("name=\"note\""));
        assert!(text.contains("filename=\"image.png\""));
        assert!(text.ends_with("--test-boundary--\r\n"));
    }

    #[test]
    fn multipart_rejects_header_injection_before_writing() {
        let form = MultipartForm::with_boundary("safe").text("x\r\nInjected: yes", "value");
        let mut output = Vec::new();
        let error = form.write_to(&mut output).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(output.is_empty());
    }

    #[test]
    fn multipart_escapes_quoted_metadata() {
        let form = MultipartForm::with_boundary("safe").bytes(
            "a\"b",
            Some("c\\d.txt"),
            None::<String>,
            b"body".to_vec(),
        );
        let encoded = String::from_utf8(form.encode().unwrap()).unwrap();
        assert!(encoded.contains("name=\"a\\\"b\""));
        assert!(encoded.contains("filename=\"c\\\\d.txt\""));
    }

    #[test]
    fn blocking_reader_is_bounded_and_becomes_replayable_bytes() {
        let form = MultipartForm::with_boundary("safe")
            .blocking_reader(
                "file",
                Some("data.bin".to_string()),
                Some("application/octet-stream".to_string()),
                &b"payload"[..],
                7,
            )
            .unwrap();
        let first = form.encode().unwrap();
        let second = form.encode().unwrap();
        assert_eq!(first, second);
        assert!(first.windows(7).any(|window| window == b"payload"));

        let error = MultipartForm::new()
            .blocking_reader("file", None, None, &b"too large"[..], 3)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
