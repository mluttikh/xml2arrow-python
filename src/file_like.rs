//! Minimal `Read` adapter over a Python file-like object.
//!
//! Vendored in place of the `pyo3-file` dependency:
//! - We only need `read()`. The upstream crate also exposes Write/Seek/fileno,
//!   none of which we use.
//! - Pinning to an upstream release blocks PyO3 version bumps.
//! - A local implementation lets us tailor the fast paths (see the planned
//!   slurp-to-bytes optimization for in-memory file-likes).
//!
//! The text-mode branch matches `pyo3-file`'s approach (request at most
//!  `buf.len() / 4` chars per call so the UTF-8 encoding fits) and exists
//! only so `parser.parse(open(path))` keeps working without `"rb"`.

use std::borrow::Cow;
use std::io::{self, Read, Write as _};

use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

/// A read-only adapter over a Python file-like object.
pub struct PyBinaryFile {
    inner: Py<PyAny>,
    is_text: bool,
}

impl PyBinaryFile {
    /// Validates that `obj` exposes a `read` attribute and captures whether
    /// it is an `io.TextIOBase` subclass so `read()` can encode text results.
    pub fn from_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        if !obj.hasattr(intern!(py, "read"))? {
            return Err(PyTypeError::new_err(
                "parse() expects a path, bytes-like, or file-like object with a read() method",
            ));
        }
        let is_text = obj.is_instance(text_io_base(py)?)?;
        Ok(Self {
            inner: obj.clone().unbind(),
            is_text,
        })
    }
}

impl Read for PyBinaryFile {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        Python::attach(|py| {
            let inner = self.inner.bind(py);
            if self.is_text {
                // Request at most buf.len() / 4 characters — every Unicode
                // scalar encodes to ≤ 4 UTF-8 bytes, so the result always
                // fits without a spill buffer.
                if buf.len() < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "buffer must be at least 4 bytes for a text-mode file-like input",
                    ));
                }
                let chunk = inner.call_method1(intern!(py, "read"), (buf.len() / 4,))?;
                let text = chunk.extract::<Cow<'_, str>>()?;
                let bytes = text.as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            } else {
                let chunk = inner.call_method1(intern!(py, "read"), (buf.len(),))?;
                let bytes = chunk.extract::<Cow<'_, [u8]>>()?;
                buf.write_all(&bytes)?;
                Ok(bytes.len())
            }
        })
    }
}

fn text_io_base(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static INSTANCE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    INSTANCE
        .get_or_try_init(py, || {
            let io = PyModule::import(py, "io")?;
            Ok(io.getattr("TextIOBase")?.unbind())
        })
        .map(|cell| cell.bind(py))
}
