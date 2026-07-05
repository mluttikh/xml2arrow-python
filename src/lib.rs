use arrow::pyarrow::ToPyArrow;
use pyo3::{
    exceptions::{PyOSError, PyValueError},
    prelude::*,
    types::{PyByteArray, PyBytes, PyDict},
};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use xml2arrow::Parser;
use xml2arrow::config::Config;
use xml2arrow::errors::{
    InvalidConfigError, ParseError, UnsupportedConversionError, Xml2ArrowError, XmlParsingError,
    YamlParsingError,
};

mod file_like;
use file_like::PyBinaryFile;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn _get_version() -> &'static str {
    VERSION
}

/// Rebuilds a filesystem error as a Python `OSError` that carries the path;
/// the raw `io::Error` drops it, which made "No such file or directory
/// (os error 2)" unactionable. Also catches the common mistake of passing
/// XML *content* as a `str`, which would otherwise surface as a baffling
/// `FileNotFoundError` (or `ENAMETOOLONG` for larger documents).
fn open_error(err: std::io::Error, path: &Path) -> PyErr {
    let text = path.to_string_lossy();
    if text
        .trim_start_matches('\u{feff}')
        .trim_start()
        .starts_with('<')
    {
        return PyValueError::new_err(
            "source looks like XML content, not a file path; pass XML content as bytes \
             (e.g. source.encode()) or wrap it in io.StringIO",
        );
    }
    let Some(code) = err.raw_os_error() else {
        return err.into();
    };
    // Strip io::Error's "(os error N)" suffix; OSError re-renders the code.
    let msg = err.to_string();
    let msg = msg.split(" (os error ").next().unwrap_or(&msg).to_string();
    let filename = text.into_owned();
    // OSError's multi-arg constructor picks the right subclass
    // (FileNotFoundError, PermissionError, ...) from the error code and
    // includes the filename in the message. On Windows `raw_os_error` is a
    // winerror, which OSError translates only via its fourth argument.
    #[cfg(windows)]
    let args = (code, msg, filename, code);
    #[cfg(not(windows))]
    let args = (code, msg, filename);
    PyOSError::new_err(args)
}

/// Represents an XML input source.
///
/// `Bytes` (zero-copy) and `OwnedBytes` (a safe copy of a mutable
/// `bytearray`) route through [`parse_xml_slice`]; `File` and `FileLike`
/// stream through [`parse_xml`].
pub enum XmlInput<'py> {
    Bytes(Bound<'py, PyBytes>),
    OwnedBytes(Vec<u8>),
    File(File),
    FileLike(PyBinaryFile),
}

impl<'a, 'py> FromPyObject<'a, 'py> for XmlInput<'py> {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let ob: &Bound<'py, PyAny> = &obj;
        if let Ok(b) = ob.cast::<PyBytes>() {
            return Ok(Self::Bytes(b.clone()));
        }
        if let Ok(ba) = ob.cast::<PyByteArray>() {
            return Ok(Self::OwnedBytes(ba.to_vec()));
        }
        // `PathBuf` extraction accepts both `str` and `os.PathLike`.
        if let Ok(path) = ob.extract::<PathBuf>() {
            return match File::open(&path) {
                Ok(f) => Ok(Self::File(f)),
                Err(e) => Err(open_error(e, &path)),
            };
        }
        Ok(Self::FileLike(PyBinaryFile::from_bound(ob)?))
    }
}

/// A streaming adapter over `File` and file-like Python objects.
enum XmlReader {
    File(File),
    FileLike(PyBinaryFile),
}

impl Read for XmlReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
        }
    }
}

/// A parser for converting XML files to Arrow tables based on a configuration.
///
/// The configuration's path trie is compiled once, here, and reused for every
/// `parse()` call via [`xml2arrow::Parser`]. This matters most when one parser
/// instance processes many files: the fixed per-document setup cost (config
/// validation + path-trie construction) is paid a single time rather than on
/// every parse.
#[pyclass(name = "XmlToArrowParser")]
pub struct XmlToArrowParser {
    config_path: PathBuf,
    parser: Parser,
}

#[pymethods]
impl XmlToArrowParser {
    /// Creates a new XmlToArrowParser instance from a YAML configuration file.
    ///
    /// Args:
    ///     config_path (str or PathLike): The path to the YAML configuration file.
    ///
    /// Returns:
    ///     XmlToArrowParser: A new parser instance.
    #[new]
    pub fn new(config_path: PathBuf) -> PyResult<Self> {
        // A missing/unreadable config should name the offending path; the
        // io::Error that bubbles out of `from_yaml_file` drops it.
        std::fs::metadata(&config_path).map_err(|e| open_error(e, &config_path))?;
        // Compile the config once here. `Parser::new` also runs config
        // validation, so an invalid config now surfaces at construction time
        // rather than on the first `parse()` call.
        let config = Config::from_yaml_file(config_path.clone())?;
        Ok(XmlToArrowParser {
            config_path,
            parser: Parser::new(&config)?,
        })
    }

    /// Parses an XML source and returns a dictionary of Arrow RecordBatches.
    ///
    /// In-memory inputs (``bytes`` and ``bytearray``) take a zero-copy fast
    /// path. Paths and file-like objects stream through a buffered reader.
    /// The GIL is released while parsing, so threads sharing one parser
    /// instance can parse different sources in parallel.
    ///
    /// Args:
    ///     source: The XML to parse. Accepts ``str``, ``os.PathLike``,
    ///         ``bytes``, ``bytearray``, or a readable file-like object.
    ///
    /// Returns:
    ///     dict: A dictionary where keys are table names (strings) and values are PyArrow RecordBatch objects.
    #[pyo3(signature = (source))]
    pub fn parse(&self, py: Python<'_>, source: XmlInput<'_>) -> PyResult<Py<PyAny>> {
        // Detaching from the interpreter is sound for every variant: `Bytes`
        // is immutable, `OwnedBytes` was copied at extraction, `File` is a
        // plain OS handle, and `FileLike` re-attaches for each read() call.
        let batches = match source {
            XmlInput::Bytes(b) => {
                let bytes = b.as_bytes();
                py.detach(|| self.parser.parse_slice(bytes))?
            }
            XmlInput::OwnedBytes(v) => py.detach(|| self.parser.parse_slice(&v))?,
            XmlInput::File(f) => {
                py.detach(|| self.parser.parse(BufReader::new(XmlReader::File(f))))?
            }
            XmlInput::FileLike(f) => {
                py.detach(|| self.parser.parse(BufReader::new(XmlReader::FileLike(f))))?
            }
        };
        let tables = PyDict::new(py);
        for (name, batch) in batches {
            let py_batch = batch.to_pyarrow(py)?;
            tables.set_item(name, py_batch)?;
        }
        Ok(tables.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "XmlToArrowParser(config_path='{}')",
            self.config_path.to_string_lossy()
        )
    }
}

/// A Python module for parsing XML files to Arrow RecordBatches.
#[pymodule]
fn _xml2arrow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XmlToArrowParser>()?;
    m.add("Xml2ArrowError", py.get_type::<Xml2ArrowError>())?;
    m.add("XmlParsingError", py.get_type::<XmlParsingError>())?;
    m.add("YamlParsingError", py.get_type::<YamlParsingError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    m.add(
        "UnsupportedConversionError",
        py.get_type::<UnsupportedConversionError>(),
    )?;
    m.add("InvalidConfigError", py.get_type::<InvalidConfigError>())?;
    m.add_wrapped(wrap_pyfunction!(_get_version))?;
    Ok(())
}
