use arrow::pyarrow::ToPyArrow;
use pyo3::{
    exceptions::{PyOSError, PyTypeError, PyValueError},
    intern,
    prelude::*,
    sync::PyOnceLock,
    types::{PyByteArray, PyBytes, PyDict, PyMemoryView, PyTuple},
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

/// Cached `io.BytesIO` type object, used to route in-memory streams to the
/// slice fast path instead of chunked Python `read()` calls.
fn bytes_io(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static INSTANCE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    INSTANCE
        .get_or_try_init(py, || {
            let io = PyModule::import(py, "io")?;
            Ok(io.getattr("BytesIO")?.unbind())
        })
        .map(|cell| cell.bind(py))
}

/// Represents an XML input source.
///
/// `Bytes` (zero-copy) and `OwnedBytes` (a safe copy of a mutable
/// `bytearray`) route through the slice parser; `File` and `FileLike`
/// stream through a buffered reader. Other in-memory shapes (`BytesIO`,
/// buffer exporters) are snapshotted into one of the first two at
/// extraction time.
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
        let py = ob.py();
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
        // `BytesIO` would work through the generic file-like fallback, but
        // one no-size read() drains the whole remainder as a single bytes
        // object — one interpreter call instead of a chunked read() loop,
        // with the streaming path's cursor semantics (consume from the
        // current position, leave the cursor at EOF) for free.
        if ob.is_instance(bytes_io(py)?)? {
            let rest = ob.call_method0(intern!(py, "read"))?;
            return Ok(Self::Bytes(rest.cast_into().map_err(PyErr::from)?));
        }
        // Streaming file-likes. This check deliberately precedes the buffer
        // fallback below so that mmap — which both exports a buffer and has
        // read() — streams: one-chunk-at-a-time peak memory is the point of
        // mmapping a large file in the first place.
        if ob.hasattr(intern!(py, "read"))? {
            return Ok(Self::FileLike(PyBinaryFile::from_bound(ob)?));
        }
        // Remaining buffer exporters: memoryview, NumPy uint8 arrays,
        // array('B'), ... Snapshotted with one tobytes() copy — the buffer
        // protocol is outside the abi3-py310 limited API, so zero-copy here
        // must wait until the wheel's Python floor moves to 3.11.
        if let Ok(view) = PyMemoryView::from(ob) {
            let bytes = view.call_method0(intern!(py, "tobytes"))?;
            return Ok(Self::Bytes(bytes.cast_into().map_err(PyErr::from)?));
        }
        Err(PyTypeError::new_err(
            "parse() expects a path, bytes-like or buffer object, or a file-like \
             object with a read() method",
        ))
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

/// Where the parser's configuration came from — kept so `__repr__` can show
/// it and `__reduce__` can rebuild the parser on unpickling.
enum ConfigSource {
    Path(PathBuf),
    Yaml(String),
}

/// A parser for converting XML files to Arrow tables based on a configuration.
///
/// The configuration's path trie is compiled once, here, and reused for every
/// `parse()` call via [`xml2arrow::Parser`]. This matters most when one parser
/// instance processes many files: the fixed per-document setup cost (config
/// validation + path-trie construction) is paid a single time rather than on
/// every parse.
///
/// `module = ...` matters for pickling: `__reduce__` serializes the class by
/// reference, and pickle must be able to import it from that location.
#[pyclass(name = "XmlToArrowParser", module = "xml2arrow._xml2arrow")]
pub struct XmlToArrowParser {
    source: ConfigSource,
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
            source: ConfigSource::Path(config_path),
            parser: Parser::new(&config)?,
        })
    }

    /// Creates a parser from a YAML configuration string.
    ///
    /// Args:
    ///     yaml (str): The configuration in YAML format — the same schema a
    ///         configuration file uses.
    ///
    /// Returns:
    ///     XmlToArrowParser: A new parser instance.
    #[staticmethod]
    pub fn from_yaml_str(yaml: &str) -> PyResult<Self> {
        // Mirrors `Config::from_yaml_file` (whose docs name
        // `yaml_serde::from_str` as the route for string input):
        // deserialize, validate, compile. Errors surface as
        // YamlParsingError / InvalidConfigError exactly like the path
        // constructor.
        let config: Config = yaml_serde::from_str(yaml).map_err(xml2arrow::Error::from)?;
        config.validate()?;
        Ok(XmlToArrowParser {
            source: ConfigSource::Yaml(yaml.to_owned()),
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
    ///         ``bytes``, ``bytearray``, any object exporting the buffer
    ///         protocol (``memoryview``, NumPy ``uint8`` arrays, ... —
    ///         snapshotted with one copy), or a readable file-like object.
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
            XmlInput::FileLike(f) => py.detach(|| {
                // 64 KiB per refill: each one is a Python read() round-trip
                // (text mode requests capacity/4 characters), so the default
                // 8 KiB buffer costs 8x the interpreter-call overhead.
                let reader = BufReader::with_capacity(64 * 1024, XmlReader::FileLike(f));
                self.parser.parse(reader)
            })?,
        };
        let tables = PyDict::new(py);
        for (name, batch) in batches {
            let py_batch = batch.to_pyarrow(py)?;
            tables.set_item(name, py_batch)?;
        }
        Ok(tables.into())
    }

    fn __repr__(&self) -> String {
        match &self.source {
            ConfigSource::Path(p) => {
                format!("XmlToArrowParser(config_path='{}')", p.to_string_lossy())
            }
            ConfigSource::Yaml(_) => "XmlToArrowParser(from_yaml_str=...)".to_string(),
        }
    }

    /// Supports pickling, and therefore multiprocessing: path-built parsers
    /// re-read their configuration file in the child process, while
    /// YAML-string parsers carry the configuration inside the pickle.
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
        let cls = py.get_type::<Self>();
        match &self.source {
            ConfigSource::Path(path) => Ok((cls.into_any(), PyTuple::new(py, [path.clone()])?)),
            ConfigSource::Yaml(yaml) => Ok((
                cls.getattr(intern!(py, "from_yaml_str"))?,
                PyTuple::new(py, [yaml.as_str()])?,
            )),
        }
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
