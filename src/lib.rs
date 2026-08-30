use arrow::pyarrow::ToPyArrow;
use pyo3::{
    exceptions::{PyKeyError, PyOSError, PyValueError},
    prelude::*,
    types::{PyByteArray, PyBytes, PyDict},
};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use xml2arrow::config::Config;
use xml2arrow::errors::{
    InvalidConfigError, ParseError, UnsupportedConversionError, Xml2ArrowError, XmlParsingError,
    YamlParsingError,
};
use xml2arrow::{BatchOptions, Parser};

mod file_like;
mod streaming;
use file_like::PyBinaryFile;
use streaming::RecordBatchStream;

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

/// A streaming adapter over every input kind, so the streaming API has one
/// reader type to name rather than one per input.
///
/// `pub(crate)` because src/streaming.rs owns readers built from this.
pub(crate) enum XmlReader {
    /// In-memory input, owned. Used only by the streaming API, which needs a
    /// reader it can own; `parse()` still takes the zero-copy slice path.
    Buffer(std::io::Cursor<Vec<u8>>),
    File(File),
    FileLike(PyBinaryFile),
}

impl Read for XmlReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Buffer(c) => c.read(buf),
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
    /// `None` when built from a YAML string. `__repr__` says so rather than
    /// inventing a path that never existed.
    config_path: Option<PathBuf>,
    /// Upstream's `Parser` is itself a handle over shared compiled state, so
    /// this is one refcounted trie however many streams are cloned off it.
    parser: Parser,
}

/// Folds the optional per-call overrides onto upstream's defaults. `None`
/// (the Python-side default) means "keep upstream's value", so defaults are
/// defined in exactly one place — the `BatchOptions::default()` impl.
fn batch_options(
    max_rows_per_batch: Option<usize>,
    max_bytes_per_batch: Option<usize>,
) -> BatchOptions {
    let mut options = BatchOptions::default();
    if let Some(rows) = max_rows_per_batch {
        options.max_rows_per_batch = rows;
    }
    if let Some(bytes) = max_bytes_per_batch {
        options.max_bytes_per_batch = bytes;
    }
    options
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
        let config = Config::from_yaml_file(&config_path)?;
        Ok(XmlToArrowParser {
            config_path: Some(config_path),
            parser: Parser::new(&config)?,
        })
    }

    /// Creates a parser from a YAML configuration string.
    ///
    /// The counterpart to the path constructor, for callers that already hold
    /// the YAML: an embedded default, a configuration fetched from a service
    /// or built by a tool, or a test that would rather not touch the
    /// filesystem. The configuration is validated exactly as a file-loaded one
    /// is, so a parser obtained either way is equally trustworthy.
    ///
    /// Named to match upstream's `Config::from_yaml_str`, which it delegates
    /// to — the two APIs are read side by side often enough that a spelling
    /// difference would be one more thing to remember.
    ///
    /// Args:
    ///     yaml (str): The YAML configuration.
    ///
    /// Returns:
    ///     XmlToArrowParser: A new parser instance.
    ///
    /// Raises:
    ///     YamlParsingError: If the string is not valid YAML, or does not
    ///         describe a configuration.
    ///     InvalidConfigError: If the configuration parses but is not valid.
    #[staticmethod]
    pub fn from_yaml_str(yaml: &str) -> PyResult<Self> {
        let config = Config::from_yaml_str(yaml)?;
        Ok(XmlToArrowParser {
            config_path: None,
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

    /// Parses an XML source incrementally, yielding batches with bounded memory.
    ///
    /// Returns an iterator of ``(table_name, batch)`` tuples. A table's batch
    /// is emitted whenever it reaches ``max_rows_per_batch`` rows or
    /// ``max_bytes_per_batch`` accumulated value bytes, so memory stays
    /// bounded by the batch limits instead of the document size — this is the
    /// entry point for XML files too large to parse with ``parse()``.
    /// Concatenating a table's batches in yield order reproduces exactly what
    /// ``parse()`` would have returned for it.
    ///
    /// Parsing happens as you iterate, on the calling thread, releasing the
    /// GIL for each batch so other Python threads keep running.
    ///
    /// Args:
    ///     source: The XML to parse. Accepts ``str``, ``os.PathLike``,
    ///         ``bytes``, ``bytearray``, or a readable file-like object.
    ///         (Unlike ``parse()``, in-memory ``bytes`` are copied once.)
    ///     max_rows_per_batch: Rows per batch before a flush (default 8192).
    ///     max_bytes_per_batch: Value bytes per batch before a flush
    ///         (default 128 MiB).
    ///
    /// Returns:
    ///     RecordBatchStream: An iterator of (str, pyarrow.RecordBatch) tuples.
    ///
    /// Raises:
    ///     Xml2ArrowError: From the iterator, not this call, when parsing
    ///         fails mid-stream. Batches yielded before it remain valid.
    #[pyo3(signature = (source, *, max_rows_per_batch=None, max_bytes_per_batch=None))]
    pub fn parse_batches(
        &self,
        source: XmlInput<'_>,
        max_rows_per_batch: Option<usize>,
        max_bytes_per_batch: Option<usize>,
    ) -> PyResult<RecordBatchStream> {
        streaming::parse_batches_impl(
            &self.parser,
            source,
            batch_options(max_rows_per_batch, max_bytes_per_batch),
        )
    }

    /// Streams the config's single output table as a native pyarrow reader.
    ///
    /// For configurations defining exactly one table with fields — the
    /// common shape for very large documents — this returns a
    /// ``pyarrow.RecordBatchReader``, directly consumable by
    /// ``pyarrow.parquet.ParquetWriter``, ``pyarrow.dataset``, DuckDB, and
    /// anything else speaking the Arrow C stream protocol. The reader's
    /// schema is available before any parsing happens.
    ///
    /// Args:
    ///     source: The XML to parse. Accepts ``str``, ``os.PathLike``,
    ///         ``bytes``, ``bytearray``, or a readable file-like object.
    ///         Every source is read incrementally, so memory stays bounded by
    ///         the batch limits.
    ///     max_rows_per_batch: Rows per batch before a flush (default 8192).
    ///     max_bytes_per_batch: Value bytes per batch before a flush
    ///         (default 128 MiB).
    ///
    /// Returns:
    ///     pyarrow.RecordBatchReader: The table's batches, in row order.
    ///
    /// Raises:
    ///     InvalidConfigError: If the configuration does not define exactly
    ///         one table with fields. Raised here, before any parsing.
    ///
    /// Note:
    ///     Failures that happen *while* the returned reader is consumed reach
    ///     Python through Arrow's C stream interface, which carries only a
    ///     message. They therefore arrive as ``pyarrow.ArrowException``
    ///     subclasses (typically ``ArrowInvalid``) quoting the original error,
    ///     **not** as ``Xml2ArrowError``. Use ``parse_batches()`` instead when
    ///     you need to catch this package's own exception types.
    #[pyo3(signature = (source, *, max_rows_per_batch=None, max_bytes_per_batch=None))]
    pub fn parse_single_table(
        &self,
        py: Python<'_>,
        source: XmlInput<'_>,
        max_rows_per_batch: Option<usize>,
        max_bytes_per_batch: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        streaming::parse_single_table_impl(
            py,
            &self.parser,
            source,
            batch_options(max_rows_per_batch, max_bytes_per_batch),
        )
    }

    /// Returns the pyarrow schema of an output table without parsing anything.
    ///
    /// The schema is fully determined by the configuration: one ``<level>``
    /// UInt32 index column per ``levels`` entry, followed by the configured
    /// fields. Useful for setting up schema-first sinks (Parquet writers,
    /// dataset registrations) before the first batch arrives.
    ///
    /// Args:
    ///     table: The table name as defined in the configuration.
    ///
    /// Returns:
    ///     pyarrow.Schema: The table's schema.
    ///
    /// Raises:
    ///     KeyError: If the configuration has no output table of that name
    ///         (structural tables — empty ``fields`` — produce no output).
    pub fn schema(&self, py: Python<'_>, table: &str) -> PyResult<Py<PyAny>> {
        match self.parser.schema(table) {
            Some(schema) => Ok(schema.to_pyarrow(py)?.unbind()),
            None => Err(PyKeyError::new_err(format!(
                "no output table named '{table}' in the configuration"
            ))),
        }
    }

    /// Returns advisory warnings about the configuration, as a list of
    /// human-readable strings.
    ///
    /// These are configurations that are *valid* but commonly surprising — most
    /// often a table whose row boundaries are inferred from more than one child
    /// element, which yields one partially-filled row per child rather than one
    /// row per record. That rule depends on which fields happen to be
    /// configured, so adding a column can change a table's row count.
    ///
    /// The list is empty for a configuration with nothing to flag. Warnings
    /// never change how a document parses, and the library never prints them:
    /// what to do with them is the caller's decision.
    ///
    /// Returns:
    ///     list[str]: One message per finding, in configuration order.
    ///
    /// Example:
    ///     >>> parser = XmlToArrowParser("config.yaml")
    ///     >>> for warning in parser.warnings():
    ///     ...     logging.warning("xml2arrow config: %s", warning)
    pub fn warnings(&self) -> Vec<String> {
        // Rendered to strings rather than exposed structurally: upstream's
        // `Lint` is `#[non_exhaustive]` and gains variants in minor releases,
        // so a structured mirror here would either fall behind or force a
        // binding change for every new lint. The `Display` text is what the
        // upstream documentation directs hosts to log.
        self.parser
            .warnings()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    fn __repr__(&self) -> String {
        match &self.config_path {
            Some(path) => format!("XmlToArrowParser(config_path='{}')", path.to_string_lossy()),
            None => "XmlToArrowParser(<from YAML string>)".to_string(),
        }
    }
}

/// A Python module for parsing XML files to Arrow RecordBatches.
#[pymodule]
fn _xml2arrow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XmlToArrowParser>()?;
    m.add_class::<RecordBatchStream>()?;
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
