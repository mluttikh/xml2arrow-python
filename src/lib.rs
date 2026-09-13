use arrow::pyarrow::ToPyArrow;
use pyo3::{
    exceptions::{PyKeyError, PyOSError, PyTypeError, PyValueError},
    intern,
    prelude::*,
    sync::PyOnceLock,
    types::{PyByteArray, PyBytes, PyDict, PyMemoryView, PyTuple},
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
            // A buffer only means "these bytes" when its elements *are*
            // bytes and they are laid out in order. A `float64` array
            // exports a buffer just as happily as a `uint8` one, and
            // `tobytes()` would hand us its raw representation: no `<`
            // anywhere, so the parse succeeds and returns zero rows. An
            // empty result for a wrong input is the one outcome worth
            // ruling out, so both properties are checked rather than
            // assumed.
            let itemsize: usize = view.getattr(intern!(py, "itemsize"))?.extract()?;
            let contiguous: bool = view.getattr(intern!(py, "c_contiguous"))?.extract()?;
            if itemsize != 1 || !contiguous {
                return Err(PyTypeError::new_err(format!(
                    "parse() needs a contiguous buffer of bytes, but got one with \
                     itemsize {itemsize} (contiguous: {contiguous}). Convert it \
                     first, e.g. with .tobytes() or .astype('uint8')"
                )));
            }
            let bytes = view.call_method0(intern!(py, "tobytes"))?;
            return Ok(Self::Bytes(bytes.cast_into().map_err(PyErr::from)?));
        }
        Err(PyTypeError::new_err(
            "parse() expects a path, bytes-like or buffer object, or a file-like \
             object with a read() method",
        ))
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
    /// Where the configuration came from. Retained rather than discarded so
    /// `__repr__` can say which, and so `__reduce__` has something to rebuild
    /// from — a path parser re-reads its file in the child process, a
    /// YAML-string parser carries the string.
    source: ConfigSource,
    /// Upstream's `Parser` is itself a handle over shared compiled state, so
    /// this is one refcounted trie however many streams are cloned off it.
    parser: Parser,
    /// The configuration the parser was compiled from, kept for
    /// `to_version_2`: converting it, rather than re-reading the source,
    /// converts exactly what this parser parses, even if the file has changed
    /// since.
    config: Config,
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
            source: ConfigSource::Path(config_path),
            parser: Parser::new(&config)?,
            config,
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
            source: ConfigSource::Yaml(yaml.to_owned()),
            parser: Parser::new(&config)?,
            config,
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
    /// The schema is fully determined by the configuration: the table's key
    /// and link columns (``_id``, ``_<table>_id``, ...), or in a version 1
    /// configuration its ``<level>`` columns, followed by the configured
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
    /// configured, so adding a column can change a table's row count. A
    /// configuration that does not declare ``version: 2`` also gets a
    /// deprecation notice, last, listing what it still needs to change.
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

    /// Converts the configuration to format version 2, without changing what
    /// it produces.
    ///
    /// Every document parses to the same tables, columns, values and errors
    /// under the converted configuration as under this one. A part that
    /// version 2 can only express by changing the output is left as it was and
    /// listed in ``unconverted``; the converted configuration declares
    /// ``version: 2`` only when that list is empty. A configuration that
    /// already declares ``version: 2`` comes back unchanged.
    ///
    /// The YAML is written fresh: the original's comments and layout are not
    /// kept, and keys left at their defaults are omitted.
    ///
    /// Returns:
    ///     Conversion: The converted configuration as YAML, and the parts
    ///         left for you to decide.
    ///
    /// Example:
    ///     >>> conversion = XmlToArrowParser("config.yaml").to_version_2()
    ///     >>> for part in conversion.unconverted:
    ///     ...     print("left for you:", part)
    ///     >>> Path("config-v2.yaml").write_text(conversion.yaml)
    pub fn to_version_2(&self) -> PyResult<Conversion> {
        let conversion = self.config.to_version_2()?;
        let yaml = yaml_serde::to_string(&conversion.config).map_err(xml2arrow::Error::from)?;
        Ok(Conversion {
            yaml,
            // Strings, as `warnings()` returns: upstream's `Unconverted` is
            // `#[non_exhaustive]`, and its `Display` text says what to decide.
            unconverted: conversion
                .unconverted
                .iter()
                .map(ToString::to_string)
                .collect(),
        })
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

    fn __repr__(&self) -> String {
        match &self.source {
            // Byte-identical to what a path-built parser has always rendered;
            // a test pins it.
            ConfigSource::Path(path) => {
                format!("XmlToArrowParser(config_path='{}')", path.to_string_lossy())
            }
            // Shaped like a call, as the path form is: a repr should read
            // like the expression that rebuilds the object, and this one
            // names the constructor that would.
            ConfigSource::Yaml(_) => "XmlToArrowParser(from_yaml_str=...)".to_string(),
        }
    }
}

/// The result of [`XmlToArrowParser::to_version_2`]: the converted
/// configuration as YAML, and the parts left for the author to decide.
///
/// Holds text rather than a parser, because the point of converting is to
/// write the result down and review it. `XmlToArrowParser.from_yaml_str`
/// turns it into a parser when one is wanted.
#[pyclass(name = "Conversion", module = "xml2arrow._xml2arrow", frozen)]
pub struct Conversion {
    yaml: String,
    unconverted: Vec<String>,
}

#[pymethods]
impl Conversion {
    /// The converted configuration, as YAML.
    ///
    /// It declares ``version: 2`` when ``unconverted`` is empty. Otherwise
    /// every other part is converted, and the configuration keeps its version
    /// until the listed parts are resolved.
    #[getter]
    fn yaml(&self) -> &str {
        &self.yaml
    }

    /// The parts that could not be converted without changing the output,
    /// one message each, saying what would change.
    #[getter]
    fn unconverted(&self) -> Vec<String> {
        self.unconverted.clone()
    }

    fn __repr__(&self) -> String {
        format!("Conversion(unconverted={})", self.unconverted.len())
    }
}

/// A Python module for parsing XML files to Arrow RecordBatches.
#[pymodule]
fn _xml2arrow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XmlToArrowParser>()?;
    m.add_class::<RecordBatchStream>()?;
    m.add_class::<Conversion>()?;
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
