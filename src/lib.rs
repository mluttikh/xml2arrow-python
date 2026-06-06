use arrow::pyarrow::ToPyArrow;
use pyo3::{
    prelude::*,
    types::{PyByteArray, PyBytes, PyDict},
};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
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
        if let Ok(path) = ob.extract::<PathBuf>() {
            return Ok(Self::File(File::open(path)?));
        }
        if let Ok(path) = ob.extract::<String>() {
            return Ok(Self::File(File::open(path)?));
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
    ///
    /// Args:
    ///     source: The XML to parse. Accepts ``str``, ``os.PathLike``,
    ///         ``bytes``, ``bytearray``, or a readable file-like object.
    ///
    /// Returns:
    ///     dict: A dictionary where keys are table names (strings) and values are PyArrow RecordBatch objects.
    #[pyo3(signature = (source))]
    pub fn parse(&self, py: Python<'_>, source: XmlInput<'_>) -> PyResult<Py<PyAny>> {
        let batches = match source {
            XmlInput::Bytes(b) => self.parser.parse_slice(b.as_bytes())?,
            XmlInput::OwnedBytes(v) => self.parser.parse_slice(&v)?,
            XmlInput::File(f) => self.parser.parse(BufReader::new(XmlReader::File(f)))?,
            XmlInput::FileLike(f) => self.parser.parse(BufReader::new(XmlReader::FileLike(f)))?,
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
