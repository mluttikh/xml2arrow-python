use arrow::pyarrow::ToPyArrow;
use pyo3::{
    prelude::*,
    types::{PyByteArray, PyBytes, PyDict},
};
use pyo3_file::PyFileLikeObject;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use xml2arrow::config::Config;
use xml2arrow::errors::{
    NoTableOnStackError, ParseError, TableNotFoundError, UnsupportedConversionError,
    UnsupportedDataTypeError, Xml2ArrowError, XmlParsingError, YamlParsingError,
};
use xml2arrow::{parse_xml, parse_xml_slice};

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
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'py> for XmlInput<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(b) = ob.downcast::<PyBytes>() {
            return Ok(Self::Bytes(b.clone()));
        }
        if let Ok(ba) = ob.downcast::<PyByteArray>() {
            return Ok(Self::OwnedBytes(ba.to_vec()));
        }
        if let Ok(path) = ob.extract::<PathBuf>() {
            return Ok(Self::File(File::open(path)?));
        }
        if let Ok(path) = ob.extract::<String>() {
            return Ok(Self::File(File::open(path)?));
        }
        Ok(Self::FileLike(PyFileLikeObject::with_requirements(
            ob.clone().unbind(),
            true,
            false,
            false,
            false,
        )?))
    }
}

/// A streaming adapter over `File` and file-like Python objects.
enum XmlReader {
    File(File),
    FileLike(PyFileLikeObject),
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
#[pyclass(name = "XmlToArrowParser")]
pub struct XmlToArrowParser {
    config_path: PathBuf,
    config: Config,
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
        Ok(XmlToArrowParser {
            config_path: config_path.clone(),
            config: Config::from_yaml_file(config_path)?,
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
            XmlInput::Bytes(b) => parse_xml_slice(b.as_bytes(), &self.config)?,
            XmlInput::OwnedBytes(v) => parse_xml_slice(&v, &self.config)?,
            XmlInput::File(f) => parse_xml(BufReader::new(XmlReader::File(f)), &self.config)?,
            XmlInput::FileLike(f) => {
                parse_xml(BufReader::new(XmlReader::FileLike(f)), &self.config)?
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
    m.add(
        "UnsupportedDataTypeError",
        py.get_type::<UnsupportedDataTypeError>(),
    )?;
    m.add("TableNotFoundError", py.get_type::<TableNotFoundError>())?;
    m.add("NoTableOnStackError", py.get_type::<NoTableOnStackError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    m.add(
        "UnsupportedConversionError",
        py.get_type::<UnsupportedConversionError>(),
    )?;
    m.add_wrapped(wrap_pyfunction!(_get_version))?;
    Ok(())
}
