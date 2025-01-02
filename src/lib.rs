use arrow::pyarrow::ToPyArrow;
use pyo3::{prelude::*, types::PyDict};
use pyo3_file::PyFileLikeObject;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use xml2arrow::config::Config;
use xml2arrow::errors::{
    NoTableOnStackError, ParseError, TableNotFoundError, UnsupportedDataTypeError, Xml2ArrowError,
    XmlParsingError, YamlParsingError,
};
use xml2arrow::parse_xml;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn _get_version() -> &'static str {
    VERSION
}

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileOrFileLike {
    File(File),
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'py> for FileOrFileLike {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(File::open(path)?))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(File::open(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.clone().unbind(),
                true,
                false,
                false,
                false,
            )?))
        }
    }
}

impl Read for FileOrFileLike {
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

    /// Parses an XML file and returns a dictionary of Arrow RecordBatches.
    ///
    /// Args:
    ///     source (str, PathLike or file like object): The XML file to parse.
    ///
    /// Returns:
    ///     dict: A dictionary where keys are table names (strings) and values are PyArrow RecordBatch objects.
    #[pyo3(signature = (source))]
    pub fn parse(&self, source: FileOrFileLike) -> PyResult<PyObject> {
        let reader = BufReader::new(source);
        let batches = parse_xml(reader, &self.config)?;
        Python::with_gil(|py| {
            let tables = PyDict::new(py);
            for (name, batch) in batches {
                let py_batch = batch.to_pyarrow(py)?;
                tables.set_item(name, py_batch)?;
            }
            Ok(tables.into())
        })
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
    m.add_wrapped(wrap_pyfunction!(_get_version))?;
    Ok(())
}
