use arrow::pyarrow::ToPyArrow;
use pyo3::{prelude::*, types::PyDict};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use xml2arrow::config::Config;
use xml2arrow::errors::{
    NoTableOnStackError, ParseError, TableNotFoundError, UnsupportedDataTypeError, XmlParsingError,
    YamlParsingError,
};
use xml2arrow::parse_xml;

/// A parser for converting XML files to Arrow tables based on a configuration.
#[pyclass(name = "XmlToArrowParser")]
pub struct XmlToArrowParser {
    pub config: Config,
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
            config: Config::from_yaml_file(config_path)?,
        })
    }

    /// Parses an XML file and returns a dictionary of Arrow RecordBatches.
    ///
    /// Args:
    ///     path (str or PathLike): The path to the XML file to parse.
    ///
    /// Returns:
    ///     dict: A dictionary where keys are table names (strings) and values are PyArrow RecordBatch objects.
    #[pyo3(signature = (path))]
    pub fn parse(&self, path: PathBuf) -> PyResult<PyObject> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
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
}

/// A Python module for parsing XML files to Arrow RecordBatches.
#[pymodule]
fn _xml2arrow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XmlToArrowParser>()?;
    m.add("XmlParsingError", py.get_type::<XmlParsingError>())?;
    m.add("YamlParsingError", py.get_type::<YamlParsingError>())?;
    m.add(
        "UnsupportedDataTypeError",
        py.get_type::<UnsupportedDataTypeError>(),
    )?;
    m.add("TableNotFoundError", py.get_type::<TableNotFoundError>())?;
    m.add("NoTableOnStackError", py.get_type::<NoTableOnStackError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    Ok(())
}
