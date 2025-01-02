from os import PathLike
from typing import IO, Any

from pyarrow import RecordBatch

class XmlToArrowParser:
    """A parser for converting XML files to Arrow tables based on a configuration.

    Raises:
        Xml2ArrowError: If any error occurs during parsing, configuration, or Arrow table creation.
            More specific exceptions (e.g., XmlParsingError, YamlParsingError, TableNotFoundError)
            may be raised as subclasses of this base exception.
    """

    def __init__(self, source: str | PathLike) -> None:
        """Initializes the parser with a configuration file path.

        Args:
            config_path: The path to the YAML configuration file.

        Raises:
            Xml2ArrowError: If the configuration file cannot be loaded or parsed.
        """

    def parse(self, source: str | PathLike | IO[Any]) -> dict[str, RecordBatch]:
        """Parses an XML file and returns a dictionary of Arrow RecordBatches.

        Args:
            source: The XML file to parse (path or file-like object).

        Returns:
            A dictionary where keys are table names (strings) and values are PyArrow RecordBatch objects.

        Raises:
            Xml2ArrowError: If an error occurs during XML parsing or Arrow table creation.
                This can include errors such as invalid XML, incorrect configuration, or
                unsupported data types.
        """

    def __repr__(self) -> str: ...

def _get_version() -> str:
    """Returns the version of the xml2arrow package."""
