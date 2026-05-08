from os import PathLike
from typing import IO, Any, final

from pyarrow import RecordBatch

@final
class XmlToArrowParser:
    """A parser for converting XML files to Arrow tables based on a configuration.

    Raises:
        Xml2ArrowError: If any error occurs during parsing, configuration, or Arrow table creation.
            More specific exceptions (e.g., XmlParsingError, YamlParsingError, ParseError,
            UnsupportedConversionError, InvalidConfigError) may be raised as subclasses of this
            base exception.
    """

    def __init__(self, config_path: str | PathLike[str]) -> None:
        """Initializes the parser with a configuration file path.

        Args:
            config_path: The path to the YAML configuration file.

        Raises:
            Xml2ArrowError: If the configuration file cannot be loaded or parsed.
        """

    def parse(
        self,
        source: str | PathLike[str] | bytes | bytearray | IO[Any],
    ) -> dict[str, RecordBatch]:
        """Parses an XML source and returns a dictionary of Arrow RecordBatches.

        In-memory inputs (``bytes`` and ``bytearray``) take a zero-copy fast
        path. Paths and file-like objects stream through a buffered reader.

        Args:
            source: The XML to parse. Accepts a path (``str`` or ``os.PathLike``),
                an in-memory buffer (``bytes`` or ``bytearray``), or any readable
                file-like object.

        Returns:
            A dictionary where keys are table names (strings) and values are
            PyArrow RecordBatch objects.

        Raises:
            Xml2ArrowError: If an error occurs during XML parsing or Arrow
                table creation. This can include errors such as invalid XML,
                incorrect configuration, or unsupported data types.
        """

    def __repr__(self) -> str: ...

class Xml2ArrowError(Exception): ...
class XmlParsingError(Xml2ArrowError): ...
class YamlParsingError(Xml2ArrowError): ...
class ParseError(Xml2ArrowError): ...
class UnsupportedConversionError(Xml2ArrowError): ...
class InvalidConfigError(Xml2ArrowError): ...

def _get_version() -> str:
    """Returns the version of the xml2arrow package."""
