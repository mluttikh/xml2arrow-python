from os import PathLike
from typing import IO, Any, final

from pyarrow import RecordBatch

__all__ = [
    "XmlToArrowParser",
    "Xml2ArrowError",
    "XmlParsingError",
    "YamlParsingError",
    "ParseError",
    "UnsupportedConversionError",
    "InvalidConfigError",
    "_get_version",
]

@final
class XmlToArrowParser:
    """A parser for converting XML files to Arrow tables based on a configuration.

    Raises:
        Xml2ArrowError: If any error occurs during parsing, configuration, or Arrow table creation.
            More specific exceptions (e.g., XmlParsingError, YamlParsingError, ParseError,
            UnsupportedConversionError, InvalidConfigError) may be raised as subclasses of this
            base exception.
    """

    def __new__(cls, config_path: str | PathLike[str]) -> XmlToArrowParser:
        """Initializes the parser with a configuration file path.

        The configuration is loaded, validated, and compiled here, once. Reuse a
        single parser instance across many files to amortize this setup cost.

        Args:
            config_path: The path to the YAML configuration file.

        Raises:
            OSError: If the configuration file cannot be opened (e.g.
                FileNotFoundError); the message includes the path.
            Xml2ArrowError: If the configuration file cannot be loaded, parsed, or
                validated. Because validation happens at construction time, an
                invalid config (e.g. InvalidConfigError) is raised here rather
                than on the first ``parse()`` call.
        """

    @staticmethod
    def from_yaml_str(yaml: str) -> XmlToArrowParser:
        """Creates a parser from a YAML configuration string.

        Args:
            yaml: The configuration in YAML format — the same schema a
                configuration file uses.

        Returns:
            A new parser instance.

        Raises:
            Xml2ArrowError: If the configuration cannot be parsed
                (YamlParsingError) or fails validation (e.g.
                InvalidConfigError).
        """

    def parse(
        self,
        source: str | PathLike[str] | bytes | bytearray | memoryview | IO[Any],
    ) -> dict[str, RecordBatch]:
        """Parses an XML source and returns a dictionary of Arrow RecordBatches.

        In-memory inputs (``bytes`` and ``bytearray``) take a zero-copy fast
        path; ``io.BytesIO`` and buffer-protocol exporters (``memoryview``,
        NumPy ``uint8`` arrays, ...) are snapshotted with a single copy.
        Paths and file-like objects (including ``mmap.mmap``) stream through
        a buffered reader. The GIL is released while parsing, so threads
        sharing one parser instance can parse different sources in parallel.

        Args:
            source: The XML to parse. Accepts a path (``str`` or ``os.PathLike``),
                an in-memory buffer (``bytes``, ``bytearray``, or any object
                exporting the buffer protocol), or any readable file-like
                object.

        Returns:
            A dictionary where keys are table names (strings) and values are
            PyArrow RecordBatch objects.

        Raises:
            OSError: If ``source`` is a path that cannot be opened (e.g.
                FileNotFoundError); the message includes the path.
            ValueError: If ``source`` is a ``str`` holding XML content rather
                than a file path.
            TypeError: If ``source`` is not a supported input type.
            Xml2ArrowError: If an error occurs during XML parsing or Arrow
                table creation. This can include errors such as invalid XML,
                incorrect configuration, or unsupported data types. Exceptions
                raised by a file-like object's ``read()`` method propagate
                unchanged.
        """

    def __repr__(self) -> str: ...
    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Supports pickling, and therefore multiprocessing.

        Path-built parsers re-read their configuration file in the child
        process; ``from_yaml_str`` parsers carry the configuration inside
        the pickle.
        """

class Xml2ArrowError(Exception): ...
class XmlParsingError(Xml2ArrowError): ...
class YamlParsingError(Xml2ArrowError): ...
class ParseError(Xml2ArrowError): ...
class UnsupportedConversionError(Xml2ArrowError): ...
class InvalidConfigError(Xml2ArrowError): ...

def _get_version() -> str:
    """Returns the version of the xml2arrow package."""
