from os import PathLike
from typing import IO, Any, final

from pyarrow import RecordBatch, RecordBatchReader, Schema

__all__ = [
    "XmlToArrowParser",
    "RecordBatchStream",
    "Conversion",
    "Xml2ArrowError",
    "XmlParsingError",
    "YamlParsingError",
    "ParseError",
    "UnsupportedConversionError",
    "InvalidConfigError",
    "ConfigVersion1Warning",
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

        Warns:
            ConfigVersion1Warning: If the configuration uses format version 1,
                which is deprecated. ``to_version_2()`` converts it.
        """

    @staticmethod
    def from_yaml_str(yaml: str) -> XmlToArrowParser:
        """Creates a parser from a YAML configuration string.

        The counterpart to the path constructor, for callers that already hold
        the YAML: an embedded default, a configuration fetched from a service
        or built by a tool, or a test that would rather not touch the
        filesystem. The configuration is validated exactly as a file-loaded one
        is, so a parser obtained either way is equally trustworthy.

        Named to match ``Config::from_yaml_str`` in the underlying Rust crate.

        Args:
            yaml: The YAML configuration.

        Returns:
            A new parser instance.

        Raises:
            YamlParsingError: If the string is not valid YAML, or does not
                describe a configuration.
            InvalidConfigError: If the configuration parses but is not valid.

        Warns:
            ConfigVersion1Warning: If the configuration uses format version 1,
                which is deprecated. ``to_version_2()`` converts it.

        Example:
            >>> parser = XmlToArrowParser.from_yaml_str('''
            ... version: 2
            ... tables:
            ...   - name: items
            ...     scope: /data
            ...     row: item
            ...     fields:
            ...       - {name: value, path: value, data_type: Int32}
            ... ''')
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

    def parse_batches(
        self,
        source: str | PathLike[str] | bytes | bytearray | IO[Any],
        *,
        max_rows_per_batch: int | None = None,
        max_bytes_per_batch: int | None = None,
    ) -> RecordBatchStream:
        """Parses an XML source incrementally, yielding batches with bounded memory.

        Returns an iterator of ``(table_name, batch)`` tuples. A table's batch
        is emitted whenever it reaches ``max_rows_per_batch`` rows or
        ``max_bytes_per_batch`` accumulated value bytes, so memory stays
        bounded by the batch limits instead of the document size — this is the
        entry point for XML files too large to parse with ``parse()``.
        Concatenating a table's batches in yield order reproduces exactly what
        ``parse()`` would have returned for it.

        Parsing happens as you iterate, on the calling thread, releasing the
        GIL for each batch so other Python threads keep running.

        Args:
            source: The XML to parse. Accepts a path (``str`` or
                ``os.PathLike``), an in-memory buffer (``bytes`` or
                ``bytearray``), or any readable file-like object. Unlike
                ``parse()``, in-memory ``bytes`` are copied once.
            max_rows_per_batch: Rows per batch before a flush (default 8192).
            max_bytes_per_batch: Value bytes per batch before a flush
                (default 128 MiB).

        Returns:
            An iterator of ``(str, pyarrow.RecordBatch)`` tuples. Tables with
            no rows yield no batches; every yielded batch has at least one row.

        Raises:
            OSError: If ``source`` is a path that cannot be opened.
            ValueError: If ``source`` is a ``str`` holding XML content rather
                than a file path.
            TypeError: If ``source`` is not a supported input type.
            Xml2ArrowError: Raised from the iterator (not this call) when
                parsing fails mid-stream; batches yielded before the error
                remain valid.
        """

    def parse_single_table(
        self,
        source: str | PathLike[str] | bytes | bytearray | IO[Any],
        *,
        max_rows_per_batch: int | None = None,
        max_bytes_per_batch: int | None = None,
    ) -> RecordBatchReader:
        """Streams the config's single output table as a native pyarrow reader.

        For configurations defining exactly one table with fields — the common
        shape for very large documents — this returns a
        ``pyarrow.RecordBatchReader``, directly consumable by
        ``pyarrow.parquet.ParquetWriter``, ``pyarrow.dataset``, DuckDB, and
        anything else speaking the Arrow C stream protocol. The reader's
        schema is available before any parsing happens.

        Args:
            source: The XML to parse. Accepts a path (``str`` or
                ``os.PathLike``), an in-memory buffer (``bytes`` or
                ``bytearray``), or a readable file-like object (file-like
                objects). Every source is read incrementally, so memory stays
                bounded by the batch limits.
            max_rows_per_batch: Rows per batch before a flush (default 8192).
            max_bytes_per_batch: Value bytes per batch before a flush
                (default 128 MiB).

        Returns:
            The table's batches, in row order.

        Raises:
            InvalidConfigError: If the configuration does not define exactly
                one table with fields. Raised here, before any parsing.
            OSError: If ``source`` is a path that cannot be opened.
            ValueError: If ``source`` is a ``str`` holding XML content rather
                than a file path.
            TypeError: If ``source`` is not a supported input type.

        Note:
            Failures that happen *while* the returned reader is consumed reach
            Python through Arrow's C stream interface, which carries only a
            message. They therefore arrive as ``pyarrow.ArrowException``
            subclasses (typically ``ArrowInvalid``) quoting the original error,
            **not** as ``Xml2ArrowError``. Use ``parse_batches()`` instead when
            you need to catch this package's own exception types.
        """

    def schema(self, table: str) -> Schema:
        """Returns the pyarrow schema of an output table without parsing anything.

        The schema is fully determined by the configuration: the table's key
        and link columns (``_id``, ``_<table>_id``, ...), or in a version 1
        configuration its ``<level>`` columns, followed by the configured
        fields. Useful for setting up schema-first sinks (Parquet writers,
        dataset registrations) before the first batch arrives.

        Args:
            table: The table name as defined in the configuration.

        Returns:
            The table's schema.

        Raises:
            KeyError: If the configuration has no output table of that name
                (structural tables — empty ``fields`` — produce no output).
        """

    def warnings(self) -> list[str]:
        """Returns advisory warnings about the configuration.

        These are configurations that are *valid* but commonly surprising —
        most often a table whose row boundaries are inferred from more than one
        child element, which yields one partially-filled row per child rather
        than one row per record. That rule depends on which fields happen to be
        configured, so adding a column can change a table's row count. A
        configuration that does not declare ``version: 2`` also gets a
        deprecation notice, last, listing what it still needs to change.

        Warnings never change how a document parses, and this package never
        prints them: what to do with them is your decision. Logging them at
        startup is the usual choice.

        Returns:
            One message per finding, in configuration order. Empty when there
            is nothing to flag.

        Example:
            >>> parser = XmlToArrowParser("config.yaml")
            >>> for warning in parser.warnings():
            ...     logging.warning("xml2arrow config: %s", warning)
        """

    def to_version_2(self) -> Conversion:
        """Converts the configuration to format version 2, without changing what it produces.

        The converted configuration always declares ``version: 2``. A part that
        version 2 can only express by changing the output is left as it was and
        listed in ``unconverted``. When that list is empty, every document
        parses to the same tables, columns, values and errors under the
        converted configuration as under this one; otherwise the converted
        configuration does not load until the listed parts are resolved. A
        configuration that already declares ``version: 2`` comes back unchanged.

        The YAML is written fresh: the original's comments and layout are not
        kept, and keys left at their defaults are omitted.

        Returns:
            The converted configuration as YAML, and the parts left for you to
            decide.

        Example:
            >>> conversion = XmlToArrowParser("config.yaml").to_version_2()
            >>> for part in conversion.unconverted:
            ...     print("left for you:", part)
            >>> Path("config-v2.yaml").write_text(conversion.yaml)
        """

    def __repr__(self) -> str: ...

@final
class Conversion:
    """The result of ``XmlToArrowParser.to_version_2``.

    Holds text rather than a parser, because the point of converting is to
    write the result down and review it. ``XmlToArrowParser.from_yaml_str``
    turns it into a parser when one is wanted.
    """

    @property
    def yaml(self) -> str:
        """The converted configuration, as YAML.

        It always declares ``version: 2``, and loads when ``unconverted`` is
        empty. Otherwise every other part is converted, and it does not load
        until the listed parts are resolved.
        """

    @property
    def unconverted(self) -> list[str]:
        """The parts that could not be converted without changing the output, one message each."""

    def __repr__(self) -> str: ...

@final
class RecordBatchStream:
    """Iterator of ``(table_name, batch)`` tuples from ``parse_batches``.

    Parsing happens as you iterate, on the calling thread, with the GIL
    released for each batch. After exhaustion — or after a parsing error is
    raised — the iterator only raises ``StopIteration``. Dropping it early
    stops the parse and releases the input.

    Iterate from a single thread: like any Python iterator this one is not
    thread-safe, and here a concurrent ``__next__`` sees ``StopIteration``
    rather than the batch another thread is taking.
    """

    def __iter__(self) -> RecordBatchStream: ...
    def __next__(self) -> tuple[str, RecordBatch]: ...
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

class ConfigVersion1Warning(DeprecationWarning):
    """Warned when a parser is built from a configuration in format version 1, which is deprecated.

    The message lists what the configuration still needs to change, and
    ``XmlToArrowParser.to_version_2()`` converts it without changing its
    output. Filter this category to silence the warning while you migrate.
    """

def _get_version() -> str:
    """Returns the version of the xml2arrow package."""
