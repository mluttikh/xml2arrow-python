"""Tests for the streaming API: parse_batches, parse_single_table, schema.

The load-bearing property mirrors the upstream crate's contract: for any
source and batch limits, concatenating a table's streamed batches equals the
table ``parse()`` returns. Everything else here checks the Python-facing
behavior of the boundary: iterator protocol, GIL-safe file-like sources,
error delivery mid-stream, and the pyarrow-native reader.
"""

import io
from pathlib import Path

import pyarrow as pa
import pytest

from xml2arrow import RecordBatchStream, XmlToArrowParser
from xml2arrow.exceptions import InvalidConfigError, ParseError, Xml2ArrowError

SINGLE_TABLE_CONFIG = """
tables:
  - name: items
    xml_path: /root
    levels: [item]
    fields:
      - name: value
        xml_path: /root/item/value
        data_type: Int32
      - name: label
        xml_path: /root/item/label
        data_type: Utf8
"""


def items_xml(n: int) -> str:
    rows = "".join(f"<item><value>{i}</value><label>row {i}</label></item>" for i in range(n))
    return f"<root>{rows}</root>"


@pytest.fixture
def items_parser(config_factory) -> XmlToArrowParser:
    return XmlToArrowParser(config_factory(SINGLE_TABLE_CONFIG))


def collect(stream: RecordBatchStream) -> dict[str, list[pa.RecordBatch]]:
    grouped: dict[str, list[pa.RecordBatch]] = {}
    for name, batch in stream:
        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows >= 1, "streamed batches must never be empty"
        grouped.setdefault(name, []).append(batch)
    return grouped


# --- parse_batches -----------------------------------------------------------


@pytest.mark.parametrize("max_rows", [1, 3, 8192])
def test_streamed_batches_concat_to_parse_result(
    stations_parser: XmlToArrowParser, test_data_dir: Path, max_rows: int
) -> None:
    xml_path = test_data_dir / "stations.xml"
    full = stations_parser.parse(xml_path)
    stream = stations_parser.parse_batches(xml_path, max_rows_per_batch=max_rows)
    assert isinstance(stream, RecordBatchStream)

    grouped = collect(stream)
    for name, batches in grouped.items():
        assert pa.Table.from_batches(batches) == pa.Table.from_batches([full[name]])
    # Every table that has rows must appear; empty tables yield no batches.
    for name, batch in full.items():
        assert (name in grouped) == (batch.num_rows > 0)


def test_max_rows_per_batch_controls_batch_sizes(items_parser: XmlToArrowParser) -> None:
    stream = items_parser.parse_batches(items_xml(10).encode(), max_rows_per_batch=4)
    sizes = [batch.num_rows for _, batch in stream]
    assert sizes == [4, 4, 2]


def test_max_bytes_per_batch_triggers_flushes(items_parser: XmlToArrowParser) -> None:
    # Tiny byte budget: every row's "row N" label alone exceeds it, so each
    # batch carries exactly one row.
    stream = items_parser.parse_batches(items_xml(5).encode(), max_bytes_per_batch=1)
    sizes = [batch.num_rows for _, batch in stream]
    assert sizes == [1, 1, 1, 1, 1]


def test_parse_batches_accepts_all_source_types(
    items_parser: XmlToArrowParser, tmp_path: Path
) -> None:
    xml = items_xml(6)
    path = tmp_path / "items.xml"
    path.write_text(xml)

    expected = items_parser.parse(xml.encode())["items"]
    sources = [
        xml.encode(),  # bytes
        bytearray(xml.encode()),  # bytearray
        str(path),  # str path
        path,  # PathLike
        io.BytesIO(xml.encode()),  # binary file-like
        io.StringIO(xml),  # text file-like
    ]
    for source in sources:
        grouped = collect(items_parser.parse_batches(source, max_rows_per_batch=2))
        assert pa.Table.from_batches(grouped["items"]) == pa.Table.from_batches([expected]), (
            f"round-trip mismatch for source {type(source).__name__}"
        )


def test_stream_is_fused_after_exhaustion(items_parser: XmlToArrowParser) -> None:
    stream = items_parser.parse_batches(items_xml(2).encode())
    assert len(list(stream)) == 1
    with pytest.raises(StopIteration):
        next(stream)
    with pytest.raises(StopIteration):
        next(stream)
    assert "exhausted" in repr(stream)


def test_error_mid_stream_after_valid_batches(items_parser: XmlToArrowParser) -> None:
    # Rows 0-1 stream out as one-row batches; row 2's non-integer value must
    # raise from the iterator, after which it is exhausted.
    xml = (
        "<root>"
        "<item><value>0</value><label>a</label></item>"
        "<item><value>1</value><label>b</label></item>"
        "<item><value>oops</value><label>c</label></item>"
        "</root>"
    )
    stream = items_parser.parse_batches(xml.encode(), max_rows_per_batch=1)
    assert next(stream)[1].num_rows == 1
    assert next(stream)[1].num_rows == 1
    with pytest.raises(ParseError, match="oops"):
        next(stream)
    with pytest.raises(StopIteration):
        next(stream)


def test_file_like_read_exception_propagates(items_parser: XmlToArrowParser) -> None:
    class ExplodingFile(io.RawIOBase):
        def read(self, _size: int = -1) -> bytes:
            raise ValueError("boom from read()")

    stream = items_parser.parse_batches(ExplodingFile())
    # The original exception type must survive the thread + channel hop.
    with pytest.raises(ValueError, match="boom from read"):
        next(stream)


def test_abandoning_stream_early_is_clean(items_parser: XmlToArrowParser) -> None:
    # Take one batch and drop the stream: the producer thread must wind down
    # without hanging interpreter shutdown (implicitly asserted by pytest
    # exiting) and without raising.
    stream = items_parser.parse_batches(items_xml(10_000).encode(), max_rows_per_batch=1)
    next(stream)
    del stream


# --- parse_single_table ------------------------------------------------------


def test_single_table_reader_is_native_pyarrow(
    items_parser: XmlToArrowParser, tmp_path: Path
) -> None:
    xml = items_xml(10)
    path = tmp_path / "items.xml"
    path.write_text(xml)

    reader = items_parser.parse_single_table(path, max_rows_per_batch=4)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.schema == items_parser.schema("items")

    table = reader.read_all()
    assert table == pa.Table.from_batches([items_parser.parse(path)["items"]])


def test_single_table_reader_from_file_like(items_parser: XmlToArrowParser) -> None:
    # File-likes are slurped up front (GIL-deadlock avoidance), but the
    # result must be identical to the path route.
    xml = items_xml(7)
    reader = items_parser.parse_single_table(io.BytesIO(xml.encode()))
    assert reader.read_all() == pa.Table.from_batches([items_parser.parse(xml.encode())["items"]])


def test_single_table_requires_single_table_config(
    stations_parser: XmlToArrowParser, test_data_dir: Path
) -> None:
    with pytest.raises(InvalidConfigError, match="exactly one table"):
        stations_parser.parse_single_table(test_data_dir / "stations.xml")


def test_single_table_mid_stream_error_surfaces_as_pyarrow_error(
    items_parser: XmlToArrowParser,
) -> None:
    # Errors raised once the reader is being consumed cross Arrow's C stream
    # interface, which carries only a message: they arrive as pyarrow
    # exceptions quoting the original error, not as Xml2ArrowError. Pinning
    # this keeps the documented contract honest — a caller who wants
    # Xml2ArrowError must use parse_batches().
    bad = "<root><item><value>oops</value><label>a</label></item></root>"
    reader = items_parser.parse_single_table(bad.encode())
    with pytest.raises(pa.ArrowException, match="oops") as excinfo:
        reader.read_all()
    assert not isinstance(excinfo.value, Xml2ArrowError)


def test_single_table_config_error_precedes_any_parsing(
    stations_parser: XmlToArrowParser,
) -> None:
    # The config check must fire on the call itself, not from deep inside
    # pyarrow on the first batch — so it stays catchable as InvalidConfigError.
    with pytest.raises(InvalidConfigError):
        stations_parser.parse_single_table(b"not even valid xml")


def test_single_table_reader_streams_to_parquet(
    items_parser: XmlToArrowParser, tmp_path: Path
) -> None:
    # The headline integration: XML -> Parquet without materializing the
    # whole table, via the standard pyarrow writer loop.
    import pyarrow.parquet as pq

    xml = items_xml(100)
    reader = items_parser.parse_single_table(xml.encode(), max_rows_per_batch=16)
    out = tmp_path / "items.parquet"
    with pq.ParquetWriter(out, reader.schema) as writer:
        for batch in reader:
            writer.write_batch(batch)

    assert pq.read_table(out) == pa.Table.from_batches([items_parser.parse(xml.encode())["items"]])


# --- schema ------------------------------------------------------------------


def test_schema_available_without_parsing(items_parser: XmlToArrowParser) -> None:
    schema = items_parser.schema("items")
    assert isinstance(schema, pa.Schema)
    assert schema.names == ["<item>", "value", "label"]
    assert schema.field("<item>").type == pa.uint32()
    assert schema.field("value").type == pa.int32()
    assert schema.field("label").type == pa.string()


def test_schema_unknown_table_raises_key_error(items_parser: XmlToArrowParser) -> None:
    with pytest.raises(KeyError, match="nonexistent"):
        items_parser.schema("nonexistent")


# --- structural (fieldless) tables --------------------------------------------

STRUCTURAL_CONFIG = """
tables:
  - name: outline
    xml_path: /root
    levels: []
    fields: []
  - name: items
    xml_path: /root/group
    levels: [item]
    fields:
      - name: value
        xml_path: /root/group/item/value
        data_type: Int32
"""

STRUCTURAL_XML = (
    b"<root><group><item><value>1</value></item><item><value>2</value></item></group></root>"
)


@pytest.fixture
def structural_parser(parser_factory) -> XmlToArrowParser:  # type: ignore[no-untyped-def]
    return parser_factory(STRUCTURAL_CONFIG)


def test_schema_of_structural_table_raises_key_error(
    structural_parser: XmlToArrowParser,
) -> None:
    # A table with no fields produces no output, so it has no schema — the
    # case schema()'s docstring calls out, distinct from a misspelled name.
    with pytest.raises(KeyError, match="outline"):
        structural_parser.schema("outline")


def test_structural_tables_yield_no_batches(
    structural_parser: XmlToArrowParser,
) -> None:
    names = {name for name, _ in structural_parser.parse_batches(STRUCTURAL_XML)}
    assert names == {"items"}


def test_single_table_ignores_structural_siblings(
    structural_parser: XmlToArrowParser,
) -> None:
    # parse_single_table's reader assumes every batch belongs to the one
    # output table; a config with a structural sibling is where that would
    # break if structural tables ever started emitting.
    reader = structural_parser.parse_single_table(STRUCTURAL_XML)
    assert reader.schema == structural_parser.schema("items")
    assert reader.read_all().column("value").to_pylist() == [1, 2]
