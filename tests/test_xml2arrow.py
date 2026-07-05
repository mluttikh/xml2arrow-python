"""Tests for the xml2arrow package.

This module contains tests for the XmlToArrowParser class and related functionality.
"""

import array
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from xml2arrow import XmlToArrowParser
from xml2arrow.exceptions import (
    InvalidConfigError,
    ParseError,
    UnsupportedConversionError,
    Xml2ArrowError,
    XmlParsingError,
    YamlParsingError,
)


def test_xml_to_arrow_parser(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test the main XML to Arrow parsing workflow.

    Verifies:
    - All expected tables are created (report, stations, measurements)
    - Data values match expected results
    - Schema types are correct for all fields
    - Index columns are properly generated for nested structures
    """
    xml_path = test_data_dir / "stations.xml"
    record_batches = stations_parser.parse(xml_path)

    # Check if the correct tables are returned
    assert "report" in record_batches
    assert "stations" in record_batches
    assert "measurements" in record_batches

    # Expected data as lists of dictionaries
    expected_report = {
        "title": ["Meteorological Station Data"],
        "created_by": ["National Weather Service"],
        "creation_time": ["2024-12-30T13:59:15Z"],
        "document_type": [None],
    }
    expected_stations = {
        "<station>": [0, 1],
        "id": ["MS001", "MS002"],
        "latitude": [-61.39110565185547, 11.891496658325195],
        "longitude": [48.08662796020508, 135.09336853027344],
        "elevation": [547.1051025390625, 174.5334930419922],
        "description": [
            "Located in the Arctic Tundra area, used for Scientific Research.",
            "Located in the Desert area, used for Weather Forecasting.",
        ],
        "install_date": ["2024-03-31", "2024-01-17"],
    }
    expected_measurements = {
        "<station>": [0, 0, 1, 1, 1, 1],
        "<measurement>": [0, 1, 0, 1, 2, 3],
        "timestamp": [
            "2024-12-30T12:39:15Z",
            "2024-12-30T12:44:15Z",
            "2024-12-30T12:39:15Z",
            "2024-12-30T12:44:15Z",
            "2024-12-30T12:49:15Z",
            "2024-12-30T12:54:15Z",
        ],
        "temperature": [
            308.6365454803261,
            302.24516664449385,
            297.94184295363226,
            288.30369054184587,
            269.12744428486087,
            299.0029205426442,
        ],
        "pressure": [
            95043.9973486407,
            104932.15015450517,
            98940.54287187706,
            100141.3052919951,
            100052.25751769921,
            95376.2785698162,
        ],
        "humidity": [
            49.77716576844861,
            32.5687148391251,
            57.70794884397625,
            45.45094598045342,
            70.40117458947834,
            42.62088244545566,
        ],
    }

    # Compare RecordBatches directly and check types
    report_batch = record_batches["report"]
    assert report_batch.to_pydict() == expected_report
    assert report_batch.schema == pa.schema(
        [
            pa.field("title", pa.string(), nullable=False),
            pa.field("created_by", pa.string(), nullable=False),
            pa.field("creation_time", pa.string(), nullable=False),
            pa.field("document_type", pa.string(), nullable=True),
        ]
    )

    stations_batch = record_batches["stations"]
    stations = stations_batch.to_pydict()
    for key in ["<station>", "id", "description", "install_date"]:
        assert stations[key] == expected_stations[key]
    for key in ["latitude", "longitude", "elevation"]:
        for elem, exp_elem in zip(stations[key], expected_stations[key], strict=False):
            assert pytest.approx(elem) == exp_elem
    assert stations_batch.schema == pa.schema(
        [
            pa.field("<station>", pa.uint32(), nullable=False),
            pa.field("id", pa.string(), nullable=False),
            pa.field("latitude", pa.float32(), nullable=False),
            pa.field("longitude", pa.float32(), nullable=False),
            pa.field("elevation", pa.float32(), nullable=False),
            pa.field("description", pa.string(), nullable=False),
            pa.field("install_date", pa.string(), nullable=False),
        ]
    )

    measurements_batch = record_batches["measurements"]
    measurements = measurements_batch.to_pydict()
    for key in ["<station>", "<measurement>", "timestamp"]:
        assert measurements[key] == expected_measurements[key]
    for key in ["temperature", "pressure", "humidity"]:
        for elem, exp_elem in zip(measurements[key], expected_measurements[key], strict=False):
            assert pytest.approx(elem) == exp_elem
    assert measurements_batch.schema == pa.schema(
        [
            pa.field("<station>", pa.uint32(), nullable=False),
            pa.field("<measurement>", pa.uint32(), nullable=False),
            pa.field("timestamp", pa.string(), nullable=False),
            pa.field("temperature", pa.float64(), nullable=False),
            pa.field("pressure", pa.float64(), nullable=False),
            pa.field("humidity", pa.float64(), nullable=False),
        ]
    )


def test_xml_to_arrow_parser_file(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test parsing XML from a file-like object.

    Verifies that the parser can accept an open file handle
    in addition to file paths.
    """
    xml_path = test_data_dir / "stations.xml"
    with open(xml_path) as f:
        record_batches = stations_parser.parse(f)
    assert "report" in record_batches
    assert "stations" in record_batches
    assert "measurements" in record_batches


def test_xml_to_arrow_parser_repr(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test the string representation of XmlToArrowParser.

    Verifies that __repr__ returns the exact expected format with the full config path.
    """
    repr_str = repr(stations_parser)
    expected_path = str(test_data_dir / "stations.yaml")
    assert repr_str == f"XmlToArrowParser(config_path='{expected_path}')"


def test_xml_to_arrow_yaml_parsing_error() -> None:
    """Test that an empty YAML config file raises YamlParsingError.

    Verifies proper error handling when the configuration file
    is empty or malformed.
    """
    with pytest.raises(YamlParsingError, match=r"(?i)missing field|tables"):
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml") as f:
            # Empty file
            XmlToArrowParser(f.name)


def test_xml_to_arrow_parse_parse_error(
    stations_parser: XmlToArrowParser,
) -> None:
    """Test that invalid data values raise ParseError.

    Verifies that attempting to parse a non-numeric string
    as a float raises the appropriate error, with the field name,
    invalid value, and target type in the message.
    """
    with tempfile.TemporaryFile(mode="w+b") as f:
        f.write(
            rb"""
            <report>
                <monitoring_stations>
                    <monitoring_station>
                        <location>
                            <latitude>not float</latitude>
                        </location>
                    </monitoring_station>
                </monitoring_stations>
            </report>
        """
        )
        f.flush()
        f.seek(0)
        with pytest.raises(ParseError, match=r"not float.*latitude|latitude.*not float"):
            stations_parser.parse(f)


def test_parse_error_boolean(tmp_path: Path) -> None:
    """Test that an invalid boolean value includes the value and field name in the error."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: flag
        xml_path: /root/item/flag
        data_type: Boolean
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    xml_path.write_text("<root><item><flag>maybe</flag></item></root>")

    parser = XmlToArrowParser(config_path)
    with pytest.raises(ParseError, match=r"(?i)maybe.*flag.*boolean|maybe.*boolean.*flag"):
        parser.parse(xml_path)


def test_parse_error_missing_non_nullable(tmp_path: Path) -> None:
    """Test that a missing non-nullable field produces an error with the field name and path."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: count
        xml_path: /root/item/count
        data_type: Int32
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    xml_path.write_text("<root><item></item></root>")

    parser = XmlToArrowParser(config_path)
    with pytest.raises(ParseError, match=r"(?i)missing.*count|count.*missing"):
        parser.parse(xml_path)


def test_unsupported_conversion_error(tmp_path: Path) -> None:
    """Test that applying scale to non-float types raises UnsupportedConversionError.

    Verifies that the scale option is only valid for Float32 and Float64 types,
    and raises an appropriate error when used with integer types.
    Note: The error is raised during config parsing, not XML parsing.
    """
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: test_field
        xml_path: /root/field
        data_type: Int32
        nullable: false
        scale: 2.0
"""

    config_path = tmp_path / "test_config.yaml"
    config_path.write_text(config_yaml)

    # The error is raised during config parsing (XmlToArrowParser instantiation)
    with pytest.raises(
        UnsupportedConversionError,
        match=r"Scaling is only supported for Float32 and Float64.*Int32",
    ):
        XmlToArrowParser(config_path)


def test_unsupported_conversion_error_offset(tmp_path: Path) -> None:
    """Test that applying offset to non-float types raises UnsupportedConversionError."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: test_field
        xml_path: /root/field
        data_type: Utf8
        nullable: false
        offset: 1.0
"""

    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    with pytest.raises(
        UnsupportedConversionError,
        match=r"Offset is only supported for Float32 and Float64.*Utf8",
    ):
        XmlToArrowParser(config_path)


def test_invalid_config_error_duplicate_table(tmp_path: Path) -> None:
    """Test that a configuration with duplicate table names raises InvalidConfigError."""
    config_yaml = """
tables:
  - name: dup
    xml_path: /root
    levels: []
    fields:
      - name: a
        xml_path: /root/a
        data_type: Utf8
        nullable: false
  - name: dup
    xml_path: /root
    levels: []
    fields:
      - name: b
        xml_path: /root/b
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    with pytest.raises(InvalidConfigError, match=r"Duplicate table name 'dup'"):
        XmlToArrowParser(config_path)


def test_invalid_config_error_field_path_not_under_table(tmp_path: Path) -> None:
    """Test that a field xml_path outside its table's xml_path raises InvalidConfigError."""
    config_yaml = """
tables:
  - name: t
    xml_path: /root/inside
    levels: []
    fields:
      - name: stray
        xml_path: /root/outside/stray
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    with pytest.raises(InvalidConfigError, match=r"not under table"):
        XmlToArrowParser(config_path)


def test_empty_tables_are_created(tmp_path: Path) -> None:
    """Test that tables are created even when they have no matching XML elements.

    Verifies that:
    - All configured tables are present in the output
    - Empty tables have the correct schema
    - Tables with data are populated correctly
    """
    # Create a config with multiple tables, some of which won't have matching
    # XML elements
    config_yaml = """
tables:
  - name: metadata
    xml_path: /
    levels: []
    fields:
      - name: title
        xml_path: /report/header/title
        data_type: Utf8
        nullable: false
      - name: created_by
        xml_path: /report/header/created_by
        data_type: Utf8
        nullable: false
  - name: comments
    xml_path: /report/header/comments
    levels:
      - comment
    fields:
      - name: text
        xml_path: /report/header/comments/comment
        data_type: Utf8
        nullable: true
  - name: items
    xml_path: /report/data/items
    levels:
      - item
    fields:
      - name: id
        xml_path: /report/data/items/item/@id
        data_type: Utf8
        nullable: false
      - name: text
        xml_path: /report/data/items/item
        data_type: Utf8
        nullable: false
  - name: categories
    xml_path: /report/header/categories
    levels:
      - category
    fields:
      - name: name
        xml_path: /report/header/categories/category
        data_type: Utf8
        nullable: true
"""

    # XML that only contains data for some of the configured tables
    xml_data = """
<report>
    <header>
        <title>Test Report</title>
        <created_by>System</created_by>
    </header>
    <data>
        <items>
            <item id="1">Value 1</item>
            <item id="2">Value 2</item>
        </items>
    </data>
</report>
"""

    config_path = tmp_path / "test_empty_config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "test_empty_data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    record_batches = parser.parse(xml_path)

    # All tables should be present, even those with no matching XML elements
    assert len(record_batches) == 4, "Expected 4 tables to be created (including empty ones)"

    # Check that all table names are present
    assert "metadata" in record_batches, "metadata table should exist"
    assert "comments" in record_batches, "comments table should exist (even though empty)"
    assert "items" in record_batches, "items table should exist"
    assert "categories" in record_batches, "categories table should exist (even though empty)"

    # Verify metadata table has 1 row
    metadata_batch = record_batches["metadata"]
    assert metadata_batch.num_rows == 1
    assert metadata_batch.num_columns == 2  # title, created_by
    metadata_dict = metadata_batch.to_pydict()
    assert metadata_dict["title"] == ["Test Report"]
    assert metadata_dict["created_by"] == ["System"]

    # Verify comments table is empty but has correct schema
    comments_batch = record_batches["comments"]
    assert comments_batch.num_rows == 0, "comments table should have 0 rows"
    assert comments_batch.num_columns == 2, "comments table should have 2 columns (index + text)"
    assert comments_batch.schema == pa.schema(
        [
            pa.field("<comment>", pa.uint32(), nullable=False),
            pa.field("text", pa.string(), nullable=True),
        ]
    )

    # Verify items table has 2 rows
    items_batch = record_batches["items"]
    assert items_batch.num_rows == 2
    assert items_batch.num_columns == 3  # index, id, text
    items_dict = items_batch.to_pydict()
    assert items_dict["<item>"] == [0, 1]
    assert items_dict["id"] == ["1", "2"]
    assert items_dict["text"] == ["Value 1", "Value 2"]

    # Verify categories table is empty but has correct schema
    categories_batch = record_batches["categories"]
    assert categories_batch.num_rows == 0, "categories table should have 0 rows"
    assert categories_batch.num_columns == 2, (
        "categories table should have 2 columns (index + name)"
    )
    assert categories_batch.schema == pa.schema(
        [
            pa.field("<category>", pa.uint32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )


@pytest.mark.parametrize(
    ("data_type", "xml_value", "expected_value", "arrow_type"),
    [
        ("Boolean", "true", True, pa.bool_()),
        ("Boolean", "false", False, pa.bool_()),
        ("Boolean", "1", True, pa.bool_()),
        ("Boolean", "0", False, pa.bool_()),
        ("Boolean", "yes", True, pa.bool_()),
        ("Boolean", "no", False, pa.bool_()),
        ("Int8", "127", 127, pa.int8()),
        ("Int8", "-128", -128, pa.int8()),
        ("UInt8", "255", 255, pa.uint8()),
        ("UInt8", "0", 0, pa.uint8()),
        ("Int16", "32767", 32767, pa.int16()),
        ("Int16", "-32768", -32768, pa.int16()),
        ("UInt16", "65535", 65535, pa.uint16()),
        ("UInt16", "0", 0, pa.uint16()),
        ("Int32", "2147483647", 2147483647, pa.int32()),
        ("Int32", "-2147483648", -2147483648, pa.int32()),
        ("UInt32", "4294967295", 4294967295, pa.uint32()),
        ("UInt32", "0", 0, pa.uint32()),
        ("Int64", "9223372036854775807", 9223372036854775807, pa.int64()),
        ("Int64", "-9223372036854775808", -9223372036854775808, pa.int64()),
        ("UInt64", "18446744073709551615", 18446744073709551615, pa.uint64()),
        ("UInt64", "0", 0, pa.uint64()),
        ("Float32", "3.14", pytest.approx(3.14, rel=1e-5), pa.float32()),
        ("Float32", "-0.0", pytest.approx(0.0), pa.float32()),
        ("Float64", "3.141592653589793", pytest.approx(3.141592653589793), pa.float64()),
        ("Float64", "-1.7e308", pytest.approx(-1.7e308), pa.float64()),
        ("Utf8", "hello world", "hello world", pa.string()),
        ("Utf8", "", "", pa.string()),
    ],
    ids=lambda val: str(val) if not isinstance(val, pa.DataType) else "",
)
def test_data_type_parsing(
    tmp_path: Path,
    data_type: str,
    xml_value: str,
    expected_value: object,
    arrow_type: pa.DataType,
) -> None:
    """Test that all supported data types are correctly parsed and produce the right Arrow type."""
    config_yaml = f"""
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item/value
        data_type: {data_type}
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_data = f"<root><item><value>{xml_value}</value></item></root>"
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 1
    assert batch.schema.field("value").type == arrow_type
    assert batch.to_pydict()["value"][0] == expected_value


@pytest.mark.parametrize(
    ("data_type", "xml_value", "arrow_type"),
    [
        ("Boolean", "true", pa.bool_()),
        ("Int8", "42", pa.int8()),
        ("UInt8", "42", pa.uint8()),
        ("Int16", "42", pa.int16()),
        ("UInt16", "42", pa.uint16()),
        ("Int32", "42", pa.int32()),
        ("UInt32", "42", pa.uint32()),
        ("Int64", "42", pa.int64()),
        ("UInt64", "42", pa.uint64()),
        ("Float32", "3.14", pa.float32()),
        ("Float64", "3.14", pa.float64()),
        ("Utf8", "hello", pa.string()),
    ],
)
def test_data_type_nullable(
    tmp_path: Path,
    data_type: str,
    xml_value: str,
    arrow_type: pa.DataType,
) -> None:
    """Test that nullable fields produce null when the element is missing."""
    config_yaml = f"""
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item/value
        data_type: {data_type}
        nullable: true
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    # Two rows: one with value, one without
    xml_data = f"""<root>
        <item><value>{xml_value}</value></item>
        <item></item>
    </root>"""
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 2
    assert batch.schema.field("value").type == arrow_type
    assert batch.schema.field("value").nullable is True
    values = batch.to_pydict()["value"]
    assert values[0] is not None
    assert values[1] is None


def test_unicode_in_elements_and_attributes(tmp_path: Path) -> None:
    """Test that non-ASCII text in elements and attributes is preserved."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: label
        xml_path: /root/item/@label
        data_type: Utf8
        nullable: false
      - name: value
        xml_path: /root/item/value
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_data = (
        '<root><item label="日本語ラベル"><value>Ünïcödé — «données» 中文 🌍</value></item></root>'
    )
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data, encoding="utf-8")

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 1
    assert batch.to_pydict()["label"] == ["日本語ラベル"]
    assert batch.to_pydict()["value"] == ["Ünïcödé — «données» 中文 🌍"]


def test_xml_with_bom(tmp_path: Path) -> None:
    """Test that XML files with a UTF-8 BOM are parsed correctly."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    # Write UTF-8 BOM (\xef\xbb\xbf) followed by XML content
    xml_path.write_bytes(b"\xef\xbb\xbf<root><item>hello</item></root>")

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 1
    assert batch.to_pydict()["value"] == ["hello"]


def test_large_attribute_value(tmp_path: Path) -> None:
    """Test that very large attribute values are not truncated."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: data
        xml_path: /root/item/@data
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    large_value = "x" * 100_000
    xml_data = f'<root><item data="{large_value}"/></root>'
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 1
    assert batch.to_pydict()["data"] == [large_value]


def test_xml_with_entities_and_cdata(tmp_path: Path) -> None:
    """Test that XML entities and CDATA sections are handled correctly."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: entity_text
        xml_path: /root/item/entity_text
        data_type: Utf8
        nullable: false
      - name: cdata_text
        xml_path: /root/item/cdata_text
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_data = """<root><item>
        <entity_text>&lt;div&gt; &amp; &quot;test&quot;</entity_text>
        <cdata_text><![CDATA[<raw> & "unescaped"]]></cdata_text>
    </item></root>"""
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    batch = result["test_table"]
    assert batch.num_rows == 1
    assert batch.to_pydict()["entity_text"] == ['<div> & "test"']
    assert batch.to_pydict()["cdata_text"] == ['<raw> & "unescaped"']


def test_stop_at_paths(tmp_path: Path) -> None:
    """Test that stop_at_paths stops parsing early and ignores later data."""
    config_yaml = """
parser_options:
  stop_at_paths:
    - /report/header
tables:
  - name: header
    xml_path: /report
    levels: [header]
    fields:
      - name: title
        xml_path: /report/header/title
        data_type: Utf8
        nullable: false
  - name: items
    xml_path: /report/data
    levels:
      - item
    fields:
      - name: value
        xml_path: /report/data/item/value
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_data = """<report>
        <header><title>Report Title</title></header>
        <data>
            <item><value>should not appear</value></item>
        </data>
    </report>"""
    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    # Header should be parsed
    header_batch = result["header"]
    assert header_batch.num_rows == 1
    assert header_batch.to_pydict()["title"] == ["Report Title"]

    # Items should be empty because parsing stopped at /report/header
    items_batch = result["items"]
    assert items_batch.num_rows == 0


def test_parser_reuse_across_files(tmp_path: Path) -> None:
    """Test that a single parser instance can parse multiple XML files."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Int32
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    parser = XmlToArrowParser(config_path)

    for i in range(3):
        xml_path = tmp_path / f"data_{i}.xml"
        xml_path.write_text(f"<root><item>{i * 10}</item></root>")
        result = parser.parse(xml_path)
        batch = result["test_table"]
        assert batch.num_rows == 1
        assert batch.to_pydict()["value"] == [i * 10]


def test_concurrent_parser_reuse(tmp_path: Path) -> None:
    """Test that a parser can be used from multiple threads concurrently."""
    from concurrent.futures import ThreadPoolExecutor

    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Int32
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    # Create XML files before spawning threads
    xml_paths = []
    for i in range(10):
        xml_path = tmp_path / f"data_{i}.xml"
        xml_path.write_text(f"<root><item>{i}</item></root>")
        xml_paths.append((xml_path, i))

    parser = XmlToArrowParser(config_path)

    def parse_file(args: tuple) -> int:
        path, expected = args
        result = parser.parse(path)
        return result["test_table"].to_pydict()["value"][0]

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(parse_file, xml_paths))

    assert results == list(range(10))


def test_pathlib_path_input(tmp_path: Path) -> None:
    """Test that pathlib.Path objects are accepted for both config and XML input."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    xml_path.write_text("<root><item>test</item></root>")

    # Both config_path and xml_path are pathlib.Path objects
    assert isinstance(config_path, Path)
    assert isinstance(xml_path, Path)

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    assert result["test_table"].to_pydict()["value"] == ["test"]


def test_bookstore_namespaced_xml(test_data_dir: Path) -> None:
    """Integration test with namespaced XML, CDATA, entities, comments, and self-closing tags.

    Exercises:
    - Namespace prefixes on elements and attributes (bk:, rv:) — stripped by parser
    - XML entities in text content (&amp;, &quot; via O'Connor)
    - CDATA sections in review text
    - XML comments (<!-- -->) ignored
    - Self-closing elements (<bk:tags/>) producing no child rows
    - Hyphenated element/attribute names (store-name, in-stock)
    - Boolean attribute values
    - Multi-level nesting (books → tags, books → reviews)
    - Unicode author name (Japanese)
    - Varying child counts across parent rows
    """
    config_path = test_data_dir / "bookstore.yaml"
    xml_path = test_data_dir / "bookstore.xml"

    parser = XmlToArrowParser(config_path)
    result = parser.parse(xml_path)

    assert set(result.keys()) == {"store", "books", "tags", "reviews"}

    # Store metadata (single-row, root-level table)
    store = result["store"].to_pydict()
    assert store["store_name"] == ["The Great Bookshop"]
    assert store["currency"] == ["EUR"]

    # Books table — 3 books with namespaced attributes and entity-escaped text
    books = result["books"]
    assert books.num_rows == 3
    books_dict = books.to_pydict()
    assert books_dict["<book>"] == [0, 1, 2]
    assert books_dict["id"] == ["B001", "B002", "B003"]
    assert books_dict["in_stock"] == [True, False, True]
    assert books_dict["title"] == [
        "The Art of Programming",
        "Data & Algorithms",  # &amp; entity decoded
        "Empty Spaces: A Meditation",
    ]
    assert books_dict["author"] == [
        'Jane O\'Connor & Bob "The Coder" Smith',  # entities decoded
        "María García-López",  # accented characters
        "田中太郎",  # Japanese
    ]
    assert books_dict["price"] == [
        pytest.approx(29.99),
        pytest.approx(45.50),
        pytest.approx(15.00),
    ]
    assert books_dict["pages"] == [384, 612, 128]
    assert books.schema.field("pages").type == pa.uint16()

    # Tags — varying counts per book: 2, 1, 0 (self-closing <bk:tags/>)
    tags = result["tags"]
    assert tags.num_rows == 3
    tags_dict = tags.to_pydict()
    assert tags_dict["<book>"] == [0, 0, 1]
    assert tags_dict["<tag>"] == [0, 1, 0]
    assert tags_dict["tag"] == ["programming", "computer science", "algorithms"]

    # Reviews — book 0 has 2, book 1 has 0 (comment only), book 2 has 1
    reviews = result["reviews"]
    assert reviews.num_rows == 3
    reviews_dict = reviews.to_pydict()
    assert reviews_dict["<book>"] == [0, 0, 2]
    assert reviews_dict["<review>"] == [0, 1, 0]
    assert reviews_dict["rating"] == [5, 4, 3]
    assert reviews_dict["reviewer"] == ["Alice", "Bob", "Charlie"]
    assert reviews_dict["text"] == [
        "Excellent book! <must-read> for all developers.",  # CDATA preserved raw
        "Good but could use more examples.",
        "It was okay — nothing special.",  # em dash
    ]
    assert reviews.schema.field("rating").type == pa.uint8()


@pytest.mark.parametrize(
    ("xml_data", "expected_fragment"),
    [
        # Unclosed tag
        ("<root><item>text</root>", "item"),
        # Mismatched closing tag
        ("<root><item>text</other></root>", "item"),
    ],
    ids=["unclosed-tag", "mismatched-tag"],
)
def test_malformed_xml_raises(tmp_path: Path, xml_data: str, expected_fragment: str) -> None:
    """Test that structurally invalid XML raises XmlParsingError with context."""
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: true
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    xml_path.write_text(xml_data)

    parser = XmlToArrowParser(config_path)
    with pytest.raises(XmlParsingError, match=expected_fragment):
        parser.parse(xml_path)


def test_non_xml_input_is_rejected(tmp_path: Path) -> None:
    """Garbage input is an error, not an empty table.

    ``<<<not xml at all>>>`` parses as an element that never closes, so the
    document is truncated. Returning an empty table for it would be
    indistinguishable from a valid document that happened to contain no rows —
    the silent-data-loss shape this parser refuses.
    """
    config_yaml = """
tables:
  - name: test_table
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: true
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_yaml)

    xml_path = tmp_path / "data.xml"
    xml_path.write_text("<<<not xml at all>>>")

    parser = XmlToArrowParser(config_path)
    with pytest.raises(XmlParsingError, match="Truncated document"):
        parser.parse(xml_path)


def test_parse_bytes_input(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test that parse() accepts bytes input via the zero-copy fast path.

    The result must match what the streaming path produces from the same file.
    """
    xml_path = test_data_dir / "stations.xml"
    xml_bytes = xml_path.read_bytes()

    from_bytes = stations_parser.parse(xml_bytes)
    from_path = stations_parser.parse(xml_path)

    assert set(from_bytes.keys()) == set(from_path.keys())
    for name in from_path:
        assert from_bytes[name].schema == from_path[name].schema
        assert from_bytes[name].to_pydict() == from_path[name].to_pydict()


def test_parse_bytearray_input(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test that parse() accepts bytearray input via the zero-copy fast path."""
    xml_path = test_data_dir / "stations.xml"
    xml_data = bytearray(xml_path.read_bytes())

    from_bytearray = stations_parser.parse(xml_data)
    from_path = stations_parser.parse(xml_path)

    assert set(from_bytearray.keys()) == set(from_path.keys())
    for name in from_path:
        assert from_bytearray[name].to_pydict() == from_path[name].to_pydict()


def test_parse_empty_bytes(parser_factory) -> None:
    """Test that an empty bytes input produces an empty result without crashing."""
    parser = parser_factory(
        """
tables:
  - name: items
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: true
"""
    )
    result = parser.parse(b"")
    assert result["items"].num_rows == 0


def test_parse_bytes_malformed_raises(parser_factory) -> None:
    """Test that malformed XML in a bytes input raises XmlParsingError."""
    parser = parser_factory(
        """
tables:
  - name: items
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: true
"""
    )
    with pytest.raises(XmlParsingError, match="item"):
        parser.parse(b"<root><item>text</root>")


def test_parse_bytes_preserves_unicode(parser_factory) -> None:
    """Test that non-ASCII text in bytes input is decoded correctly."""
    parser = parser_factory(
        """
tables:
  - name: items
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: false
"""
    )
    xml = "<root><item>Ünïcödé 中文 🌍</item></root>".encode()
    batch = parser.parse(xml)["items"]
    assert batch.to_pydict()["value"] == ["Ünïcödé 中文 🌍"]


def test_parse_str_path(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test that parse() accepts a plain str path, not just os.PathLike."""
    record_batches = stations_parser.parse(str(test_data_dir / "stations.xml"))
    assert "stations" in record_batches


def test_parse_missing_file_error_includes_path(
    stations_parser: XmlToArrowParser, tmp_path: Path
) -> None:
    """Test that a nonexistent XML path raises FileNotFoundError naming the file."""
    missing = tmp_path / "nope.xml"
    with pytest.raises(FileNotFoundError, match="nope.xml"):
        stations_parser.parse(missing)
    with pytest.raises(FileNotFoundError, match="nope.xml"):
        stations_parser.parse(str(missing))


def test_missing_config_file_error_includes_path(tmp_path: Path) -> None:
    """Test that a nonexistent config path raises FileNotFoundError naming the file."""
    with pytest.raises(FileNotFoundError, match="no_config.yaml"):
        XmlToArrowParser(tmp_path / "no_config.yaml")


def test_parse_xml_content_as_str_raises_helpful_error(
    stations_parser: XmlToArrowParser,
) -> None:
    """Test that passing XML content as a str gets a hint instead of FileNotFoundError."""
    with pytest.raises(ValueError, match=r"looks like XML content"):
        stations_parser.parse("<report></report>")
    # Leading whitespace and an XML declaration should still trigger the hint
    with pytest.raises(ValueError, match=r"looks like XML content"):
        stations_parser.parse('\n  <?xml version="1.0"?><report/>')


def test_parse_rejects_non_source_types(stations_parser: XmlToArrowParser) -> None:
    """Test that unsupported source types raise TypeError with the documented message."""
    with pytest.raises(TypeError, match=r"path, bytes-like or buffer object, or a file-like"):
        stations_parser.parse(12345)  # type: ignore[arg-type]


def test_file_like_read_exception_propagates(stations_parser: XmlToArrowParser) -> None:
    """Test that an exception raised inside a file-like's read() is re-raised as-is.

    Before the read-error slot in PyBinaryFile, the original exception was
    flattened into an XmlParsingError message string.
    """

    class ExplodingReader:
        def read(self, _size: int) -> bytes:
            raise ConnectionResetError("connection lost mid-stream")

    with pytest.raises(ConnectionResetError, match="connection lost mid-stream"):
        stations_parser.parse(ExplodingReader())


def test_file_like_read_wrong_type_raises_type_error(
    stations_parser: XmlToArrowParser,
) -> None:
    """Test that a read() returning a non-buffer type surfaces as TypeError."""

    class BadReader:
        def read(self, _size: int) -> int:
            return 42

    with pytest.raises(TypeError):
        stations_parser.parse(BadReader())


def test_parse_closed_file_raises_value_error(
    stations_parser: XmlToArrowParser, test_data_dir: Path
) -> None:
    """Test that parsing an already-closed file raises the original ValueError."""
    f = open(test_data_dir / "stations.xml", "rb")
    f.close()
    with pytest.raises(ValueError, match="closed file"):
        stations_parser.parse(f)


def test_parse_releases_the_gil(tmp_path: Path) -> None:
    """Test that parse() releases the GIL so other Python threads keep running.

    A worker thread parses a document sized so that one parse takes ~0.3s on
    this machine; the main thread must be able to execute Python (several
    loop iterations) before the worker finishes. If parse() held the GIL
    throughout, the main thread would freeze until the parse completed,
    accumulating at most the 1-2 iterations it can squeeze in before the
    worker enters the extension call.

    The document size is calibrated rather than hardcoded: a release wheel
    parses roughly an order of magnitude faster than a debug build, and CI
    runners add sleep-granularity noise, so a fixed size either starves the
    counter on fast machines or wastes time on slow ones.
    """
    import threading
    import time

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
tables:
  - name: items
    xml_path: /root
    levels: [item]
    fields:
      - name: value
        xml_path: /root/item/value
        data_type: Int64
        nullable: false
"""
    )
    parser = XmlToArrowParser(config_path)

    def build_doc(items: int) -> bytes:
        return b"<root>" + b"<item><value>12345</value></item>" * items + b"</root>"

    # Calibrate: measure a 200k-item parse, then scale the document so one
    # parse takes ~0.3s (capped at 2M items / ~70 MB to bound memory and
    # runtime on very slow machines).
    target_seconds = 0.3
    items = 200_000
    start = time.perf_counter()
    parser.parse(build_doc(items))
    duration = time.perf_counter() - start
    if duration < target_seconds:
        items = min(int(items * target_seconds / max(duration, 1e-6)), 2_000_000)
    big = build_doc(items)

    started = threading.Event()
    done = threading.Event()
    worker_error: list[BaseException] = []

    def work() -> None:
        started.set()
        try:
            parser.parse(big)
        except BaseException as exc:  # pragma: no cover - only on regression
            worker_error.append(exc)
        finally:
            # Always set, even on failure: the main loop below must not spin
            # forever if the parse raises.
            done.set()

    worker = threading.Thread(target=work)
    worker.start()
    assert started.wait(timeout=10)
    iterations = 0
    deadline = time.monotonic() + 60
    while not done.is_set() and time.monotonic() < deadline:
        iterations += 1
        time.sleep(0.001)
    worker.join(timeout=10)
    assert done.is_set(), "worker never finished parsing"
    assert not worker_error, f"parse() raised in the worker: {worker_error[0]!r}"
    assert iterations >= 3, "main thread was starved: parse() appears to hold the GIL"


@pytest.mark.parametrize(
    "wrap",
    [
        pytest.param(lambda data: memoryview(data), id="memoryview-readonly"),
        pytest.param(lambda data: memoryview(bytearray(data)), id="memoryview-writable"),
        pytest.param(lambda data: array.array("B", data), id="array-B"),
    ],
)
def test_parse_buffer_protocol_inputs(
    stations_parser: XmlToArrowParser, test_data_dir: Path, wrap
) -> None:
    """Test that buffer-protocol exporters are accepted and match the bytes path."""
    data = (test_data_dir / "stations.xml").read_bytes()
    from_buffer = stations_parser.parse(wrap(data))
    from_bytes = stations_parser.parse(data)
    assert set(from_buffer.keys()) == set(from_bytes.keys())
    for name in from_bytes:
        assert from_buffer[name].to_pydict() == from_bytes[name].to_pydict()


def test_parse_mmap_input(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test that an mmap of the XML file parses via the streaming path."""
    import mmap

    with open(test_data_dir / "stations.xml", "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            record_batches = stations_parser.parse(mm)
    assert "stations" in record_batches


def test_parse_bytesio_fast_path(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test BytesIO parsing: content, cursor position honored, cursor left at EOF."""
    import io

    data = (test_data_dir / "stations.xml").read_bytes()
    expected = stations_parser.parse(data)

    bio = io.BytesIO(data)
    result = stations_parser.parse(bio)
    assert set(result.keys()) == set(expected.keys())
    assert bio.tell() == len(data), "cursor should be at EOF, as after read()"

    # Content before the current cursor position must be skipped, exactly as
    # the streaming read() path would.
    with_junk = io.BytesIO(b"JUNK" + data)
    with_junk.seek(4)
    result = stations_parser.parse(with_junk)
    assert set(result.keys()) == set(expected.keys())


def test_parse_stringio_input(stations_parser: XmlToArrowParser, test_data_dir: Path) -> None:
    """Test that StringIO streams through the text-mode file-like path."""
    import io

    text = (test_data_dir / "stations.xml").read_text()
    record_batches = stations_parser.parse(io.StringIO(text))
    assert "stations" in record_batches


def test_from_yaml_str(test_data_dir: Path) -> None:
    """Test that from_yaml_str builds a parser equivalent to the file constructor."""
    yaml = (test_data_dir / "stations.yaml").read_text()
    xml = (test_data_dir / "stations.xml").read_bytes()

    from_str = XmlToArrowParser.from_yaml_str(yaml).parse(xml)
    from_file = XmlToArrowParser(test_data_dir / "stations.yaml").parse(xml)

    assert set(from_str.keys()) == set(from_file.keys())
    for name in from_file:
        assert from_str[name].to_pydict() == from_file[name].to_pydict()


def test_from_yaml_str_repr() -> None:
    """Test that a YAML-string parser's repr does not pretend to have a path."""
    parser = XmlToArrowParser.from_yaml_str(
        """
tables:
  - name: items
    xml_path: /root
    levels: []
    fields:
      - name: value
        xml_path: /root/item
        data_type: Utf8
        nullable: true
"""
    )
    assert repr(parser) == "XmlToArrowParser(from_yaml_str=...)"


def test_from_yaml_str_invalid_yaml_raises() -> None:
    """Test that malformed YAML raises YamlParsingError, like the file constructor."""
    with pytest.raises(YamlParsingError):
        XmlToArrowParser.from_yaml_str("tables: [not: [valid")


def test_from_yaml_str_invalid_config_raises() -> None:
    """Test that from_yaml_str validates the config, like the file constructor."""
    yaml = """
tables:
  - name: dup
    xml_path: /root
    levels: []
    fields:
      - name: a
        xml_path: /root/a
        data_type: Utf8
        nullable: false
  - name: dup
    xml_path: /root
    levels: []
    fields:
      - name: b
        xml_path: /root/b
        data_type: Utf8
        nullable: false
"""
    with pytest.raises(InvalidConfigError, match=r"Duplicate table name 'dup'"):
        XmlToArrowParser.from_yaml_str(yaml)


def test_pickle_roundtrip_path_parser(
    stations_parser: XmlToArrowParser, test_data_dir: Path
) -> None:
    """Test that a path-built parser survives pickling (multiprocessing hand-off)."""
    import pickle

    restored = pickle.loads(pickle.dumps(stations_parser))
    assert repr(restored) == repr(stations_parser)

    xml = (test_data_dir / "stations.xml").read_bytes()
    original = stations_parser.parse(xml)
    reparsed = restored.parse(xml)
    assert set(reparsed.keys()) == set(original.keys())
    for name in original:
        assert reparsed[name].to_pydict() == original[name].to_pydict()


def test_pickle_roundtrip_yaml_parser(test_data_dir: Path) -> None:
    """Test that a from_yaml_str parser pickles without needing any file on disk."""
    import pickle

    yaml = (test_data_dir / "stations.yaml").read_text()
    parser = XmlToArrowParser.from_yaml_str(yaml)
    restored = pickle.loads(pickle.dumps(parser))

    xml = (test_data_dir / "stations.xml").read_bytes()
    assert set(restored.parse(xml).keys()) == set(parser.parse(xml).keys())


def test_exception_hierarchy() -> None:
    """Test that every public exception subclasses Xml2ArrowError and is catchable via it."""
    from xml2arrow import exceptions

    for name in exceptions.__all__:
        exc = getattr(exceptions, name)
        assert issubclass(exc, Xml2ArrowError), f"{name} must subclass Xml2ArrowError"


def test_version_matches_installed_metadata() -> None:
    """Test that the compiled module's version agrees with the installed package's."""
    from importlib.metadata import version

    from xml2arrow import __version__

    assert __version__ == version("xml2arrow")


def test_version_returns_string() -> None:
    """Test that the package version is a non-empty string.

    Verifies that _get_version() returns a valid version string.
    """
    from xml2arrow import __version__

    assert isinstance(__version__, str)
    assert len(__version__) > 0
    # Version should follow semver pattern (at least major.minor.patch)
    parts = __version__.split(".")
    assert len(parts) >= 3, f"Version {__version__} should have at least 3 parts"


def test_warnings_is_empty_for_a_config_with_nothing_to_flag(
    tmp_path: Path,
) -> None:
    """A table that declares its row boundaries has nothing surprising to report."""
    config = tmp_path / "config.yaml"
    config.write_text(
        """
tables:
  - name: items
    xml_path: /root/items
    row: item
    fields:
      - {name: a, path: a, data_type: Int32}
      - {name: b, path: b, data_type: Int32}
"""
    )
    assert XmlToArrowParser(config).warnings() == []


def test_warnings_reports_inferred_row_boundaries(tmp_path: Path) -> None:
    """The lint that matters in practice: rows inferred from several children.

    Without ``row:``, a row ends whenever *any* configured child of the table
    element closes. Here ``title`` and ``created`` are both direct children of
    ``/root/header``, so the table yields *two* half-filled rows per header
    rather than one. The warning is what tells you that before the row counts
    do.

    Note the shape matters: fields nested one level deeper (a single ``<item>``
    child holding them) have only one distinct child element and are correctly
    not flagged.
    """
    config = tmp_path / "config.yaml"
    config.write_text(
        """
tables:
  - name: header
    xml_path: /root/header
    levels: []
    fields:
      - {name: title, xml_path: /root/header/title, data_type: Int32}
      - {name: created, xml_path: /root/header/created, data_type: Int32}
"""
    )
    warnings = XmlToArrowParser(config).warnings()

    assert len(warnings) == 1
    assert isinstance(warnings[0], str)
    # Names the table, so a config with several has an actionable message.
    assert "header" in warnings[0]


def test_warnings_does_not_change_parsing(tmp_path: Path) -> None:
    """Lints are advisory: asking for them must not alter what a parse returns.

    The config is the flagged shape, and the assertion on ``num_rows`` records
    what the warning is warning *about*: one ``<header>`` holding two configured
    child elements produces two half-filled rows, not one row with two columns.
    """
    config = tmp_path / "config.yaml"
    config.write_text(
        """
tables:
  - name: header
    xml_path: /root/header
    levels: []
    fields:
      - {name: title, xml_path: /root/header/title, data_type: Int32, nullable: true}
      - {name: created, xml_path: /root/header/created, data_type: Int32, nullable: true}
"""
    )
    xml = b"<root><header><title>1</title><created>2</created></header></root>"

    before = XmlToArrowParser(config).parse(xml)
    parser = XmlToArrowParser(config)
    assert parser.warnings()  # non-empty, and consumed
    after = parser.parse(xml)

    assert before["header"] == after["header"]
    assert before["header"].num_rows == 2


class TestFromYamlString:
    """Constructing a parser from YAML held in memory rather than on disk."""

    def test_matches_a_parser_built_from_the_same_file(self, tmp_path: Path) -> None:
        """The two constructors must agree, or there are two sets of rules."""
        yaml = """
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: value, path: value, data_type: Int32}
"""
        config = tmp_path / "config.yaml"
        config.write_text(yaml)
        xml = b"<data><item><value>1</value></item><item><value>2</value></item></data>"

        from_file = XmlToArrowParser(config).parse(xml)
        from_string = XmlToArrowParser.from_yaml_str(yaml).parse(xml)

        assert from_file["items"] == from_string["items"]

    def test_repr_says_where_the_config_came_from(self) -> None:
        """No path exists, so __repr__ must not invent one."""
        parser = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: value, path: value, data_type: Int32}
"""
        )
        assert repr(parser) == "XmlToArrowParser(<from YAML string>)"

    def test_malformed_yaml_raises_yaml_parsing_error(self) -> None:
        with pytest.raises(YamlParsingError):
            XmlToArrowParser.from_yaml_str("tables: [oh no: {")

    def test_invalid_config_raises_invalid_config_error(self) -> None:
        """Validation runs here too: a field pointing outside its table."""
        with pytest.raises(InvalidConfigError):
            XmlToArrowParser.from_yaml_str(
                """
tables:
  - name: items
    xml_path: /data
    fields:
      - {name: value, xml_path: /elsewhere/value, data_type: Int32}
"""
            )


class TestConfigFeatures:
    """The configuration features added in xml2arrow 0.20.

    These are config-only: they need no binding surface of their own, which is
    exactly why they need tests here — nothing else would notice if a future
    upgrade stopped honouring one.
    """

    STATIONS = b"""<report>
      <stations>
        <station id="alpha">
          <measurements>
            <m seq="1"><value>1.5</value></m>
            <m seq="2"><value>2.5</value></m>
          </measurements>
        </station>
        <station id="beta">
          <measurements>
            <m seq="3"><value>3.5</value></m>
          </measurements>
        </station>
      </stations>
    </report>"""

    def test_declared_row_gives_one_row_per_element(self) -> None:
        """`row:` replaces inferred boundaries, and says so in the config."""
        parser = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: stations
    xml_path: /report/stations
    row: station
    fields:
      - {name: id, path: "@id", data_type: Utf8}
"""
        )
        batch = parser.parse(self.STATIONS)["stations"]
        assert batch.num_rows == 2
        assert batch.column("id").to_pylist() == ["alpha", "beta"]
        # Declaring the row is what silences the row-boundary lint specifically.
        # Other lints may still fire — this config trips the non-nullable-Utf8
        # one — so assert on the concern rather than on an empty list.
        assert not any("child element" in w for w in parser.warnings())

    def test_relative_field_paths_resolve_against_the_row(self) -> None:
        """`path:` is relative to the row element; the absolute form still works."""
        relative = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: stations
    xml_path: /report/stations
    row: station
    fields:
      - {name: id, path: "@id", data_type: Utf8}
"""
        )
        absolute = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: stations
    xml_path: /report/stations
    row: station
    fields:
      - {name: id, xml_path: /report/stations/station/@id, data_type: Utf8}
"""
        )
        assert (
            relative.parse(self.STATIONS)["stations"] == absolute.parse(self.STATIONS)["stations"]
        )

    def test_parent_link_is_a_real_join_key(self) -> None:
        """`links:` produces a global ordinal, so a join is correct.

        The legacy positional `levels` column would report 0 for both stations
        here, silently attributing beta's measurement to alpha.
        """
        parser = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: stations
    xml_path: /report/stations
    row: station
    fields:
      - {name: id, path: "@id", data_type: Utf8}
  - name: measurements
    xml_path: /report/stations/station/measurements
    row: m
    links:
      - parent: stations
    fields:
      - {name: seq, path: "@seq", data_type: Int32}
"""
        )
        tables = parser.parse(self.STATIONS)
        assert tables["stations"].column("_id").to_pylist() == [0, 1]
        # Two measurements under alpha (0), one under beta (1).
        assert tables["measurements"].column("_stations_id").to_pylist() == [0, 0, 1]

    def test_value_policies_apply(self) -> None:
        """A per-field policy opts out of one historical quirk at a time."""
        parser = XmlToArrowParser.from_yaml_str(
            """
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: n, path: n, data_type: Int32, nullable: true, null_values: ["N/A"]}
      - {name: s, path: s, data_type: Utf8, trim: true}
"""
        )
        batch = parser.parse(b"<data><item><n>N/A</n><s> hi </s></item></data>")["items"]
        assert batch.column("n").to_pylist() == [None]
        assert batch.column("s").to_pylist() == ["hi"]

    def test_version_2_changes_the_defaults(self) -> None:
        """`version: 2` trims every type, where v1 leaves Utf8 as written."""
        body = """
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: s, path: s, data_type: Utf8}
"""
        xml = b"<data><item><s> hi </s></item></data>"

        v1 = XmlToArrowParser.from_yaml_str(body).parse(xml)["items"]
        v2 = XmlToArrowParser.from_yaml_str("version: 2\n" + body).parse(xml)["items"]

        assert v1.column("s").to_pylist() == [" hi "]
        assert v2.column("s").to_pylist() == ["hi"]

    def test_version_2_rejects_an_unmigrated_config(self) -> None:
        """The whole point of the assertion: it fails loudly when not met."""
        with pytest.raises(InvalidConfigError, match="version: 2"):
            XmlToArrowParser.from_yaml_str(
                """
version: 2
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: value, xml_path: /data/item/value, data_type: Int32}
"""
            )
