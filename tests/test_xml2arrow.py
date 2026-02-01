"""Tests for the xml2arrow package.

This module contains tests for the XmlToArrowParser class and related functionality.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from xml2arrow import XmlToArrowParser
from xml2arrow.exceptions import (
    ParseError,
    UnsupportedConversionError,
    YamlParsingError,
)


def test_xml_to_arrow_parser(
    stations_parser: XmlToArrowParser, test_data_dir: Path
) -> None:
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
        for elem, exp_elem in zip(stations[key], expected_stations[key]):
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
        for elem, exp_elem in zip(measurements[key], expected_measurements[key]):
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


def test_xml_to_arrow_parser_file(
    stations_parser: XmlToArrowParser, test_data_dir: Path
) -> None:
    """Test parsing XML from a file-like object.

    Verifies that the parser can accept an open file handle
    in addition to file paths.
    """
    xml_path = test_data_dir / "stations.xml"
    with open(xml_path, "r") as f:
        record_batches = stations_parser.parse(f)
    assert "report" in record_batches
    assert "stations" in record_batches
    assert "measurements" in record_batches


def test_xml_to_arrow_parser_repr(stations_parser: XmlToArrowParser) -> None:
    """Test the string representation of XmlToArrowParser.

    Verifies that __repr__ returns the expected format with the config path.
    """
    repr_str = repr(stations_parser)
    assert repr_str.startswith("XmlToArrowParser(config_path='")
    assert repr_str.endswith("stations.yaml')")


def test_xml_to_arrow_yaml_parsing_error() -> None:
    """Test that an empty YAML config file raises YamlParsingError.

    Verifies proper error handling when the configuration file
    is empty or malformed.
    """
    with pytest.raises(YamlParsingError):
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml") as f:
            # Empty file
            XmlToArrowParser(f.name)


def test_xml_to_arrow_parse_parse_error(
    stations_parser: XmlToArrowParser,
) -> None:
    """Test that invalid data values raise ParseError.

    Verifies that attempting to parse a non-numeric string
    as a float raises the appropriate error.
    """
    with pytest.raises(ParseError):
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
            f.flush()  # Ensure data is written to the file
            f.seek(0)  # Reset the file pointer to the beginning
            stations_parser.parse(f)


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
    with pytest.raises(UnsupportedConversionError) as excinfo:
        XmlToArrowParser(config_path)

    assert "Scaling is only supported for Float32 and Float64" in str(excinfo.value)
    assert "Int32" in str(excinfo.value)


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
    assert len(record_batches) == 4, (
        "Expected 4 tables to be created (including empty ones)"
    )

    # Check that all table names are present
    assert "metadata" in record_batches, "metadata table should exist"
    assert "comments" in record_batches, (
        "comments table should exist (even though empty)"
    )
    assert "items" in record_batches, "items table should exist"
    assert "categories" in record_batches, (
        "categories table should exist (even though empty)"
    )

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
    assert comments_batch.num_columns == 2, (
        "comments table should have 2 columns (index + text)"
    )
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
