import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from xml2arrow import XmlToArrowParser
from xml2arrow.exceptions import ParseError, YamlParsingError


@pytest.fixture(scope="module")
def test_data_dir() -> Path:
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="module")
def parser(test_data_dir: Path) -> XmlToArrowParser:
    config_path = test_data_dir / "stations.yaml"
    return XmlToArrowParser(config_path)


def test_xml_to_arrow_parser(parser: XmlToArrowParser, test_data_dir: Path) -> None:
    xml_path = test_data_dir / "stations.xml"
    record_batches = parser.parse(xml_path)

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
    parser: XmlToArrowParser, test_data_dir: Path
) -> None:
    xml_path = test_data_dir / "stations.xml"
    with open(xml_path, "r") as f:
        record_batches = parser.parse(f)
    assert "report" in record_batches
    assert "stations" in record_batches
    assert "measurements" in record_batches


def test_xml_to_arrow_parser_repr(parser: XmlToArrowParser) -> None:
    repr_str = repr(parser)
    assert repr_str.startswith("XmlToArrowParser(config_path='")
    assert repr_str.endswith("stations.yaml')")


def test_xml_to_arrow_yaml_parsing_error() -> None:
    with pytest.raises(YamlParsingError):
        with tempfile.NamedTemporaryFile(mode="w+") as f:
            # Empty file
            XmlToArrowParser(f.name)


def test_xml_to_arrow_parse_parse_error(parser: XmlToArrowParser) -> None:
    with pytest.raises(ParseError):
        with tempfile.TemporaryFile(mode="w+") as f:
            f.write(r"""
                <report>
                    <monitoring_stations>
                        <monitoring_station>
                            <location>
                                <latitude>not float</latitude>
                            </location>
                        </monitoring_station>
                    </monitoring_stations>
                </report>
            """)
            f.flush()  # Ensure data is written to the file
            f.seek(0)  # Reset the file pointer to the beginning
            parser.parse(f)
