"""Shared pytest fixtures for xml2arrow tests."""

from pathlib import Path
from typing import Generator

import pytest
from xml2arrow import XmlToArrowParser


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return the path to the test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def stations_parser(test_data_dir: Path) -> XmlToArrowParser:
    """Create a parser configured for the stations test data."""
    config_path = test_data_dir / "stations.yaml"
    return XmlToArrowParser(config_path)


@pytest.fixture
def simple_config(tmp_path: Path) -> Path:
    """Create a simple test configuration file."""
    config = tmp_path / "config.yaml"
    config.write_text(
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
    return config


@pytest.fixture
def simple_xml(tmp_path: Path) -> Path:
    """Create a simple test XML file."""
    xml = tmp_path / "data.xml"
    xml.write_text("<root><item>test</item></root>")
    return xml


@pytest.fixture
def config_factory(tmp_path: Path):
    """Factory fixture for creating custom configuration files.

    Usage:
        def test_something(config_factory):
            config_path = config_factory('''
                tables:
                  - name: test
                    xml_path: /root
                    ...
            ''')
    """

    def _create_config(content: str) -> Path:
        config = tmp_path / "config.yaml"
        config.write_text(content)
        return config

    return _create_config


@pytest.fixture
def xml_factory(tmp_path: Path):
    """Factory fixture for creating custom XML files.

    Usage:
        def test_something(xml_factory):
            xml_path = xml_factory('<root><item>value</item></root>')
    """

    def _create_xml(content: str) -> Path:
        xml = tmp_path / "data.xml"
        xml.write_text(content)
        return xml

    return _create_xml


@pytest.fixture
def parser_factory(tmp_path: Path) -> Generator:
    """Factory fixture for creating parser instances with custom configs.

    Usage:
        def test_something(parser_factory):
            parser = parser_factory('''
                tables:
                  - name: test
                    xml_path: /root
                    ...
            ''')
    """

    def _create_parser(config_content: str) -> XmlToArrowParser:
        config = tmp_path / "config.yaml"
        config.write_text(config_content)
        return XmlToArrowParser(config)

    yield _create_parser
