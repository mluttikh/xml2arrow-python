"""Defines exceptions raised by the xml2arrow package."""

from ._xml2arrow import (
    InvalidConfigError,
    ParseError,
    UnsupportedConversionError,
    Xml2ArrowError,
    XmlParsingError,
    YamlParsingError,
)

__all__ = [
    "Xml2ArrowError",
    "XmlParsingError",
    "YamlParsingError",
    "ParseError",
    "UnsupportedConversionError",
    "InvalidConfigError",
]
