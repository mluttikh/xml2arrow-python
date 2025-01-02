"""Defines exceptions raised by the xml2arrow package."""

from ._xml2arrow import (
    NoTableOnStackError,
    ParseError,
    TableNotFoundError,
    UnsupportedDataTypeError,
    Xml2ArrowError,
    XmlParsingError,
    YamlParsingError,
)

__all__ = [
    "Xml2ArrowError",
    "XmlParsingError",
    "YamlParsingError",
    "UnsupportedDataTypeError",
    "TableNotFoundError",
    "NoTableOnStackError",
    "ParseError",
]
