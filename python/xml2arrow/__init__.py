"""This module provides functionality to parse XML files into Arrow tables based on a configuration."""

from . import exceptions
from ._xml2arrow import Xml2ArrowError, XmlToArrowParser, _get_version

__version__: str = _get_version()

__all__ = ["XmlToArrowParser", "Xml2ArrowError", "exceptions"]
