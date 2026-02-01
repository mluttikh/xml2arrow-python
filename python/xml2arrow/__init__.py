"""Parse XML files into Arrow tables using a YAML configuration."""

from . import exceptions
from ._xml2arrow import Xml2ArrowError, XmlToArrowParser, _get_version

__version__: str = _get_version()

__all__ = ["XmlToArrowParser", "Xml2ArrowError", "exceptions"]
