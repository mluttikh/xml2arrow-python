"""Parse XML files into Arrow tables using a YAML configuration."""

from . import exceptions
from ._xml2arrow import (
    Conversion,
    RecordBatchStream,
    Xml2ArrowError,
    XmlToArrowParser,
    _get_version,
)

__version__: str = _get_version()

__all__ = [
    "Conversion",
    "RecordBatchStream",
    "XmlToArrowParser",
    "Xml2ArrowError",
    "exceptions",
]
