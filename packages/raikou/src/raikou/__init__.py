from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .dataframe import RaikouDataFrame
from .expr import col, lit
from .schema import Schema
from .session import connect

try:
    __version__ = version("raikou")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = [
    "RaikouDataFrame",
    "Schema",
    "__version__",
    "col",
    "connect",
    "lit",
]
