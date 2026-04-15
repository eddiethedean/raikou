from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .engine import SparkExecutionEngine
from .roots import SparkRoot
from .session import get_or_create_spark

try:
    __version__ = version("raikou-core")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = [
    "SparkExecutionEngine",
    "SparkRoot",
    "__version__",
    "get_or_create_spark",
]
