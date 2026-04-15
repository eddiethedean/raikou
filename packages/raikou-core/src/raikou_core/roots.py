from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SparkRoot:
    """Root data wrapper for Spark-backed execution plans."""

    df: Any
