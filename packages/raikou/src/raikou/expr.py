from __future__ import annotations

from typing import Any


def col(name: str) -> Any:
    """Spark column reference."""
    from pyspark.sql import functions as F

    return F.col(name)


def lit(value: Any) -> Any:
    """Spark literal."""
    from pyspark.sql import functions as F

    return F.lit(value)
