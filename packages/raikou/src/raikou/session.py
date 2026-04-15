from __future__ import annotations

import os
import sys
from typing import Any


def connect(*, app_name: str = "raikou", master: str = "local[2]", **configs: Any) -> Any:
    """Create (or get) a SparkSession with sensible local defaults."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    b = SparkSession.builder.appName(app_name).master(master)
    for k, v in configs.items():
        b = b.config(str(k), str(v))
    return b.getOrCreate()
