from __future__ import annotations

import os
import sys
from typing import Any


def get_or_create_spark(*, app_name: str = "raikou-core", **builder_kwargs: Any) -> Any:
    """
    Return a local SparkSession suitable for tests and simple scripts.

    This helper avoids importing Spark at module import time in downstream users.
    """

    # Force executors/workers to use the current Python executable (common local-dev footgun).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Avoid hostname resolution edge cases in constrained environments.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_DRIVER_HOST", "127.0.0.1")

    from pyspark.sql import SparkSession

    b = SparkSession.builder.appName(app_name)
    if "master" not in builder_kwargs:
        b = b.master("local[2]")
    for k, v in builder_kwargs.items():
        if k == "master":
            b = b.master(str(v))
        elif k == "config":
            # allow passing a dict of spark configs
            if isinstance(v, dict):
                for ck, cv in v.items():
                    b = b.config(str(ck), str(cv))
        else:
            b = b.config(str(k), str(v))
    return b.getOrCreate()
