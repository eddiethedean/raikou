from __future__ import annotations

import os
import sys

import pytest

from raikou_core.session import get_or_create_spark


@pytest.mark.spark
def test_get_or_create_spark_sets_env_and_accepts_config_dict() -> None:
    os.environ.pop("PYSPARK_PYTHON", None)
    os.environ.pop("PYSPARK_DRIVER_PYTHON", None)

    spark = get_or_create_spark(
        app_name="raikou-session-test",
        master="local[1]",
        config={"spark.ui.enabled": "false"},
    )
    try:
        assert os.environ["PYSPARK_PYTHON"] == sys.executable
        assert os.environ["PYSPARK_DRIVER_PYTHON"] == sys.executable
    finally:
        spark.stop()

