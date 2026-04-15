from __future__ import annotations

import pytest

from raikou import RaikouDataFrame, Schema, col, connect


class Row(Schema):
    x: int
    y: str


@pytest.mark.spark
def test_raikou_dataframe_select_filter(spark) -> None:
    sdf = spark.createDataFrame([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
    df = RaikouDataFrame[Row].from_spark_dataframe(sdf)
    out = df.filter(col("x") > 1).select("y").to_dict()
    assert out == {"y": ["b"]}


def test_connect_accepts_configs() -> None:
    # Smoke: exercise the config loop without needing a running cluster.
    # If Spark is unavailable in the environment, skip.
    try:
        s = connect(app_name="raikou-config-test", master="local[1]", spark_ui_enabled="false")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark is not available: {exc!r}")
    finally:
        try:
            s.stop()
        except Exception:
            pass
