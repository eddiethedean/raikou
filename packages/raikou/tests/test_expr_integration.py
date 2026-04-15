from __future__ import annotations

import pytest

from raikou import col, lit


@pytest.mark.spark
def test_col_and_lit_helpers_smoke(spark) -> None:
    df = spark.createDataFrame([{"x": 1}])
    out = [r[0] for r in df.select((col("x") + lit(1)).alias("y")).collect()]
    assert out == [2]
