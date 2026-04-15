from __future__ import annotations

from pathlib import Path

import pytest

from pydantable_protocol.exceptions import UnsupportedEngineOperationError
from pyspark.sql import functions as F

from raikou_core.engine import SparkExecutionEngine
from raikou_core.roots import SparkRoot


@pytest.mark.spark
def test_global_select_with_global_agg_marker(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    plan = eng.make_plan({})
    plan = eng.plan_global_select(plan, [("sx", ("_global_agg", F.sum(F.col("x"))))])
    out = eng.execute_plan(plan, SparkRoot(df))
    assert out == {"sx": [6]}


@pytest.mark.spark
def test_fill_null_subset_vs_all_columns(spark, eng: SparkExecutionEngine) -> None:
    # Provide an explicit schema so Spark doesn't fail inference on all-null columns.
    df = spark.createDataFrame(
        [(None, None), (1, None)],
        schema="x long, y string",
    )
    root = SparkRoot(df)

    p0 = eng.make_plan({})
    p1 = eng.plan_fill_null(p0, subset=["x"], value=0, strategy=None)
    out1 = eng.execute_plan(p1, root)
    assert out1["x"] == [0, 1]
    assert out1["y"] == [None, None]

    p2 = eng.plan_fill_null(p0, subset=None, value=0, strategy=None)
    out2 = eng.execute_plan(p2, root)
    assert out2["x"] == [0, 1]
    # Spark will only fill columns compatible with the given value's type.
    assert out2["y"] == [None, None]


@pytest.mark.spark
def test_drop_nulls_threshold_vs_how(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame(
        [
            {"x": None, "y": None},  # 0 non-null
            {"x": 1, "y": None},  # 1 non-null
            {"x": 1, "y": "a"},  # 2 non-null
        ]
    )
    root = SparkRoot(df)
    p0 = eng.make_plan({})

    # thresh=2 means keep rows with >=2 non-nulls in subset.
    p1 = eng.plan_drop_nulls(p0, subset=["x", "y"], how="any", threshold=2)
    out1 = eng.execute_plan(p1, root)
    assert out1["x"] == [1]
    assert out1["y"] == ["a"]

    # how="all" drops only rows where all subset columns are null.
    p2 = eng.plan_drop_nulls(p0, subset=["x", "y"], how="all", threshold=None)
    out2 = eng.execute_plan(p2, root)
    assert out2["x"] == [1, 1]


@pytest.mark.spark
def test_execute_join_multiple_keys_and_suffix_collision(spark, eng: SparkExecutionEngine) -> None:
    left = spark.createDataFrame([{"k1": 1, "k2": 1, "v": 10}])
    right = spark.createDataFrame([{"k1": 1, "k2": 1, "v": 99}])

    lp = eng.make_plan({})
    rp = eng.make_plan({})
    joined_root, _ = eng.execute_join(
        lp,
        SparkRoot(left),
        rp,
        SparkRoot(right),
        left_on=["k1", "k2"],
        right_on=["k1", "k2"],
        how="inner",
        suffix="_right",
    )
    out = eng.execute_plan(eng.make_plan({}), joined_root)
    assert out["v"] == [10]
    assert out["v_right"] == [99]


@pytest.mark.spark
def test_groupby_agg_supported_ops_and_unsupported_raises(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"k": 1, "x": 1}, {"k": 1, "x": 3}, {"k": 2, "x": 10}])
    root = SparkRoot(df)
    plan = eng.make_plan({})

    out_root, _ = eng.execute_groupby_agg(
        plan,
        root,
        by=["k"],
        aggregations={
            "sx": ("sum", "x"),
            "ax": ("avg", "x"),
            "mn": ("min", "x"),
            "mx": ("max", "x"),
            "cx": ("count", "x"),
        },
    )
    out = eng.execute_plan(eng.make_plan({}), out_root)
    assert set(out) == {"k", "sx", "ax", "mn", "mx", "cx"}

    with pytest.raises(UnsupportedEngineOperationError, match="Unsupported groupby agg op"):
        eng.execute_groupby_agg(plan, root, by=["k"], aggregations={"bad": ("median", "x")})


@pytest.mark.spark
def test_write_csv_separator(tmp_path: Path, spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1, "y": "a"}])
    root = SparkRoot(df)
    plan = eng.make_plan({})

    out_dir = tmp_path / "csv_sc"
    eng.write_csv(plan, root, str(out_dir), separator=ord(";"))
    assert out_dir.exists()
