from __future__ import annotations

from pathlib import Path

import pytest

from pydantable_protocol.exceptions import UnsupportedEngineOperationError
from pyspark.sql import functions as F

from raikou_core.engine import SparkExecutionEngine
from raikou_core.roots import SparkRoot


def test_engine_make_plan_fallback_and_literals() -> None:
    eng = SparkExecutionEngine()
    p = eng.make_plan(object())
    assert p.schema_descriptors() == {}
    lit = eng.make_literal(value=123)
    # Avoid importing Spark here; just ensure we got an engine expr.
    assert type(lit).__name__ == "Lit"


def test_engine_async_methods_raise() -> None:
    eng = SparkExecutionEngine()
    with pytest.raises(UnsupportedEngineOperationError):
        eng.collect_batches()


@pytest.mark.spark
def test_apply_plan_drop_rename_rowcount_unique_subset_none(
    spark, eng: SparkExecutionEngine
) -> None:
    df = spark.createDataFrame([{"x": 2, "y": "a"}, {"x": 2, "y": "b"}, {"x": 1, "y": "c"}])
    root = SparkRoot(df)

    plan = eng.make_plan({})
    plan = eng.plan_rename(plan, {"y": "yy"})
    plan = eng.plan_drop(plan, ["x"])
    plan = eng.plan_with_row_count(plan, name="rn", offset=10)
    plan = eng.plan_unique(plan, subset=None, keep="any", maintain_order=False)
    out = eng.execute_plan(plan, root)
    assert "x" not in out
    assert "yy" in out
    assert out["rn"]  # has values


@pytest.mark.spark
def test_apply_plan_explode_posexplode_unnest_via_plan_ops(
    spark, eng: SparkExecutionEngine
) -> None:
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("tags", T.ArrayType(T.IntegerType(), containsNull=False), nullable=True),
            T.StructField(
                "addr",
                T.StructType([T.StructField("street", T.StringType(), nullable=True)]),
                nullable=True,
            ),
        ]
    )
    df = spark.createDataFrame([(1, [1, 2], ("x",))], schema=schema)
    root = SparkRoot(df)

    p0 = eng.make_plan({})
    p1 = eng.plan_explode(p0, ["tags"], outer=True)
    out1 = eng.execute_plan(p1, root)
    assert out1["tags"] == [1, 2]

    p2 = eng.plan_posexplode(p0, list_column="tags", pos_name="pos", value_name="val", outer=False)
    out2 = eng.execute_plan(p2, root)
    assert out2["pos"] == [0, 1]
    assert out2["val"] == [1, 2]

    p3 = eng.plan_unnest(p0, columns=["addr"])
    out3 = eng.execute_plan(p3, root)
    assert out3["addr_street"] == ["x"]
    assert "addr" not in out3


@pytest.mark.spark
def test_apply_plan_global_select_without_marker(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    plan = eng.plan_global_select(eng.make_plan({}), [("sx", F.sum(F.col("x")))])
    out = eng.execute_plan(plan, SparkRoot(df))
    assert out == {"sx": [3]}


@pytest.mark.spark
def test_apply_plan_sort_desc_nulls_first_and_unique_keep_invalid_raises(
    spark, eng: SparkExecutionEngine
) -> None:
    df = spark.createDataFrame([{"x": None}, {"x": 1}, {"x": 2}])
    plan = eng.make_plan({})
    plan = eng.plan_sort(plan, keys=["x"], desc=[True], nulls_last=[False], maintain_order=False)
    out = eng.execute_plan(plan, SparkRoot(df))
    assert out["x"][0] is None

    bad = eng.plan_unique(eng.make_plan({}), subset=["x"], keep="last", maintain_order=False)
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_plan(bad, SparkRoot(df))


@pytest.mark.spark
def test_apply_plan_fill_null_strategy_raises(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([(1,)], schema="x long")
    plan = eng.make_plan({})
    plan = eng.plan_fill_null(plan, subset=None, value=0, strategy="forward")
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_plan(plan, SparkRoot(df))


@pytest.mark.spark
def test_unknown_plan_op_raises_typeerror(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1}])
    plan = eng.make_plan({}).add(("__nope__",))
    with pytest.raises(TypeError, match="Unknown plan op"):
        eng.execute_plan(plan, SparkRoot(df))


@pytest.mark.spark
def test_execute_join_key_length_mismatch_raises(spark, eng: SparkExecutionEngine) -> None:
    left = spark.createDataFrame([{"k": 1}])
    right = spark.createDataFrame([{"k": 1}])
    with pytest.raises(TypeError, match="join keys must be same length"):
        eng.execute_join(
            eng.make_plan({}),
            SparkRoot(left),
            eng.make_plan({}),
            SparkRoot(right),
            left_on=["k", "k2"],
            right_on=["k"],
            how="inner",
            suffix="_r",
        )


@pytest.mark.spark
def test_execute_concat_invalid_how_raises(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1}])
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_concat(
            eng.make_plan({}),
            SparkRoot(df),
            eng.make_plan({}),
            SparkRoot(df),
            how="horizontal",
        )


@pytest.mark.spark
def test_write_parquet_partition_and_mkdir(
    tmp_path: Path, spark, eng: SparkExecutionEngine
) -> None:
    df = spark.createDataFrame([{"p": 1, "x": 1}, {"p": 2, "x": 2}])
    root = SparkRoot(df)
    plan = eng.make_plan({})

    out_path = tmp_path / "nested" / "out.parquet"
    eng.write_parquet(plan, root, str(out_path), partition_by=["p"], mkdir=True)
    assert out_path.exists()


@pytest.mark.spark
def test_write_csv_write_kwargs_branch(tmp_path: Path, spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1}])
    root = SparkRoot(df)
    plan = eng.make_plan({})
    out_dir = tmp_path / "csv_opts"
    eng.write_csv(plan, root, str(out_dir), write_kwargs={"header": "true"})
    assert out_dir.exists()
