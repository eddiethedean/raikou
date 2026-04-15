from __future__ import annotations

import pytest


def test_spark_schema_to_descriptors_scalar_types() -> None:
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import types as T

    from raikou_core.schema import spark_schema_to_descriptors

    schema = T.StructType(
        [
            T.StructField("i", T.IntegerType(), nullable=False),
            T.StructField("f", T.DoubleType(), nullable=True),
            T.StructField("b", T.BooleanType(), nullable=True),
            T.StructField("s", T.StringType(), nullable=True),
            T.StructField("bin", T.BinaryType(), nullable=True),
            T.StructField("d", T.DateType(), nullable=True),
            T.StructField("ts", T.TimestampType(), nullable=True),
        ]
    )
    desc = spark_schema_to_descriptors(schema)
    assert desc["i"] == {"base": "int", "nullable": False}
    assert desc["f"]["base"] in ("float", "decimal")
    assert desc["b"]["base"] == "bool"
    assert desc["s"]["base"] == "str"
    assert desc["bin"]["base"] == "binary"
    assert desc["d"]["base"] == "date"
    assert desc["ts"]["base"] == "datetime"


def test_spark_schema_to_descriptors_nested_struct_list_map() -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import types as T

    from raikou_core.schema import spark_schema_to_descriptors

    schema = T.StructType(
        [
            T.StructField(
                "st",
                T.StructType([T.StructField("x", T.LongType(), nullable=True)]),
                nullable=True,
            ),
            T.StructField("xs", T.ArrayType(T.StringType(), containsNull=False), nullable=True),
            T.StructField(
                "m",
                T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=True),
                nullable=True,
            ),
        ]
    )
    desc = spark_schema_to_descriptors(schema)
    assert desc["st"]["kind"] == "struct"
    assert desc["xs"]["kind"] == "list"
    assert desc["m"]["kind"] == "map"

