# API guide

This guide focuses on the user-facing `raikou` package.

## Core types

- `Schema`: a Pydantic v2 model used to describe row shape.
- `RaikouDataFrame[SchemaT]`: a small DataFrame-style wrapper over a Spark DataFrame plus a `raikou-core` plan.

## Common workflow

1. Create/get a SparkSession via `connect()`
2. Create a Spark DataFrame
3. Wrap it with `RaikouDataFrame[YourSchema].from_spark_dataframe(...)`
4. Build transformations (`filter`, `select`, `with_columns`)
5. Collect via `to_dict()`

## Supported operations (today)

`raikou` currently exposes a small subset of operations:

- `select(*cols)`
- `filter(condition)`
- `with_columns(**new_cols)`
- `to_dict()`

Under the hood these map to `raikou-core` plan operations applied to Spark.

