# Concepts

`raikou` is a small, typed facade over Spark that is powered by `raikou-core`.

## Two layers

- `raikou`: user-facing helpers (`connect`, `RaikouDataFrame`, `Schema`, `col`/`lit`)
- `raikou-core`: a Spark-backed execution engine + a minimal plan representation

## Plans

In `raikou-core`, transformations are represented as an immutable `SparkPlan` (a sequence of ops).
`SparkExecutionEngine` applies those ops to a Spark DataFrame to produce a new Spark DataFrame.

This allows higher layers to build up a plan without immediately executing it.

## Roots

Execution starts from a *root* Spark DataFrame, wrapped as `SparkRoot(df)`.

## Expressions

Expressions are either:

- engine-owned `SparkExpr` nodes (like `Col("x")`, `Lit(1)`, `BinaryOp("+", ...)`), or
- raw Spark `pyspark.sql.Column` objects.

At execution time, `raikou-core` normalizes them to Spark Columns.

