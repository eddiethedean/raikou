# raikou

High-level Spark helpers built on **`raikou-core`**.

PyPI: https://pypi.org/project/raikou/

Repository: https://github.com/eddiethedean/raikou

`raikou` provides:

- SparkSession helpers (`connect`)
- a small DataFrame-style facade (`RaikouDataFrame`) backed by `raikou-core`'s engine
- convenience expression helpers (`col`, `lit`)

## Requirements

- **Python**: 3.10+
- **Spark**: `pyspark>=3.4,<4` (a Spark runtime must be available in your environment)

## Install

```bash
pip install raikou
```

## Quick start

```python
from raikou import RaikouDataFrame, Schema, col, connect


class Row(Schema):
    x: int
    y: str


spark = connect(master="local[2]")
sdf = spark.createDataFrame([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])

df = RaikouDataFrame[Row].from_spark_dataframe(sdf)
out = df.filter(col("x") > 1).select("y").to_dict()
```

## What’s in the box?

- **`connect(...)`**: convenience helper for creating/configuring a `SparkSession`
- **`Schema`**: typed row schema for a `RaikouDataFrame`
- **`RaikouDataFrame[T]`**: a small, typed facade backed by the `raikou-core` engine
- **`col(...)` / `lit(...)`**: expression helpers used with `filter`, `select`, etc.

## Development (monorepo)

From the repository root:

```bash
make install-editable
make test
make lint
make format
```

## Relationship to `raikou-core`

`raikou` depends on [`raikou-core`](https://pypi.org/project/raikou-core/) for the
Spark-backed execution engine and planning/expression layer. If you only need the
engine primitives (e.g. for building another library), you can depend on
`raikou-core` directly.

## License

MIT (see repository `LICENSE`).
