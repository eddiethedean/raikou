# Raikou

Spark helpers for Python built on a small Spark execution core.

[![CI](https://github.com/eddiethedean/raikou/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/raikou/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

This repository contains two packages:

- **`raikou`**: high-level conveniences (`connect`, `RaikouDataFrame`, `Schema`, `col` / `lit`)
- **`raikou-core`**: Spark-backed `ExecutionEngine` implementation compatible with **`pydantable-protocol`**

Docs live in `docs/` and are published via MkDocs.

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
print(out)
```

## Repository layout

- `packages/raikou-core`: engine + planning/expression helpers
- `packages/raikou`: user-facing API built on `raikou-core`
- `docs/`: documentation site (MkDocs Material)

## Development

```bash
make install-editable
make test
make lint
make format
make build
```

