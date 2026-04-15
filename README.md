# raikou-core

Spark execution core for typed DataFrame engines.

This repository publishes **`raikou-core`**, a standalone Python package that:

- depends on **`pydantable-protocol`** (engine protocols + shared exceptions)
- depends on **`pyspark`**
- implements a Spark-backed `ExecutionEngine` suitable for use by `pydantable` and `raikou`

## Development

```bash
make install-editable
make test
make lint
make format
make build
```

