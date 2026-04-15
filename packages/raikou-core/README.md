# raikou-core

Spark-backed execution core used by [`raikou`](https://pypi.org/project/raikou/).

`raikou-core` implements an `ExecutionEngine` compatible with
[`pydantable-protocol`](https://pypi.org/project/pydantable-protocol/) and
contains the planning/expression primitives that `raikou` builds on.

## Who should use this package?

- **Most users**: install and use [`raikou`](https://pypi.org/project/raikou/) (higher-level API).
- **Library authors / advanced users**: depend on `raikou-core` directly if you want
  the engine/planner without the `raikou` convenience layer.

## Requirements

- **Python**: 3.10+
- **Spark**: `pyspark>=3.4,<4` (a Spark runtime must be available in your environment)

## Install

```bash
pip install raikou-core
```

## Development (monorepo)

From the repository root:

```bash
make install-editable
make test
make lint
make format
```

## Related

- **High-level API**: [`raikou`](https://pypi.org/project/raikou/)
- **Documentation**: built from `docs/` via MkDocs (`mkdocs.yml`)

## License

MIT (see repository `LICENSE`).

