# Contributing

Thanks for helping improve **raikou** and **raikou-core**.

## Setup

```bash
pip install -e ./packages/raikou-core -e "./packages/raikou[dev]"
```

Run tests from the repository root:

```bash
pytest
```

Coverage gate:

```bash
make coverage
```

Lint and format (same paths as `Makefile` and CI):

```bash
make lint
make format
```

Or explicitly:

```bash
ruff check packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests
ruff format packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests
ty check packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests
```

## Releases

- Bump **both** `packages/raikou-core/pyproject.toml` and `packages/raikou/pyproject.toml`
  to the same `[project].version`.
- Update **CHANGELOG.md** (fold **Unreleased** into the new section).
- Ensure **CI** is green on `main`.
- Tag `vX.Y.Z` and push the tag.

