# Contributing

Thanks for helping improve **raikou** and **raikou-core**.

## Setup

From the repository root:

```bash
make install-editable
```

## Common commands

```bash
make test
make coverage
make lint
make format
```

## Releases

- Bump **both** `packages/raikou-core/pyproject.toml` and `packages/raikou/pyproject.toml` to the same version.
- Update `CHANGELOG.md`.
- Ensure CI is green on `main`.
- Tag `vX.Y.Z` and push the tag.

