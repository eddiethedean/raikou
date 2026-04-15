.PHONY: help test coverage coverage-html lint format format-check build build-check upload-pypi install-editable clean

help:
	@echo "raikou monorepo"
	@echo ""
	@echo " make install-editable  pip install -e packages/raikou-core -e packages/raikou"
	@echo " make test              pytest"
	@echo " make coverage          pytest with coverage gate"
	@echo " make coverage-html     pytest + htmlcov/"
	@echo " make lint              ruff check (src + tests)"
	@echo " make format            ruff format (src + tests)"
	@echo " make format-check      ruff format --check"
	@echo " make build             python -m build + twine check (package)"
	@echo " make upload-pypi       build then twine upload (needs creds)"
	@echo " make clean             remove dist/ and build/ under packages/*"

install-editable:
	python -m pip install -e "./packages/raikou-core[dev]" -e "./packages/raikou[dev]"

test:
	pytest

coverage:
	pytest --cov=packages/raikou-core/src/raikou_core --cov=packages/raikou/src/raikou --cov-report=term-missing --cov-fail-under=74

coverage-html:
	pytest --cov=packages/raikou-core/src/raikou_core --cov=packages/raikou/src/raikou --cov-report=term-missing --cov-report=html --cov-fail-under=74

lint:
	ruff check packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests

format:
	ruff format packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests

format-check:
	ruff format --check packages/raikou-core/src packages/raikou/src packages/raikou-core/tests packages/raikou/tests

build-check: build

build:
	cd packages/raikou-core && rm -rf dist build && python -m build && python -m twine check dist/*
	cd packages/raikou && rm -rf dist build && python -m build && python -m twine check dist/*

# Requires: pip install build twine ; PyPI token via TWINE_USERNAME / TWINE_PASSWORD.
upload-pypi: build
	cd packages/raikou-core && python -m twine upload dist/*
	cd packages/raikou && python -m twine upload dist/*

clean:
	rm -rf packages/raikou-core/dist packages/raikou-core/build
	rm -rf packages/raikou/dist packages/raikou/build

