.PHONY: help install install-dev test lint format type coverage clean docs

help:
	@echo "Available commands:"
	@echo "  make install      - Install the package"
	@echo "  make install-dev  - Install package in development mode with dev dependencies"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linting checks"
	@echo "  make format       - Format code with black and isort"
	@echo "  make type         - Run type checking with mypy"
	@echo "  make coverage     - Run tests with coverage report"
	@echo "  fix               - fix formatting & linting issues with ruff"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make docs         - Build documentation"

install:
	pip install .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest tests -n auto

lint:
	black --check subnet tests
	isort --check-only subnet tests
	flake8 subnet tests

format:
	black subnet tests
	isort subnet tests

type:
	mypy subnet

fix:
	python -m ruff check --fix

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

docs:
	cd docs && make html

# protobufs management

PB = subnet/protocols/pb/mock_protocol.proto

PY = $(PB:.proto=_pb2.py)
PYI = $(PB:.proto=_pb2.pyi)

## Set default to `protobufs`, otherwise `format` is called when typing only `make`
all: protobufs

.PHONY: protobufs clean-proto

protobufs: $(PY)

%_pb2.py: %.proto
	protoc --python_out=. --mypy_out=. $<

clean-proto:
	rm -f $(PY) $(PYI)

# Force protobuf regeneration by making them always out of date
$(PY): FORCE

FORCE:
