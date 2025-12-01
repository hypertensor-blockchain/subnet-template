# py-libp2p-subnet

A Python libp2p subnet template framework implementation.

⚠️ This is a work in progress and is not yet ready for production use.

## Installation

### From Source

Clone the repository and install:

```bash
git clone https://github.com/hayotensor/py-libp2p-subnet.git
cd py-libp2p-subnet
python -m venv .venv
source .venv/bin/activate
pip install .
```

### Development Installation

For development, install with dev dependencies:

```bash
pip install -e ".[dev]"
```

```python
# Import your package
import subnet

# Add usage examples here
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=subnet --cov-report=html

# Run specific test file
pytest tests/test_example.py
```

### Code Quality

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing

Run all quality checks:

```bash
make lint
make test
```

### Pre-commit Hooks

Install pre-commit hooks:

```bash
pre-commit install
```

## Documentation

Coming soon...

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
