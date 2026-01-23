"""Example test file."""

import subnet

# pytest tests/test_example.py::test_version -rP


def test_version():
    """Test that version is defined."""
    assert hasattr(subnet, "__version__")
    assert isinstance(subnet.__version__, str)
