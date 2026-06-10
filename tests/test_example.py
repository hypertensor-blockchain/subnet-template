"""Example test file."""

import pytest

import subnet

# python -m pytest tests/test_example.py::test_version -rP


def test_version():
    """Test that version is defined."""
    assert hasattr(subnet, "__version__")
    assert isinstance(subnet.__version__, str)


# python -m pytest tests/test_example.py::test_async -rP


@pytest.mark.asyncio
async def test_async():
    """Test that version is defined."""
    assert hasattr(subnet, "__version__")
    assert isinstance(subnet.__version__, str)
