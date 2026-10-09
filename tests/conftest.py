import gzip
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def gzipped(tmp_path):
    """Returns a function that gzips a fixture file (Darwin ships every file gzipped)."""
    def _gzip(name: str) -> Path:
        target = tmp_path / f"{name}.gz"
        target.write_bytes(gzip.compress((FIXTURES / name).read_bytes()))
        return target
    return _gzip
