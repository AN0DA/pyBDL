"""Python interface for Local Data Bank (LDB) API."""

import tomllib
from pathlib import Path

from pyldb.client import LDB
from pyldb.config import LDBConfig

# Read version from pyproject.toml (single source of truth)
def _get_version() -> str:
    """Get version from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    try:
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)
            return pyproject["project"]["version"]
    except (OSError, KeyError):
        # Fallback if pyproject.toml is not available (e.g., in installed package)
        return "0.0.1"


__version__ = _get_version()
__all__ = ["LDB", "LDBConfig"]
