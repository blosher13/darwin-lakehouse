"""Settings loaded from the project's .env file.

The same file is mounted into every container (Kafka Connect, the Kafka Streams app and the
reference sync), so all secrets live in one place. Set DARWIN_ENV_FILE to use another path.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings:
    def __init__(self, env_file: str | os.PathLike | None = None):
        self.env_file = Path(env_file or os.getenv("DARWIN_ENV_FILE") or PROJECT_ROOT / ".env")
        if not self.env_file.exists():
            raise FileNotFoundError(f"{self.env_file} not found - copy .env.example to .env and fill it in")
        self._values = {k: (v or "").strip() for k, v in dotenv_values(self.env_file).items()}

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key) or default

    def require(self, key: str) -> str:
        value = self._values.get(key)
        if not value:
            raise KeyError(f"'{key}' is missing from {self.env_file} (see .env.example)")
        return value

    @property
    def catalog(self) -> str:
        """Unity Catalog catalog for all tables (Free Edition workspaces have 'workspace')."""
        return self.get("databricks_catalog", "workspace")

    @property
    def schema(self) -> str:
        return self.get("databricks_schema", "darwin")

    def table(self, name: str) -> str:
        """Fully qualified name, e.g. workspace.darwin.pushport_bronze."""
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def databricks_url(self) -> str:
        """Workspace URL without any path, e.g. https://dbc-123.cloud.databricks.com."""
        parsed = urlparse(self.require("databricks_host"))
        return f"{parsed.scheme}://{parsed.netloc}"

    def append(self, values: dict[str, str]) -> None:
        """Adds new keys to the .env file (used by the one-off workspace setup)."""
        with open(self.env_file, "a") as f:
            f.write("\n" + "".join(f"{k}={v}\n" for k, v in values.items()))
        self._values.update(values)
