"""Minimal Databricks REST client: workspace APIs, SQL statements and Volume files."""

from __future__ import annotations

import time
from functools import cached_property
from pathlib import Path
from typing import Any

import requests


class DatabricksError(RuntimeError):
    pass


class DatabricksClient:
    def __init__(self, url: str, token: str, timeout: int = 120):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    @classmethod
    def from_settings(cls, settings) -> DatabricksClient:
        return cls(settings.databricks_url, settings.require("databricks_token"))

    # ------------------------------------------------------------------ REST

    def api(self, method: str, path: str, json: dict | None = None, **kwargs) -> dict[str, Any]:
        response = self.session.request(method, self.url + path, json=json, timeout=self.timeout, **kwargs)
        body = response.json() if response.content else {}
        if not response.ok or "error_code" in body:
            raise DatabricksError(f"{method} {path} -> {response.status_code}: {body}")
        return body

    @cached_property
    def current_user(self) -> str:
        return self.api("GET", "/api/2.0/preview/scim/v2/Me")["userName"]

    # ------------------------------------------------------------------ SQL

    @cached_property
    def warehouse_id(self) -> str:
        warehouses = self.api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
        if not warehouses:
            raise DatabricksError("No SQL warehouse found in the workspace")
        return warehouses[0]["id"]

    def sql(self, statement: str) -> list[list[Any]]:
        """Runs a statement on the SQL warehouse (starting it if needed) and returns the rows."""
        result = self.api("POST", "/api/2.0/sql/statements", {
            "warehouse_id": self.warehouse_id, "statement": statement, "wait_timeout": "50s"})
        while result["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(3)
            result = self.api("GET", f"/api/2.0/sql/statements/{result['statement_id']}")
        if result["status"]["state"] != "SUCCEEDED":
            raise DatabricksError(f"SQL failed: {statement[:300]}\n{result['status']}")
        return result.get("result", {}).get("data_array") or []

    def sql_script(self, script: str) -> None:
        """Runs a multi-statement script whose statements end with ';' at end of line."""
        for statement in script.split(";\n"):
            if statement.strip() and not all(
                    line.strip().startswith("--") for line in statement.strip().splitlines()):
                self.sql(statement)

    # ------------------------------------------------------------------ Volumes

    def upload(self, local_path: Path, volume_path: str) -> None:
        with open(local_path, "rb") as f:
            self.api("PUT", f"/api/2.0/fs/files{volume_path}", params={"overwrite": "true"},
                     headers={"Content-Type": "application/octet-stream"}, data=f)

    def list_directory(self, volume_path: str) -> list[dict[str, Any]]:
        return self.api("GET", f"/api/2.0/fs/directories{volume_path}").get("contents", [])

    def delete_directory(self, volume_path: str) -> None:
        """Deletes a Volume directory and the files directly inside it."""
        for entry in self.list_directory(volume_path):
            self.api("DELETE", f"/api/2.0/fs/files{entry['path']}")
        self.api("DELETE", f"/api/2.0/fs/directories{volume_path}")
