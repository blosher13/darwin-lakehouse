"""Validates the Power BI project.

1. Every JSON file against the schema named in its "$schema" (schemas and their $refs are
   fetched from Microsoft's public json-schemas repository, so this needs network access).
2. Every field a visual uses exists in the TMDL semantic model as the right kind (column/measure).

Exit code 1 on any violation - used in CI.

    python powerbi/validate_pbir.py
"""

from __future__ import annotations

import json
import re
import sys
from functools import cache
from pathlib import Path

import requests
from jsonschema import Draft7Validator
from referencing import Registry, Resource

HERE = Path(__file__).resolve().parent
PUBLIC = "https://developer.microsoft.com/json-schemas/"
RAW = "https://raw.githubusercontent.com/microsoft/json-schemas/main/"


@cache
def fetch(uri: str) -> dict:
    # developer.microsoft.com serves the same files as the GitHub repo; GitHub is faster and stable
    response = requests.get(uri.replace(PUBLIC, RAW), timeout=30)
    response.raise_for_status()
    return response.json()


def retrieve(uri: str) -> Resource:
    return Resource.from_contents(fetch(uri))


def model_fields() -> set[tuple[str, str, str]]:
    """(kind, table, name) for every column and measure in the TMDL model."""
    fields = set()
    for tmdl in (HERE / "DarwinRail.SemanticModel" / "definition" / "tables").glob("*.tmdl"):
        table = None
        for line in tmdl.read_text().splitlines():
            if m := re.match(r"^table (\S+)", line):
                table = m.group(1)
            elif m := re.match(r"^\t(column|measure) (?:'([^']+)'|(\S+))", line):
                fields.add((m.group(1).capitalize(), table, m.group(2) or m.group(3)))
    return fields


def field_references(node):
    """Yields (kind, table, name) for every Column/Measure reference in a visual definition."""
    if isinstance(node, dict):
        for kind in ("Column", "Measure"):
            ref = node.get(kind)
            if isinstance(ref, dict) and "Property" in ref:
                yield kind, ref["Expression"]["SourceRef"]["Entity"], ref["Property"]
        for value in node.values():
            yield from field_references(value)
    elif isinstance(node, list):
        for value in node:
            yield from field_references(value)


def check_field_references() -> int:
    fields = model_fields()
    missing = 0
    for path in sorted(HERE.rglob("visual.json")):
        for kind, table, name in set(field_references(json.loads(path.read_text()))):
            if (kind, table, name) not in fields:
                print(f"FAIL {path.relative_to(HERE)}: {kind} {table}[{name}] is not in the semantic model")
                missing += 1
    print(f"{'ok  ' if not missing else 'FAIL'} field references checked against {len(fields)} model fields")
    return missing


def main() -> int:
    registry = Registry(retrieve=retrieve)
    files = sorted(p for p in HERE.rglob("*") if p.suffix in (".json", ".pbir", ".pbism", ".pbip")
                   and "StaticResources" not in p.parts)
    failures = 0
    for path in files:
        document = json.loads(path.read_text())
        schema_uri = document.get("$schema")
        if not schema_uri:
            print(f"SKIP {path.relative_to(HERE)} (no $schema)")
            continue
        schema = fetch(schema_uri)
        validator = Draft7Validator({**schema, "$id": schema_uri}, registry=registry)
        errors = sorted(validator.iter_errors(document), key=lambda e: list(e.path))
        for error in errors:
            print(f"FAIL {path.relative_to(HERE)} at /{'/'.join(map(str, error.path))}: {error.message[:300]}")
        failures += bool(errors)
        if not errors:
            print(f"ok   {path.relative_to(HERE)}")
    print(f"\n{len(files) - failures}/{len(files)} files valid")
    failures += check_field_references()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
