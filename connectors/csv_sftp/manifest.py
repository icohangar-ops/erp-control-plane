"""Manifest (control file) validation for CSV/SFTP drops.

A file is only considered complete when its manifest has arrived and validates:
the manifest declares filenames, SHA-256 checksums, row counts, and the schema
version for the batch (see docs/ARCHITECTURE.md — file-finalization semantics
are unreliable, so the control file is the completeness signal).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


class ManifestError(Exception):
    """The manifest itself is missing or malformed — the whole batch is untrusted."""


@dataclass(frozen=True)
class ManifestFile:
    name: str
    sha256: str
    rows: int
    encoding: str
    delimiter: str


@dataclass(frozen=True)
class Manifest:
    batch_id: str
    generated_at: str
    source_company: str
    schema_version: str
    files: dict[str, ManifestFile]

    def entry_for(self, file_name: str) -> ManifestFile | None:
        return self.files.get(file_name)


def load_manifest(path: Path) -> Manifest:
    """Parse and structurally validate a manifest.json control file."""
    if not path.exists():
        raise ManifestError(
            f"manifest control file not found at {path} — a batch without its manifest is "
            f"treated as incomplete (file-finalization semantics are unreliable)"
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"manifest at {path} is not valid JSON: {exc}") from exc

    missing = [k for k in ("batch_id", "generated_at", "schema_version", "files") if k not in raw]
    if missing:
        raise ManifestError(f"manifest at {path} is missing required keys: {missing}")
    if not isinstance(raw["files"], list) or not raw["files"]:
        raise ManifestError(f"manifest at {path} declares no files")

    files: dict[str, ManifestFile] = {}
    for entry in raw["files"]:
        for key in ("name", "sha256", "rows"):
            if key not in entry:
                raise ManifestError(f"manifest file entry is missing '{key}': {entry}")
        spec = ManifestFile(
            name=entry["name"],
            sha256=str(entry["sha256"]).lower(),
            rows=int(entry["rows"]),
            encoding=entry.get("encoding", "utf-8"),
            delimiter=entry.get("delimiter", ","),
        )
        if spec.name in files:
            raise ManifestError(f"manifest declares file {spec.name} twice")
        files[spec.name] = spec

    return Manifest(
        batch_id=str(raw["batch_id"]),
        generated_at=str(raw["generated_at"]),
        source_company=str(raw.get("source_company", "unknown")),
        schema_version=str(raw["schema_version"]),
        files=files,
    )
