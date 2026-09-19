"""csv_sftp — validated CSV-drop / SFTP connector (the reference implementation)."""

from connectors.csv_sftp.connector import (
    AUDIT_PROMOTED,
    AUDIT_QUARANTINED,
    AUDIT_SKIPPED_DUPLICATE,
    CsvSftpConnector,
    FileQuarantined,
)
from connectors.csv_sftp.manifest import Manifest, ManifestError, ManifestFile, load_manifest

__all__ = [
    "AUDIT_PROMOTED",
    "AUDIT_QUARANTINED",
    "AUDIT_SKIPPED_DUPLICATE",
    "CsvSftpConnector",
    "FileQuarantined",
    "Manifest",
    "ManifestError",
    "ManifestFile",
    "load_manifest",
]
