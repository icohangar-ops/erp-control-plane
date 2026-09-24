"""Cross-platform equivalent of check_connector_flags.sh."""

import re
import sys
from pathlib import Path


def main() -> int:
    guide = Path("docs/CONNECTOR_GUIDE.md")
    if not guide.is_file():
        print(f"FAIL: missing file {guide}", file=sys.stderr)
        return 1
    text = guide.read_text(encoding="utf-8")
    checks = {
        "flags section heading": r"^## Connector-wave flags",
        "UOM deferral decision": r"Canonical conversion-factor column: deferred",
        "UOM reversal condition": r"Reversal condition:",
        "BisTrack invoice-scan assumption": r"site-global invoice numbering",
        "BisTrack safe path": r"per-type invoice_document_types",
        "DMSi header/detail nesting": r"parallel rowset",
        "DMSi confirm-at-onboarding": r"confirmed at onboarding",
        "Spruce SOAP NDA gate": r"NDA-gated",
    }
    missing = [
        label for label, pattern in checks.items() if not re.search(pattern, text, re.MULTILINE)
    ]
    if missing:
        print("FAIL: " + ", ".join(missing), file=sys.stderr)
        return 1
    print(f"OK: connector-wave flags recorded in {guide} with decisions and reversal conditions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
