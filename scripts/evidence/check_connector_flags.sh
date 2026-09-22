#!/usr/bin/env bash
# Evidence check: connector-wave flags are recorded with decision + reversal condition.
#
# Backs the evidence/matrix.yaml rows for the flags collected across the
# connector wave and recorded in docs/CONNECTOR_GUIDE.md ("Connector-wave
# flags"). SIGPIPE discipline: never pipe into `grep -q` under `set -o pipefail`
# (writer can hit SIGPIPE exit 141 and a pipefail turns the race into a spurious
# failure) — every check uses `grep -c` exit-status/count forms.
set -u

GUIDE="docs/CONNECTOR_GUIDE.md"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# check_contains FILE PATTERN LABEL — at least one match required.
check_contains() {
    local file="$1" pattern="$2" label="$3" count
    [ -f "$file" ] || fail "$label: missing file $file"
    count=$(grep -c -E "$pattern" "$file") || true
    [ "${count:-0}" -ge 1 ] || fail "$label: pattern not documented in $file: $pattern"
}

check_contains "$GUIDE" "^## Connector-wave flags" "flags section heading"
check_contains "$GUIDE" "Canonical conversion-factor column: deferred" "UOM deferral decision"
check_contains "$GUIDE" "Reversal condition:" "UOM deferral reversal condition"
check_contains "$GUIDE" "site-global invoice numbering" "BisTrack invoice-scan assumption"
check_contains "$GUIDE" "per-type invoice_document_types" "BisTrack safe path"
check_contains "$GUIDE" "parallel rowset" "DMSi header/detail nesting"
check_contains "$GUIDE" "confirmed at onboarding" "DMSi confirm-at-onboarding"
check_contains "$GUIDE" "NDA-gated" "Spruce SOAP NDA gate"

echo "OK: connector-wave flags recorded in $GUIDE with decisions and reversal conditions"
exit 0
