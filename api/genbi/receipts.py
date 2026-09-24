"""Tool-approval receipts (cubiczan-chp-mcp scheme, [Gov] T2).

A human approval on the control surface — today the ``confirmed_by`` human
lock on answer promotion — is only as good as its binding. An approval given
for one request must not quietly authorize a different one. A receipt binds
the approval cryptographically to:

- the **exact normalized arguments** the tool will execute (canonical JSON);
- the **policy version** in force when the human approved (the governing
  GenBI knobs — row cap, timeout, human-lock policy, read-only target id,
  marts schema — hashed into one version string);
- an **expiry** (tz-aware ISO-8601; a receipt past expiry is dead).

Structure and crypto
--------------------
The MAC is HMAC-SHA256 over the canonical JSON of a **fixed tuple** (every
field below except ``mac``), ``sort_keys`` + compact separators — the same
canonicalization the CHP decision ledger uses for body sealing::

    {"schema", "receipt_id", "tool", "actor", "args", "args_sha256",
     "policy_version", "issued_at", "expires_at", "nonce"}

``args_sha256`` is the SHA-256 of the canonical normalized arguments, carried
explicitly so the binding is checkable without re-serializing the whole args
tree. ``receipt_id`` is the first 16 hex chars of SHA-256 over the same
canonical tuple — deterministic, so any two receipts that differ anywhere
also differ in id.

Verification is fail-closed in every direction: a malformed receipt, an
unknown schema, a MAC mismatch, a tool/actor/policy mismatch, an ambiguous
argument binding (the receipt's args do not normalize to *exactly* the
arguments the tool is about to execute), an expired receipt, or a **replay**
(a receipt id redeemed a second time) — each is refused. Verification and
redemption are one atomic step: a receipt is consumed the moment it verifies.

Consumed-on-verification is deliberate: a receipt that verified but whose
promotion later failed downstream is spent — re-approval is cheap, reusing a
spent approval is not honest.

Key handling
------------
The signing key comes from ``GENBI_APPROVAL_RECEIPT_KEY`` (32 bytes,
hex-encoded). Local/demo runs may use the documented dev default below; in
production mode (``ENVIRONMENT`` in {"production", "prod"}) a missing or
dev-default key is a hard refusal — the service must not sign or verify with
a key the whole internet can read. NEVER commit a real key anywhere.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# --- scheme constants -------------------------------------------------------

RECEIPT_SCHEMA = "cubiczan-chp-mcp/approval-receipt/v1"
PROMOTION_TOOL = "genbi.answers.promote"
POLICY_SCHEMA = "genbi-promotion-policy/v1"

# Documented DEV-ONLY default (public knowledge — never role it to production).
# 32 bytes of fixed hex so local runs and tests are reproducible without a
# committed secret. Production mode refuses to use it (see resolve_receipt_key).
DEV_APPROVAL_RECEIPT_KEY_HEX = "9c1f4d2e6ba5480f83d21c7e5a0946b3f70d8e2c14a6b93d5f802e7c4a1d6b39"

# Fixed MAC tuple — field order is irrelevant (canonical JSON sorts keys) but
# the FIELD SET is the contract: change it and every old receipt fails MAC.
_MAC_TUPLE_FIELDS = (
    "schema",
    "receipt_id",
    "tool",
    "actor",
    "args",
    "args_sha256",
    "policy_version",
    "issued_at",
    "expires_at",
    "nonce",
)

PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod"})


class ReceiptError(Exception):
    """Base class for receipt scheme failures."""


class ReceiptKeyUnavailable(ReceiptError):
    """No usable receipt signing key — fail closed (unset/dev key in production)."""


class ReceiptRejection(ReceiptError):
    """The receipt is not a valid approval for exactly this request."""


# --- canonicalization -------------------------------------------------------


def canonical_json(value: Any) -> str:
    """Canonical JSON: sorted keys, compact separators, UTF-8 friendly."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def normalize_value(value: Any) -> Any:
    """Normalize a value tree for binding: sort mapping keys, drop None.

    Dropping None makes ``{"a": 1, "b": None}`` and ``{"a": 1}`` the same
    binding (optional fields passed explicitly as null vs omitted are not an
    ambiguity a caller can exploit — they normalize identically). Any other
    difference — an extra key, a changed value, a different type — produces a
    different normalized tree and a different binding.
    """
    if isinstance(value, dict):
        return {
            key: normalize_value(item) for key, item in sorted(value.items()) if item is not None
        }
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    return value


def _parse_iso_utc(value: Any, field: str) -> dt.datetime:
    if not isinstance(value, str):
        raise ReceiptRejection(f"{field} must be an ISO-8601 string")
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ReceiptRejection(f"{field} is not valid ISO-8601: {exc}") from exc
    if parsed.tzinfo is None:
        raise ReceiptRejection(f"{field} must be timezone-aware")
    return parsed


# --- key resolution ---------------------------------------------------------


def is_production(env: Mapping[str, str]) -> bool:
    return env.get("ENVIRONMENT", "local").strip().lower() in PRODUCTION_ENVIRONMENTS


def resolve_receipt_key(env: Mapping[str, str]) -> bytes:
    """The receipt HMAC key from the environment, failing closed.

    - ``GENBI_APPROVAL_RECEIPT_KEY`` set -> the hex-decoded 32-byte key
      (invalid hex or wrong length is a refusal, not a fallback).
    - unset in production mode -> refusal (fail closed).
    - unset locally -> the documented dev default.
    """
    raw = env.get("GENBI_APPROVAL_RECEIPT_KEY", "").strip()
    if not raw:
        if is_production(env):
            raise ReceiptKeyUnavailable(
                "GENBI_APPROVAL_RECEIPT_KEY is not set in production mode —"
                " refusing to run approvals without a real receipt key."
            )
        return bytes.fromhex(DEV_APPROVAL_RECEIPT_KEY_HEX)
    try:
        key = bytes.fromhex(raw)
    except ValueError as exc:
        raise ReceiptKeyUnavailable(
            "GENBI_APPROVAL_RECEIPT_KEY must be 64 hex characters (32 bytes)."
        ) from exc
    if len(key) != 32:
        raise ReceiptKeyUnavailable(
            f"GENBI_APPROVAL_RECEIPT_KEY must decode to 32 bytes, got {len(key)}."
        )
    if is_production(env) and hmac.compare_digest(key, bytes.fromhex(DEV_APPROVAL_RECEIPT_KEY_HEX)):
        raise ReceiptKeyUnavailable(
            "GENBI_APPROVAL_RECEIPT_KEY is the documented dev-only key in"
            " production mode — refusing (it is public knowledge)."
        )
    return key


# --- policy version ---------------------------------------------------------


def promotion_policy_version(settings: Any) -> str:
    """Bind receipts to the governing GenBI knobs (a settings snapshot hash)."""
    snapshot = {
        "row_cap": settings.row_cap,
        "statement_timeout_seconds": settings.statement_timeout_seconds,
        "chp_require_human_lock": settings.chp_require_human_lock,
        "superset_readonly_database_id": settings.superset_readonly_database_id,
        "marts_schema": settings.marts_schema,
    }
    digest = hashlib.sha256(canonical_json(snapshot).encode("utf-8")).hexdigest()[:12]
    return f"{POLICY_SCHEMA}#{digest}"


def promotion_args(
    *,
    question: str,
    sql: str,
    answer_date: dt.date,
    backing: Any,
    viz: Any,
) -> dict[str, Any]:
    """The exact arguments the promote tool will execute, binding-shaped."""
    return {
        "question": question,
        "sql": sql,
        "answer_date": answer_date.isoformat(),
        "backing": backing.model_dump(by_alias=True, mode="json") if backing else None,
        "viz": viz.model_dump(by_alias=True, mode="json"),
    }


def args_binding(args: Mapping[str, Any]) -> str:
    """The canonical normalized-args string a receipt binds (and args_sha256 hashes)."""
    return canonical_json(normalize_value(dict(args)))


# --- ledger -----------------------------------------------------------------


# One process-wide lock per ledger path: services are assembled per request, so
# single-use redemption must serialize across instances, not just within one.
_ledger_path_locks: dict[str, threading.Lock] = {}
_ledger_path_locks_guard = threading.Lock()


def _ledger_path_lock(path: Path) -> threading.Lock:
    with _ledger_path_locks_guard:
        return _ledger_path_locks.setdefault(str(path), threading.Lock())


class ApprovalReceiptLedger:
    """Append-only JSONL of issued + redeemed receipts, alongside the CHP ledger."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, entry: dict[str, Any]) -> None:
        line = json.dumps(entry, ensure_ascii=False)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def redemption(self, receipt_id: str) -> dict[str, Any] | None:
        """The redemption event for this receipt id, when it was already spent."""
        for event in self._read_all():
            if event.get("event") == "redeemed" and event.get("receipt_id") == receipt_id:
                return event
        return None

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        """Newest-first receipt events (issued + redeemed)."""
        return list(reversed(self._read_all()))[:limit]

    def redeem_once(self, redemption: dict[str, Any]) -> dict[str, Any] | None:
        """Record a redemption unless this receipt id is already spent.

        The spend-check and the append run under one process-wide per-path
        lock (every service instance for this ledger shares it), so two
        concurrent promotes cannot both observe an unspent receipt.
        """
        with _ledger_path_lock(self.path):
            prior = self.redemption(redemption["receipt_id"])
            if prior is None:
                self.append(redemption)
            return prior

    def _read_all(self) -> list[dict[str, Any]]:
        with self._lock:
            if not self.path.exists():
                return []
            lines = self.path.read_text(encoding="utf-8").splitlines()
        return [json.loads(line) for line in lines if line.strip()]


# --- signer / verifier ------------------------------------------------------


class ApprovalReceiptService:
    """Signs, verifies, and redeems tool-approval receipts for one deployment."""

    def __init__(self, ledger_path: Path, key: bytes) -> None:
        self.key = key
        self.records = ApprovalReceiptLedger(ledger_path)

    @classmethod
    def from_settings(cls, settings: Any) -> ApprovalReceiptService:
        """Resolve the key fail-closed from the settings snapshot (no os.environ read)."""
        env: dict[str, str] = {"ENVIRONMENT": settings.environment}
        if settings.approval_receipt_key:
            env["GENBI_APPROVAL_RECEIPT_KEY"] = settings.approval_receipt_key
        return cls(settings.approval_receipts_path, resolve_receipt_key(env))

    # ------------------------------------------------------------------ sign
    def sign(
        self,
        *,
        tool: str,
        actor: str,
        args: Mapping[str, Any],
        policy_version: str,
        ttl_seconds: int = 3600,
        now: dt.datetime | None = None,
        nonce: str | None = None,
    ) -> dict[str, Any]:
        """Mint one receipt binding this approval to exactly these arguments."""
        now = now or dt.datetime.now(dt.UTC)
        issued_at = now
        expires_at = issued_at + dt.timedelta(seconds=ttl_seconds)
        nonce = nonce or uuid.uuid4().hex
        binding = args_binding(args)
        receipt: dict[str, Any] = {
            "schema": RECEIPT_SCHEMA,
            "receipt_id": "",  # computed over the tuple below (id includes it)
            "tool": tool,
            "actor": actor,
            "args": normalize_value(dict(args)),
            "args_sha256": hashlib.sha256(binding.encode("utf-8")).hexdigest(),
            "policy_version": policy_version,
            "issued_at": issued_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "nonce": nonce,
        }
        receipt["receipt_id"] = hashlib.sha256(canonical_json(receipt).encode("utf-8")).hexdigest()[
            :16
        ]
        mac = hmac.new(self.key, canonical_json(receipt).encode("utf-8"), hashlib.sha256)
        receipt["mac"] = mac.hexdigest()
        # The presented MAC is bearer material: the ledger records that an
        # approval was issued, never the secret that could redeem it.
        public_receipt = {key: value for key, value in receipt.items() if key != "mac"}
        self.records.append(
            {"event": "issued", "receipt_id": receipt["receipt_id"], "receipt": public_receipt}
        )
        return receipt

    # ---------------------------------------------------------------- verify
    def verify_and_redeem(
        self,
        receipt: Any,
        *,
        tool: str,
        args: Mapping[str, Any],
        policy_version: str,
        actor: str | None = None,
        now: dt.datetime | None = None,
    ) -> dict[str, Any]:
        """Verify every binding and consume the receipt; refuse on any mismatch.

        Redemption is the replay defense: the second verify of the same
        receipt id fails even when every other check would pass.
        """
        now = now or dt.datetime.now(dt.UTC)
        receipt = self._structured(receipt)

        self._mac(receipt)
        if receipt["tool"] != tool:
            raise ReceiptRejection(
                f"tool mismatch: receipt binds {receipt['tool']!r}, not {tool!r}"
            )
        if receipt["policy_version"] != policy_version:
            raise ReceiptRejection(
                f"policy version mismatch: receipt binds {receipt['policy_version']!r},"
                f" the governing policy is {policy_version!r}"
            )

        binding = args_binding(args)
        expected_sha = hashlib.sha256(binding.encode("utf-8")).hexdigest()
        if receipt["args_sha256"] != expected_sha or args_binding(receipt["args"]) != binding:
            raise ReceiptRejection(
                "ambiguous binding denied: the receipt does not bind the exact normalized"
                " arguments this tool is about to execute (fail-closed)"
            )

        if actor is not None and receipt["actor"] != actor:
            raise ReceiptRejection(
                f"actor mismatch: receipt approves {receipt['actor']!r}, request claims {actor!r}"
            )

        expires_at = _parse_iso_utc(receipt["expires_at"], "expires_at")
        if now >= expires_at:
            raise ReceiptRejection(f"receipt expired at {expires_at.isoformat()}")

        redemption = {
            "event": "redeemed",
            "receipt_id": receipt["receipt_id"],
            "tool": receipt["tool"],
            "actor": receipt["actor"],
            "args_sha256": receipt["args_sha256"],
            "policy_version": receipt["policy_version"],
            "redeemed_at": now.isoformat(),
            "expires_at": receipt["expires_at"],
        }
        spent = self.records.redeem_once(redemption)
        if spent is not None:
            raise ReceiptRejection(
                f"receipt replay refused: {receipt['receipt_id']} was already redeemed at"
                f" {spent.get('redeemed_at')}"
            )
        return redemption

    # --------------------------------------------------------------- internal
    @staticmethod
    def _structured(receipt: Any) -> dict[str, Any]:
        if not isinstance(receipt, dict):
            raise ReceiptRejection("receipt must be a JSON object")
        missing = [field for field in (*_MAC_TUPLE_FIELDS, "mac") if field not in receipt]
        if missing:
            raise ReceiptRejection(f"malformed receipt: missing field(s) {', '.join(missing)}")
        if receipt["schema"] != RECEIPT_SCHEMA:
            raise ReceiptRejection(
                f"unknown receipt schema {receipt['schema']!r} (expected {RECEIPT_SCHEMA!r})"
            )
        for field in ("tool", "actor", "policy_version", "nonce"):
            if not isinstance(receipt[field], str) or not receipt[field]:
                raise ReceiptRejection(f"{field} must be a non-empty string")
        _parse_iso_utc(receipt["issued_at"], "issued_at")
        return receipt

    def _mac(self, receipt: dict[str, Any]) -> None:
        tuple_json = canonical_json({field: receipt[field] for field in _MAC_TUPLE_FIELDS})
        expected = hmac.new(self.key, tuple_json.encode("utf-8"), hashlib.sha256).hexdigest()
        provided = receipt["mac"]
        if not isinstance(provided, str) or not hmac.compare_digest(expected, provided):
            raise ReceiptRejection("MAC mismatch: receipt is not authentic for this deployment")
