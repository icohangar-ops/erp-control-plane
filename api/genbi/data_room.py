"""Fail-closed dual-stage authorized retrieval over acquisition data rooms (Qdrant).

The acquisition surface holds documents with hard sightlines — payroll
registers, pricing books, diligence files — where a deal manager may read a
document an analyst on the same deal must not. Vector search is the wrong
place to invent per-document access control, so this module makes the
authorization decision in exactly one place and defends it at two stages.

Reversal check (why two stages) — verified 2026-09-20 against this repo
-----------------------------------------------------------------------
Qdrant appears here only as the WrenAI AI-service's internal RAG document
store (``docker/compose/genbi/config.yaml``), pinned to OSS ``qdrant/qdrant
v1.11.0`` (digest-pinned in ``wrenai.yml``). Qdrant OSS 1.11 enforces
collection-level API-key auth and evaluates payload filters atomically with
the knN search itself, but has **no document-level ACL engine and no policy
source** — a payload filter is only as good as whatever builds it, and
nothing in this repo builds one. Grepping ``api/ genbi/ connectors/
control_plane/ docker/`` for ACL/authorization/permission middleware finds no
document-level enforcement from any policy source (the only "Authorization"
match is the Superset bearer header; tool-approval receipts bind approvals to
promotions, not document access; WrenAI's AI service reads Qdrant with a
single service role). Conclusion: no store- or middleware-level atomic ACL
exists to make a second stage redundant, so dual-stage is wired. If a future
Qdrant upgrade (or an external authorizer in front of it) enforces
document-level ACLs atomically from one uncached policy source, DELETE the
post-verify stage and this comment's successor — it would be pure latency.

Dual-stage design ([Data] P3; senso-ai retrieval_filter.py/authorization.py
reference)
---------------------------------------------------------------------------
1. **Pre-filter (stage 1).** The principal's grant set is read from the
   policy source and pushed *into* the Qdrant search as a server-side payload
   filter (``doc_id`` in allowed-ids) — the filter is evaluated atomically
   with the vector search, so unauthorized vectors are never scored.
2. **Post-verify (stage 2).** Every returned hit is re-checked against the
   same policy source in a fresh read. A hit the policy does not allow —
   drift between grant read and search, a mis-scoped filter, a payload
   missing its identity fields — is dropped and alarmed, never returned.
3. **Fail closed.** If the policy source cannot be read, retrieval raises
   (HTTP 503); the store is never queried unfiltered. A principal with no
   grants in a room gets an empty result without touching the store. A store
   outage raises too — neither path ever degrades to unfiltered results.
4. **Audit.** Every search — executed, refused, or filtered — is appended to
   the JSONL audit trail before the caller sees the result.

Single-source invariant: ACLs live ONLY in the policy source. Qdrant point
payloads carry document identity (doc_id, room, classification, title) and
never a copy of the ACL — a copied ACL would be a second, cached policy
source and would silently break revocation. Both stages resolve through the
same ``DataRoomAclPolicy`` object, whose ``FileAclPolicySource`` re-reads the
policy file on every decision (uncached by construction). Production swaps in
an external policy-decision point behind the same protocol.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import httpx

# --- scheme constants --------------------------------------------------------

ACL_SCHEMA = "cubiczan-data-room/acl/v1"

# Outcome vocabulary — stable values for dashboards over the audit trail.
EXECUTED = "executed"
POST_VERIFY_REMOVED = "post_verify_removed"
NO_GRANTS = "no_grants"
POLICY_UNAVAILABLE = "policy_unavailable"
STORE_UNAVAILABLE = "store_unavailable"


class DataRoomError(Exception):
    """Base class for data-room retrieval failures."""


class PolicySourceUnavailable(DataRoomError):
    """The policy source could not be read — retrieval refuses to run."""


class DataRoomStoreUnavailable(DataRoomError):
    """The vector store could not be reached — retrieval refuses to run."""


class PolicySchemaInvalid(PolicySourceUnavailable):
    """The policy source is unreadable as this schema — refuse, never guess."""


# --- policy source (single, uncached) -----------------------------------------


class DataRoomAclPolicy(Protocol):
    """The ONE authorization decision surface for data-room retrieval.

    Implementations must read fresh state per call (no memoization): both
    stages re-resolve here, and a revocation must be visible on the next
    search without a deploy.
    """

    def allowed_documents(self, principal: str, room: str) -> frozenset[str]: ...

    def is_allowed(self, principal: str, room: str, doc_id: str) -> bool: ...


@dataclass(frozen=True)
class FileAclPolicySource:
    """Document-level ACLs from a JSON policy file, re-read on every decision.

    Uncached by construction: every call re-reads and re-validates the file,
    so revocations take effect immediately and a missing/corrupt file fails
    closed (``PolicySourceUnavailable``) instead of serving stale grants.
    Production deployments point ``GENBI_DATA_ROOM_POLICY_PATH`` at a
    file dropped by their policy system, or implement this protocol against
    an external policy-decision point — the retrieval service cannot tell
    the difference and must not care.
    """

    path: Path

    def _read(self) -> dict[str, Any]:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise PolicySourceUnavailable(f"ACL policy file not found: {self.path}") from exc
        except OSError as exc:
            raise PolicySourceUnavailable(f"ACL policy file unreadable: {exc}") from exc
        try:
            policy = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PolicySchemaInvalid(f"ACL policy is not valid JSON: {exc}") from exc
        if not isinstance(policy, dict) or policy.get("schema") != ACL_SCHEMA:
            raise PolicySchemaInvalid(f"ACL policy schema must be exactly {ACL_SCHEMA!r}")
        if not isinstance(policy.get("rooms"), dict) or not isinstance(
            policy.get("principals"), dict
        ):
            raise PolicySchemaInvalid("ACL policy needs 'rooms' and 'principals' objects")
        return policy

    def _principal_roles(self, policy: Mapping[str, Any], principal: str) -> frozenset[str]:
        entry = policy["principals"].get(principal)
        if not isinstance(entry, dict):
            return frozenset()
        roles = entry.get("roles", [])
        if not isinstance(roles, list) or not all(isinstance(r, str) for r in roles):
            raise PolicySchemaInvalid(f"principals[{principal!r}].roles must be a list of strings")
        return frozenset(roles)

    @staticmethod
    def _grants_match(grants: Mapping[str, Any], principal: str, roles: frozenset[str]) -> bool:
        """A document is allowed iff its grant list names the principal
        directly (``principal:<identity>``) or one of the principal's roles
        (``role:<role>``). An empty/missing grant list allows nobody."""
        allow = grants.get("allow", [])
        if not isinstance(allow, list) or not all(isinstance(a, str) for a in allow):
            return False  # malformed grants on a document: allow nobody
        wanted = {f"principal:{principal}", *(f"role:{r}" for r in roles)}
        return bool(set(allow) & wanted)

    def is_allowed(self, principal: str, room: str, doc_id: str) -> bool:
        policy = self._read()
        rooms = policy["rooms"]
        documents = (
            rooms.get(room, {}).get("documents", {}) if isinstance(rooms.get(room), dict) else {}
        )
        grants = documents.get(doc_id)
        if not isinstance(grants, dict):
            return False  # unknown document: deny
        return self._grants_match(grants, principal, self._principal_roles(policy, principal))

    def allowed_documents(self, principal: str, room: str) -> frozenset[str]:
        policy = self._read()
        rooms = policy["rooms"]
        room_block = rooms.get(room)
        if not isinstance(room_block, dict):
            return frozenset()  # unknown room: no grants, never an error
        documents = room_block.get("documents", {})
        if not isinstance(documents, dict):
            raise PolicySchemaInvalid(f"rooms[{room!r}].documents must be an object")
        roles = self._principal_roles(policy, principal)
        return frozenset(
            doc_id
            for doc_id, grants in documents.items()
            if isinstance(grants, dict) and self._grants_match(grants, principal, roles)
        )


# --- Qdrant transport ---------------------------------------------------------


class QdrantTransport(Protocol):
    """The two Qdrant operations the data-room surface needs."""

    def search(
        self,
        collection: str,
        vector: Sequence[float],
        query_filter: dict[str, Any] | None,
        limit: int,
    ) -> list[dict[str, Any]]: ...

    def upsert(self, collection: str, points: list[dict[str, Any]]) -> None: ...


class HttpxQdrantTransport:
    """Qdrant REST transport (search + upsert), failing closed on every fault.

    Any transport fault — connection error, timeout, non-2xx — raises
    :class:`DataRoomStoreUnavailable`; the retrieval service never sees a
    partial answer it could mistake for "no results".
    """

    def __init__(self, base_url: str, api_key: str | None = None, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"api-key": api_key} if api_key else {}
        self._timeout = timeout

    def search(
        self,
        collection: str,
        vector: Sequence[float],
        query_filter: dict[str, Any] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {"vector": list(vector), "limit": limit, "with_payload": True}
        if query_filter is not None:
            body["filter"] = query_filter
        try:
            response = httpx.post(
                f"{self._base_url}/collections/{collection}/points/search",
                json=body,
                headers=self._headers,
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise DataRoomStoreUnavailable(f"qdrant search failed: {exc}") from exc
        return list(payload.get("result", []))

    def upsert(self, collection: str, points: list[dict[str, Any]]) -> None:
        try:
            response = httpx.put(
                f"{self._base_url}/collections/{collection}/points",
                json={"points": points},
                headers=self._headers,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except (httpx.HTTPError, ValueError) as exc:
            raise DataRoomStoreUnavailable(f"qdrant upsert failed: {exc}") from exc


class Embedder(Protocol):
    """Text -> embedding vector. Swap the demo embedder for the real model."""

    def embed(self, text: str) -> list[float]: ...


class DemoHashEmbedder:
    """Deterministic hashed-bag-of-words embedding, FOR DEMO AND TESTS ONLY.

    Authorization correctness here is independent of embedding quality — the
    stages wrap whatever vectors the store holds. Production registers a real
    embedding model (WrenAI uses text-embedding-3-large at 3072 dims); this
    embedder exists so local runs and the test suite are reproducible offline.
    """

    def __init__(self, dims: int = 64) -> None:
        self.dims = dims

    def embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dims
        for token in text.lower().split():
            bucket = int.from_bytes(hashlib.sha256(token.encode()).digest()[:8], "big")
            vec[bucket % self.dims] += 1.0
        return vec


# --- audit trail ---------------------------------------------------------------


class DataRoomAudit:
    """Thread-safe JSONL audit of every data-room retrieval event."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, event: dict[str, Any]) -> None:
        event = {**event, "timestamp": dt.datetime.now(dt.UTC).isoformat()}
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        """Newest-first retrieval events (up to ``limit``)."""
        with self._lock:
            if not self.path.exists():
                return []
            lines = self.path.read_text(encoding="utf-8").splitlines()
        events = []
        for line in reversed(lines):
            if not line.strip():
                continue
            events.append(json.loads(line))
            if len(events) >= limit:
                break
        return events


# --- retrieval service ----------------------------------------------------------


@dataclass(frozen=True)
class RetrievalResult:
    """What the caller gets: only hits that survived both stages."""

    outcome: str  # EXECUTED | POST_VERIFY_REMOVED | NO_GRANTS
    documents: list[dict[str, Any]] = field(default_factory=list)
    removed_doc_ids: list[str] = field(default_factory=list)
    grant_count: int = 0
    detail: str = ""


class DataRoomRetrievalService:
    """Dual-stage fail-closed search over acquisition data rooms."""

    def __init__(
        self,
        *,
        policy: DataRoomAclPolicy,
        transport: QdrantTransport,
        embedder: Embedder,
        collection: str,
        audit: DataRoomAudit,
        max_top_k: int = 50,
    ) -> None:
        self.policy = policy
        self.transport = transport
        self.embedder = embedder
        self.collection = collection
        self.audit = audit
        self.max_top_k = max_top_k

    @classmethod
    def from_settings(cls, settings: Any) -> DataRoomRetrievalService:
        """Assemble from GenbiSettings (env-driven, Vercel-friendly)."""
        return cls(
            policy=FileAclPolicySource(path=settings.data_room_policy_path),
            transport=HttpxQdrantTransport(
                settings.qdrant_url,
                api_key=settings.qdrant_api_key,
                timeout=settings.qdrant_timeout_seconds,
            ),
            embedder=DemoHashEmbedder(),
            collection=settings.data_room_collection,
            audit=DataRoomAudit(settings.data_room_audit_path),
        )

    # ------------------------------------------------------------------
    # Stage 1: server-side pre-filter
    # ------------------------------------------------------------------

    def _pre_filter(self, allowed: frozenset[str]) -> dict[str, Any]:
        """The Qdrant payload filter restricting the search to allowed docs.

        The filter is evaluated atomically with the knn search server-side —
        unauthorized vectors are never scored, only filtered results exist.
        """
        return {"must": [{"key": "doc_id", "match": {"any": sorted(allowed)}}]}

    def search(self, principal: str, room: str, query: str, top_k: int = 10) -> RetrievalResult:
        """Authorized retrieval: pre-filter -> search -> post-verify -> audit."""
        top_k = max(1, min(top_k, self.max_top_k))
        try:
            allowed = self.policy.allowed_documents(principal, room)
        except PolicySourceUnavailable:
            self._audit(principal, room, query, top_k, 0, [], POLICY_UNAVAILABLE)
            raise

        # Fail-closed short-circuit: nothing to see -> never touch the store.
        if not allowed:
            self._audit(principal, room, query, top_k, 0, [], NO_GRANTS)
            return RetrievalResult(
                outcome=NO_GRANTS,
                grant_count=0,
                detail="no documents granted in this room for this principal",
            )

        query_filter = self._pre_filter(allowed)
        try:
            hits = self.transport.search(
                self.collection, self.embedder.embed(query), query_filter, top_k
            )
        except DataRoomStoreUnavailable:
            self._audit(principal, room, query, top_k, len(allowed), [], STORE_UNAVAILABLE)
            raise

        # ------------------------------------------------------------------
        # Stage 2: post-verify every hit against a FRESH policy read.
        # ------------------------------------------------------------------
        kept: list[dict[str, Any]] = []
        removed: list[str] = []
        for hit in hits:
            payload = hit.get("payload") or {}
            doc_id = payload.get("doc_id")
            if (
                not isinstance(doc_id, str)
                or payload.get("room") != room  # cross-room payload: not identifiable here
                or not self.policy.is_allowed(principal, room, doc_id)
            ):
                removed.append(str(doc_id) if doc_id is not None else "<missing doc_id>")
                continue
            kept.append(
                {
                    "doc_id": doc_id,
                    "score": hit.get("score"),
                    "classification": payload.get("classification"),
                    "title": payload.get("title"),
                    "text": payload.get("text"),
                }
            )

        outcome = POST_VERIFY_REMOVED if removed else EXECUTED
        detail = (
            f"stage-2 post-verify removed {len(removed)} hit(s) the policy does not allow"
            if removed
            else f"all {len(kept)} hits verified against the policy"
        )
        self._audit(principal, room, query, top_k, len(allowed), removed, outcome)
        return RetrievalResult(
            outcome=outcome,
            documents=kept,
            removed_doc_ids=removed,
            grant_count=len(allowed),
            detail=detail,
        )

    # ------------------------------------------------------------------
    # Ingestion: ACLs are never stored on the point payload.
    # ------------------------------------------------------------------

    def index_document(
        self,
        room: str,
        doc_id: str,
        text: str,
        *,
        title: str | None = None,
        classification: str | None = None,
    ) -> None:
        """Index a data-room document. Identity fields only — ACLs live in the
        policy source, never copied onto the point (single-source invariant)."""
        payload = {"doc_id": doc_id, "room": room, "text": text}
        if title is not None:
            payload["title"] = title
        if classification is not None:
            payload["classification"] = classification
        self.transport.upsert(
            self.collection,
            [{"id": doc_id, "vector": self.embedder.embed(text), "payload": payload}],
        )

    def _audit(
        self,
        principal: str,
        room: str,
        query: str,
        top_k: int,
        grant_count: int,
        removed: list[str],
        outcome: str,
    ) -> None:
        self.audit.append(
            {
                "event": "data_room_search",
                "principal": principal,
                "room": room,
                "query": query,
                "top_k": top_k,
                "grant_count": grant_count,
                "outcome": outcome,
                "removed_doc_ids": removed,
                "removed_count": len(removed),
            }
        )
