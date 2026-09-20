"""Fail-closed dual-stage authorized retrieval over the acquisition data rooms.

The scenario these tests pin: payroll/pricing/diligence documents in one
Qdrant-backed room where a deal manager may read a document an analyst must
not — per document, not per room. Every fail-closed path matters:
policy-source outage, store outage, unknown principal, and a store that
returns hits outside its own filter (the case the post-verify stage exists
for). Nothing may ever degrade to unfiltered results.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api import index as api_index
from api.genbi import routes as genbi_routes
from api.genbi.data_room import (
    EXECUTED,
    NO_GRANTS,
    POST_VERIFY_REMOVED,
    DataRoomAudit,
    DataRoomRetrievalService,
    DataRoomStoreUnavailable,
    DemoHashEmbedder,
    FileAclPolicySource,
    PolicySchemaInvalid,
    PolicySourceUnavailable,
)

ROOM = "ridgeline_diligence"

MANAGER = "maya@acquirer.example"  # role: deal_lead
EMPLOYEE = "raul@acquirer.example"  # role: analyst

POLICY = {
    "schema": "cubiczan-data-room/acl/v1",
    "principals": {
        MANAGER: {"roles": ["deal_lead"]},
        EMPLOYEE: {"roles": ["analyst"]},
    },
    "rooms": {
        ROOM: {
            "documents": {
                "payroll_register_2025": {
                    "classification": "payroll",
                    "allow": ["role:deal_lead"],
                },
                "pricing_book_2026": {
                    "classification": "pricing",
                    "allow": ["role:deal_lead", "role:analyst"],
                },
            }
        }
    },
}


class FakeQdrant:
    """Records search calls; can be told to misbehave like a drifting store."""

    def __init__(self) -> None:
        self.search_calls: list[dict[str, Any]] = []
        self.upserts: list[list[dict[str, Any]]] = []
        self.hits_override: list[dict[str, Any]] | None = None
        self.fail_next_search = False

    def search(
        self,
        collection: str,
        vector: list[float],
        query_filter: dict[str, Any] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        self.search_calls.append(
            {"collection": collection, "vector": vector, "filter": query_filter, "limit": limit}
        )
        if self.fail_next_search:
            raise DataRoomStoreUnavailable("qdrant down")
        if self.hits_override is not None:
            return self.hits_override
        # Faithful store: one hit per allowed doc_id in the filter.
        allowed = query_filter["must"][0]["match"]["any"] if query_filter else []
        return [
            {
                "id": doc_id,
                "score": 0.9,
                "payload": {"doc_id": doc_id, "room": ROOM, "text": f"text of {doc_id}"},
            }
            for doc_id in allowed[:limit]
        ]

    def upsert(self, collection: str, points: list[dict[str, Any]]) -> None:
        self.upserts.append(points)


def write_policy(tmp_path: Path, policy: dict[str, Any] | None = None) -> Path:
    path = tmp_path / "acl.json"
    path.write_text(json.dumps(policy if policy is not None else POLICY), encoding="utf-8")
    return path


def make_service(
    tmp_path: Path,
    fake: FakeQdrant,
    policy_path: Path | None = None,
) -> DataRoomRetrievalService:
    return DataRoomRetrievalService(
        policy=FileAclPolicySource(path=policy_path or write_policy(tmp_path)),
        transport=fake,
        embedder=DemoHashEmbedder(dims=8),
        collection="data_rooms_test",
        audit=DataRoomAudit(tmp_path / "data_room_audit.jsonl"),
    )


# --- the core scenario: manager-can / employee-cannot, per document ----------


def test_manager_sees_payroll_document(tmp_path: Path) -> None:
    fake = FakeQdrant()
    result = make_service(tmp_path, fake).search(MANAGER, ROOM, "payroll register")
    assert result.outcome == EXECUTED
    assert [d["doc_id"] for d in result.documents] == [
        "payroll_register_2025",
        "pricing_book_2026",
    ]


def test_employee_cannot_see_payroll_document(tmp_path: Path) -> None:
    fake = FakeQdrant()
    result = make_service(tmp_path, fake).search(EMPLOYEE, ROOM, "payroll register")
    assert result.outcome == EXECUTED
    ids = [d["doc_id"] for d in result.documents]
    assert "payroll_register_2025" not in ids
    assert ids == ["pricing_book_2026"]


def test_pre_filter_carries_only_allowed_doc_ids(tmp_path: Path) -> None:
    """Stage 1: the grant set is pushed server-side — the filter the store
    evaluates atomically with the search contains ONLY allowed ids."""
    fake = FakeQdrant()
    make_service(tmp_path, fake).search(EMPLOYEE, ROOM, "anything")
    sent_filter = fake.search_calls[0]["filter"]
    assert sent_filter["must"][0]["match"]["any"] == ["pricing_book_2026"]


def test_no_grants_short_circuits_without_touching_the_store(tmp_path: Path) -> None:
    """Fail-closed: an unknown principal (or empty grant set) never reaches
    Qdrant — there is no search to leak and no unfiltered fallback."""
    fake = FakeQdrant()
    result = make_service(tmp_path, fake).search("mallory@acquirer.example", ROOM, "payroll")
    assert result.outcome == NO_GRANTS
    assert result.documents == []
    assert fake.search_calls == []


def test_unknown_room_yields_empty_result_without_store_call(tmp_path: Path) -> None:
    fake = FakeQdrant()
    result = make_service(tmp_path, fake).search(MANAGER, "unknown_room", "anything")
    assert result.outcome == NO_GRANTS
    assert fake.search_calls == []


# --- stage 2: post-verify catches what the pre-filter let through -------------


def test_post_verify_removes_hits_outside_the_grant_set(tmp_path: Path) -> None:
    """The drift case: the store returns a hit the filter should have excluded
    (ACL changed between grant read and search, mis-scoped filter, stale
    replica). Stage 2 drops it and alarms; the caller never sees it."""
    fake = FakeQdrant()
    fake.hits_override = [
        {
            "id": "payroll_register_2025",
            "score": 0.95,
            "payload": {"doc_id": "payroll_register_2025", "room": ROOM, "text": "salaries"},
        },
        {
            "id": "pricing_book_2026",
            "score": 0.8,
            "payload": {"doc_id": "pricing_book_2026", "room": ROOM, "text": "prices"},
        },
    ]
    result = make_service(tmp_path, fake).search(EMPLOYEE, ROOM, "payroll")
    assert result.outcome == POST_VERIFY_REMOVED
    assert result.removed_doc_ids == ["payroll_register_2025"]
    assert [d["doc_id"] for d in result.documents] == ["pricing_book_2026"]


def test_post_verify_drops_hit_with_missing_identity_payload(tmp_path: Path) -> None:
    """A hit without its identity payload is not verifiable — fail closed."""
    fake = FakeQdrant()
    fake.hits_override = [{"id": "opaque", "score": 0.99, "payload": {"text": "no identity"}}]
    result = make_service(tmp_path, fake).search(MANAGER, ROOM, "anything")
    assert result.outcome == POST_VERIFY_REMOVED
    assert result.documents == []
    assert result.removed_doc_ids == ["<missing doc_id>"]


def test_post_verify_drops_cross_room_payload(tmp_path: Path) -> None:
    fake = FakeQdrant()
    fake.hits_override = [
        {
            "id": "pricing_book_2026",
            "score": 0.8,
            "payload": {
                "doc_id": "pricing_book_2026",
                "room": "another_room",
                "text": "smuggled",
            },
        }
    ]
    result = make_service(tmp_path, fake).search(MANAGER, ROOM, "anything")
    assert result.outcome == POST_VERIFY_REMOVED
    assert result.documents == []


def test_revocation_is_visible_on_the_next_search(tmp_path: Path) -> None:
    """The policy source is uncached: rewrite the file and the very next
    search honors the revocation — no redeploy, no TTL to expire."""
    fake = FakeQdrant()
    policy_path = write_policy(tmp_path)
    service = make_service(tmp_path, fake, policy_path)
    assert service.search(MANAGER, ROOM, "payroll").outcome == EXECUTED

    revoked = json.loads(policy_path.read_text(encoding="utf-8"))
    del revoked["rooms"][ROOM]["documents"]["payroll_register_2025"]
    policy_path.write_text(json.dumps(revoked), encoding="utf-8")

    result = service.search(MANAGER, ROOM, "payroll")
    assert result.outcome == EXECUTED
    assert [d["doc_id"] for d in result.documents] == ["pricing_book_2026"]


# --- fail-closed on outages ----------------------------------------------------


def test_policy_source_outage_raises_and_never_queries_the_store(tmp_path: Path) -> None:
    """Policy unreadable -> raise. An unfiltered search is not a fallback."""
    fake = FakeQdrant()
    service = make_service(tmp_path, fake, policy_path=tmp_path / "does_not_exist.json")
    with pytest.raises(PolicySourceUnavailable):
        service.search(MANAGER, ROOM, "payroll")
    assert fake.search_calls == []


def test_corrupt_policy_is_a_refusal_not_a_guess(tmp_path: Path) -> None:
    fake = FakeQdrant()
    policy_path = tmp_path / "acl.json"
    policy_path.write_text("{not json", encoding="utf-8")
    with pytest.raises(PolicySchemaInvalid):
        make_service(tmp_path, fake, policy_path).search(MANAGER, ROOM, "payroll")
    assert fake.search_calls == []


def test_policy_with_wrong_schema_version_is_refused(tmp_path: Path) -> None:
    fake = FakeQdrant()
    bumped = {**POLICY, "schema": "cubiczan-data-room/acl/v2"}
    with pytest.raises(PolicySchemaInvalid):
        make_service(tmp_path, fake, write_policy(tmp_path, bumped)).search(MANAGER, ROOM, "x")


def test_store_outage_raises_and_is_audited(tmp_path: Path) -> None:
    fake = FakeQdrant()
    fake.fail_next_search = True
    service = make_service(tmp_path, fake)
    with pytest.raises(DataRoomStoreUnavailable):
        service.search(MANAGER, ROOM, "payroll")
    events = service.audit.list()
    assert events[0]["outcome"] == "store_unavailable"


# --- audit trail -----------------------------------------------------------------


def test_every_search_is_audited_including_refusals(tmp_path: Path) -> None:
    fake = FakeQdrant()
    service = make_service(tmp_path, fake)
    service.search(MANAGER, ROOM, "payroll", top_k=5)
    service.search("mallory@acquirer.example", ROOM, "payroll")
    service.search(EMPLOYEE, ROOM, "payroll")  # post-verify removal case

    events = service.audit.list()
    assert len(events) == 3
    assert events[0]["outcome"] == EXECUTED  # newest first
    assert events[0]["principal"] == EMPLOYEE
    assert events[1]["outcome"] == NO_GRANTS
    assert events[2]["top_k"] == 5
    assert events[2]["grant_count"] == 2


# --- ingestion: the single-source invariant ---------------------------------------


def test_indexed_payload_never_carries_the_acl(tmp_path: Path) -> None:
    """ACLs live only in the policy source; a payload copy would be a second,
    cached policy source and would silently break revocation."""
    fake = FakeQdrant()
    service = make_service(tmp_path, fake)
    service.index_document(ROOM, "payroll_register_2025", "salaries", title="Payroll")
    point = fake.upserts[0][0]
    assert point["payload"]["doc_id"] == "payroll_register_2025"
    assert point["payload"]["room"] == ROOM
    assert "allow" not in point["payload"]
    assert "acl" not in point["payload"]


# --- the committed example policy stays loadable -----------------------------------


def test_committed_example_policy_loads_and_splits_manager_employee() -> None:
    """The example in seed/ is the documented shape — it must keep parsing
    and encoding the manager-can/employee-cannot split."""
    repo_root = Path(__file__).resolve().parents[1]
    example = repo_root / "seed" / "data_rooms" / "data_room_acl.example.json"
    policy = FileAclPolicySource(path=example)
    manager_docs = policy.allowed_documents("maya@acquirer.example", "ridgeline_diligence")
    employee_docs = policy.allowed_documents("raul@acquirer.example", "ridgeline_diligence")
    assert "payroll_register_2025" in manager_docs
    assert "payroll_register_2025" not in employee_docs


# --- HTTP surface -----------------------------------------------------------------


def test_search_endpoint_returns_only_authorized_documents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeQdrant()
    monkeypatch.setattr(
        genbi_routes, "build_data_room_service", lambda: make_service(tmp_path, fake)
    )
    client = TestClient(api_index.app)
    response = client.post(
        "/api/v1/genbi/data-room/search",
        json={"principal": EMPLOYEE, "room": ROOM, "query": "payroll"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["outcome"] == EXECUTED
    assert [d["doc_id"] for d in body["documents"]] == ["pricing_book_2026"]


def test_search_endpoint_503_when_policy_source_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeQdrant()
    monkeypatch.setattr(
        genbi_routes,
        "build_data_room_service",
        lambda: make_service(tmp_path, fake, policy_path=tmp_path / "missing.json"),
    )
    client = TestClient(api_index.app)
    response = client.post(
        "/api/v1/genbi/data-room/search",
        json={"principal": MANAGER, "room": ROOM, "query": "payroll"},
    )
    assert response.status_code == 503
    assert fake.search_calls == []


def test_search_endpoint_503_when_store_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeQdrant()
    fake.fail_next_search = True
    monkeypatch.setattr(
        genbi_routes, "build_data_room_service", lambda: make_service(tmp_path, fake)
    )
    client = TestClient(api_index.app)
    response = client.post(
        "/api/v1/genbi/data-room/search",
        json={"principal": MANAGER, "room": ROOM, "query": "payroll"},
    )
    assert response.status_code == 503
    assert response.json()["detail"].startswith("qdrant down")


def test_data_room_audit_endpoint_lists_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeQdrant()
    monkeypatch.setattr(
        genbi_routes, "build_data_room_service", lambda: make_service(tmp_path, fake)
    )
    client = TestClient(api_index.app)
    client.post(
        "/api/v1/genbi/data-room/search",
        json={"principal": MANAGER, "room": ROOM, "query": "payroll"},
    )
    response = client.get("/api/v1/genbi/data-room/audit")
    assert response.status_code == 200
    events = response.json()
    assert events[0]["event"] == "data_room_search"
    assert events[0]["principal"] == MANAGER
