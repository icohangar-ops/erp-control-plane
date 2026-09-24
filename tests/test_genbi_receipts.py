"""Tool-approval receipts (cubiczan-chp-mcp scheme): binding, tamper, replay.

The scheme's promise: a receipt approves exactly one request — the tool, the
normalized arguments, the policy version, the actor — and stops approving
anything the moment any of those differ, the expiry passes, or the receipt is
redeemed a second time. Every test here is a way an approval could try to
escape its binding, and the refusal that meets it.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
from pathlib import Path
from typing import Any

import pytest

from api.genbi.receipts import (
    DEV_APPROVAL_RECEIPT_KEY_HEX,
    PROMOTION_TOOL,
    RECEIPT_SCHEMA,
    ApprovalReceiptService,
    ReceiptKeyUnavailable,
    ReceiptRejection,
    args_binding,
    canonical_json,
    normalize_value,
    promotion_args,
    promotion_policy_version,
    resolve_receipt_key,
)
from api.genbi.viz import MetricSpec, VizSpec

ARGS = {"question": "revenue by branch", "sql": "select 1", "limit": 10}
POLICY = "genbi-promotion-policy/v1#abc123"
NOW = dt.datetime(2026, 9, 20, 12, 0, 0, tzinfo=dt.UTC)


@pytest.fixture()
def service(tmp_path: Path) -> ApprovalReceiptService:
    return ApprovalReceiptService(
        tmp_path / "approval_receipts.jsonl", bytes.fromhex(DEV_APPROVAL_RECEIPT_KEY_HEX)
    )


def signed(service: ApprovalReceiptService, **overrides: Any) -> dict[str, Any]:
    """A valid receipt for ARGS/POLICY, with any field overridden post-signing."""
    receipt = service.sign(
        tool=PROMOTION_TOOL,
        actor="sam",
        args=ARGS,
        policy_version=POLICY,
        ttl_seconds=3600,
        now=NOW,
        nonce="nonce-1",
    )
    receipt.update(overrides)
    return receipt


def redeemed(
    service: ApprovalReceiptService,
    receipt: dict[str, Any],
    args: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """verify_and_redeem with the canonical request and a deterministic clock."""
    call: dict[str, Any] = {
        "tool": PROMOTION_TOOL,
        "args": ARGS if args is None else args,
        "policy_version": POLICY,
        "now": NOW,
    }
    call.update(overrides)
    return service.verify_and_redeem(receipt, **call)


def test_sign_and_verify_round_trip(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    redemption = redeemed(service, receipt, actor="sam")
    assert redemption["receipt_id"] == receipt["receipt_id"]
    # The receipt ledger recorded both events, newest first.
    events = [event["event"] for event in service.records.list()]
    assert events == ["redeemed", "issued"]


def test_receipt_is_mac_sealed_with_deterministic_id(service: ApprovalReceiptService) -> None:
    receipt = service.sign(
        tool=PROMOTION_TOOL, actor="sam", args=ARGS, policy_version=POLICY, now=NOW, nonce="n"
    )
    twin = service.sign(
        tool=PROMOTION_TOOL, actor="sam", args=ARGS, policy_version=POLICY, now=NOW, nonce="n"
    )
    assert receipt["receipt_id"] == twin["receipt_id"]
    assert receipt["mac"] and receipt["schema"] == RECEIPT_SCHEMA


def test_tampered_mac_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service, mac="0" * 64)
    with pytest.raises(ReceiptRejection, match="MAC mismatch"):
        redeemed(service, receipt)


def test_tampered_args_refused_as_ambiguous_binding(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    # The request the tool is about to execute differs from the approved one.
    with pytest.raises(ReceiptRejection, match="ambiguous binding denied"):
        redeemed(service, receipt, args={**ARGS, "sql": "select 2"})


def test_extra_argument_is_an_ambiguous_binding(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    with pytest.raises(ReceiptRejection, match="ambiguous binding denied"):
        redeemed(service, receipt, args={**ARGS, "answer_date": "2026-09-19"})


def test_none_valued_arguments_normalize_to_the_same_binding(
    service: ApprovalReceiptService,
) -> None:
    """An optional field passed explicitly as null is not a binding escape hatch."""
    receipt = signed(service)
    redemption = redeemed(service, receipt, args={**ARGS, "backing": None})
    assert redemption["receipt_id"] == receipt["receipt_id"]


def test_wrong_tool_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    with pytest.raises(ReceiptRejection, match="tool mismatch"):
        redeemed(service, receipt, tool="genbi.answers.delete")


def test_wrong_policy_version_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    with pytest.raises(ReceiptRejection, match="policy version mismatch"):
        redeemed(service, receipt, policy_version="genbi-promotion-policy/v1#other")


def test_wrong_actor_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    with pytest.raises(ReceiptRejection, match="actor mismatch"):
        redeemed(service, receipt, actor="someone.else")


def test_expired_receipt_refused(service: ApprovalReceiptService) -> None:
    receipt = service.sign(
        tool=PROMOTION_TOOL, actor="sam", args=ARGS, policy_version=POLICY, ttl_seconds=60, now=NOW
    )
    later = NOW + dt.timedelta(minutes=1, seconds=1)
    with pytest.raises(ReceiptRejection, match="expired"):
        service.verify_and_redeem(
            receipt, tool=PROMOTION_TOOL, args=ARGS, policy_version=POLICY, now=later
        )


def test_replayed_receipt_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    redeemed(service, receipt, actor="sam")
    # A second redemption — even of a byte-identical receipt — is a replay.
    with pytest.raises(ReceiptRejection, match="replay refused"):
        redeemed(service, receipt, actor="sam")


def test_replay_is_atomic_across_service_instances(tmp_path: Path) -> None:
    """Per-request service instances on one ledger path cannot both redeem."""
    path = tmp_path / "approval_receipts.jsonl"
    key = bytes.fromhex(DEV_APPROVAL_RECEIPT_KEY_HEX)
    services = [ApprovalReceiptService(path, key) for _ in range(2)]
    receipt = services[0].sign(
        tool=PROMOTION_TOOL, actor="sam", args=ARGS, policy_version=POLICY, now=NOW, nonce="n1"
    )
    barrier = threading.Barrier(len(services))
    outcomes: list[bool] = []

    def redeem(service: ApprovalReceiptService) -> None:
        barrier.wait()
        try:
            service.verify_and_redeem(
                receipt,
                tool=PROMOTION_TOOL,
                args=ARGS,
                policy_version=POLICY,
                actor="sam",
                now=NOW,
            )
            outcomes.append(True)
        except ReceiptRejection:
            outcomes.append(False)

    threads = [threading.Thread(target=redeem, args=(service,)) for service in services]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert outcomes.count(True) == 1, "exactly one redemption may win"
    assert outcomes.count(False) == len(services) - 1


def test_issued_ledger_events_carry_no_mac(service: ApprovalReceiptService) -> None:
    """The MAC is bearer material: the audit ledger never stores it."""
    receipt = signed(service)
    issued = next(event for event in service.records.list() if event["event"] == "issued")
    assert "mac" not in issued["receipt"]
    assert receipt["mac"], "the returned receipt itself is still MAC-sealed"


def test_refused_verification_leaves_the_ledger_auditable(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    redeemed(service, receipt, actor="sam")
    with pytest.raises(ReceiptRejection):
        redeemed(service, receipt, actor="sam")
    events = service.records.list()
    assert len(events) == 2  # issued + one redemption; the refused retry added nothing
    assert events[0]["event"] == "redeemed"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "other-scheme/v1"),
        ("tool", ""),
        ("actor", 7),
        ("expires_at", "not-a-timestamp"),
        ("issued_at", "2026-09-20T12:00:00"),  # naive datetime
        ("receipt_id", None),
        ("nonce", None),
        ("policy_version", None),
    ],
)
def test_malformed_receipt_fields_refused(
    service: ApprovalReceiptService, field: str, value: Any
) -> None:
    receipt = signed(service, **{field: value})
    with pytest.raises(ReceiptRejection):
        redeemed(service, receipt)


def test_missing_field_refused(service: ApprovalReceiptService) -> None:
    receipt = signed(service)
    del receipt["args_sha256"]
    with pytest.raises(ReceiptRejection, match="missing field"):
        redeemed(service, receipt)


def test_non_object_receipt_refused(service: ApprovalReceiptService) -> None:
    with pytest.raises(ReceiptRejection, match="must be a JSON object"):
        service.verify_and_redeem(
            "not-a-receipt", tool=PROMOTION_TOOL, args=ARGS, policy_version=POLICY
        )


# --- key resolution ----------------------------------------------------------


def test_key_unset_locally_falls_back_to_documented_dev_default() -> None:
    key = resolve_receipt_key({"ENVIRONMENT": "local"})
    assert key == bytes.fromhex(DEV_APPROVAL_RECEIPT_KEY_HEX)


def test_key_unset_in_production_refuses() -> None:
    with pytest.raises(ReceiptKeyUnavailable, match="not set in production"):
        resolve_receipt_key({"ENVIRONMENT": "production"})


def test_dev_default_key_in_production_refuses() -> None:
    with pytest.raises(ReceiptKeyUnavailable, match="dev-only key"):
        resolve_receipt_key(
            {"ENVIRONMENT": "prod", "GENBI_APPROVAL_RECEIPT_KEY": DEV_APPROVAL_RECEIPT_KEY_HEX}
        )


@pytest.mark.parametrize("bad", ["not-hex", "0" * 63 + "g", "0" * 62])
def test_malformed_key_values_refuse(bad: str) -> None:
    with pytest.raises(ReceiptKeyUnavailable):
        resolve_receipt_key({"ENVIRONMENT": "local", "GENBI_APPROVAL_RECEIPT_KEY": bad})


def test_valid_production_key_is_accepted() -> None:
    key = resolve_receipt_key(
        {"ENVIRONMENT": "production", "GENBI_APPROVAL_RECEIPT_KEY": "ab" * 32}
    )
    assert key == bytes.fromhex("ab" * 32)


# --- canonicalization + policy version ---------------------------------------


def test_canonical_json_is_sorted_and_compact() -> None:
    assert canonical_json({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'


def test_normalize_value_sorts_and_drops_nones() -> None:
    normalized = normalize_value({"z": None, "a": [{"y": 2, "x": None}, 3]})
    assert normalized == {"a": [{"y": 2}, 3]}


def test_args_binding_is_stable_under_key_order() -> None:
    assert args_binding({"a": 1, "b": 2}) == args_binding({"b": 2, "a": 1})


def test_policy_version_tracks_the_governing_knobs() -> None:
    class Settings:
        row_cap = 500
        statement_timeout_seconds = 30.0
        chp_require_human_lock = False
        superset_readonly_database_id = 1
        marts_schema = "main_marts"

    base = promotion_policy_version(Settings())
    assert base.startswith("genbi-promotion-policy/v1#")

    Settings.row_cap = 1000
    assert promotion_policy_version(Settings()) != base


def test_promotion_args_bind_the_exact_tool_arguments() -> None:
    viz = VizSpec(
        viz_type="echarts_timeseries_bar",
        x_axis="branch_code",
        metrics=[MetricSpec(column="revenue", aggregate="SUM", label="Revenue")],
    )
    args = promotion_args(
        question="q",
        sql="select 1",
        answer_date=dt.date(2026, 9, 19),
        backing=None,
        viz=viz,
    )
    assert json.loads(args_binding(args))["answer_date"] == "2026-09-19"
    assert args_binding(args) == args_binding(dict(reversed(list(args.items()))))
