"""Executable agent contracts: compile-time refusal, runtime evaluation.

The compile step is the fail-closed posture — a declared rule with no
enforcement hook must make the whole control surface refuse to build, never
ship as prose. The runtime step must evaluate the compiled hooks and surface
every failure as a named ContractViolation.
"""

from __future__ import annotations

from typing import Any

import pytest

from api.contracts import (
    GENBI_CONTROL_PLANE_RULES,
    STAGE_PRE_EXECUTE,
    STAGE_PRE_PERSIST,
    STAGES,
    ContractCompileError,
    ContractViolation,
    ControlPlaneRule,
    compile_contracts,
    compile_genbi_contracts,
    genbi_contract_hooks,
)


def a_hook(context: Any) -> None:  # pragma: no cover - trivial hook used in compile tests
    return None


def test_genbi_rules_compile_against_the_real_registry() -> None:
    """Every shipped GenBI rule is enforceable — the control surface builds."""
    compiled = compile_genbi_contracts()
    names = {binding.rule.name for binding in compiled.bindings}
    assert names == {rule.name for rule in GENBI_CONTROL_PLANE_RULES}
    assert len(compiled.bindings) == len(GENBI_CONTROL_PLANE_RULES)


def test_missing_hook_is_refused_at_compile_time() -> None:
    rules = [ControlPlaneRule(name="r", description="d", hook="no_such_hook", stage=STAGES[0])]
    with pytest.raises(ContractCompileError, match="cannot be enforced"):
        compile_contracts(rules, {})


def test_non_callable_hook_is_refused_at_compile_time() -> None:
    rules = [ControlPlaneRule(name="r", description="d", hook="broken", stage=STAGES[0])]
    with pytest.raises(ContractCompileError, match="not callable"):
        compile_contracts(rules, {"broken": "not a function"})


def test_unknown_stage_is_refused_at_compile_time() -> None:
    rules = [ControlPlaneRule(name="r", description="d", hook="h", stage="somewhere-else")]
    with pytest.raises(ContractCompileError, match="unknown stage"):
        compile_contracts(rules, {"h": a_hook})


def test_duplicate_rule_name_is_refused_at_compile_time() -> None:
    rules = [
        ControlPlaneRule(name="r", description="d", hook="h", stage=STAGES[0]),
        ControlPlaneRule(name="r", description="d2", hook="h", stage=STAGES[0]),
    ]
    with pytest.raises(ContractCompileError, match="duplicate"):
        compile_contracts(rules, {"h": a_hook})


def test_enforce_runs_only_the_stage_bound_hooks() -> None:
    ran: list[str] = []

    def stage_one(context: Any) -> None:
        ran.append("one")

    def stage_two(context: Any) -> None:
        ran.append("two")

    rules = [
        ControlPlaneRule(name="r1", description="d", hook="h1", stage=STAGE_PRE_EXECUTE),
        ControlPlaneRule(name="r2", description="d", hook="h2", stage=STAGE_PRE_PERSIST),
    ]
    compiled = compile_contracts(rules, {"h1": stage_one, "h2": stage_two})
    compiled.enforce(STAGE_PRE_EXECUTE, {})
    assert ran == ["one"]
    compiled.enforce(STAGE_PRE_PERSIST, {})
    assert ran == ["one", "two"]


def test_enforce_wraps_unexpected_hook_failures_in_a_named_violation() -> None:
    def exploding(context: Any) -> None:
        raise RuntimeError("boom")

    rules = [ControlPlaneRule(name="r", description="d", hook="h", stage=STAGE_PRE_EXECUTE)]
    compiled = compile_contracts(rules, {"h": exploding})
    with pytest.raises(ContractViolation, match=r"contract r.*boom"):
        compiled.enforce(STAGE_PRE_EXECUTE, {})


def test_enforce_refuses_unknown_stages_at_runtime() -> None:
    compiled = compile_genbi_contracts()
    with pytest.raises(ContractViolation, match="unknown enforcement stage"):
        compiled.enforce("not-a-stage", {})


def test_describe_lists_every_compiled_rule_as_enforced() -> None:
    described = compile_genbi_contracts().describe()
    assert len(described) == len(GENBI_CONTROL_PLANE_RULES)
    assert all(entry["enforced"] == "true" for entry in described)
    assert {entry["name"] for entry in described} >= {
        "genbi.execution.read_only_uri",
        "genbi.execution.select_only",
        "genbi.approval.human_lock_bears_receipt",
        "genbi.persistence.verified_read_only_target",
    }


# --- runtime behavior of the shipped GenBI hooks ------------------------------


class Settings:
    duckdb_uri = "duckdb:///analytics.duckdb?access_mode=READ_ONLY"
    row_cap = 500
    statement_timeout_seconds = 30.0


def test_read_only_uri_hook_enforces_the_canonical_uri() -> None:
    from api.contracts import _hook_ensure_read_only_uri

    _hook_ensure_read_only_uri({"settings": Settings()})


def test_select_only_hook_enforces_the_guardrail() -> None:
    from api.contracts import _hook_validate_select_only

    with pytest.raises(ContractViolation):
        _hook_validate_select_only({"sql": "drop table answers"})


def test_positive_bounds_hooks_refuse_degenerate_settings() -> None:
    from api.contracts import (
        _hook_row_cap_positive,
        _hook_statement_timeout_positive,
    )

    bad = Settings()
    bad.row_cap = 0
    with pytest.raises(ContractViolation, match="row_cap is 0"):
        _hook_row_cap_positive({"settings": bad})

    slow = Settings()
    slow.statement_timeout_seconds = -1
    with pytest.raises(ContractViolation, match="statement_timeout_seconds"):
        _hook_statement_timeout_positive({"settings": slow})


def test_receipt_hook_refuses_an_unbound_human_approval() -> None:
    from api.contracts import _hook_receipt_when_confirmed

    with pytest.raises(ContractViolation, match="human_lock_bears_receipt"):
        _hook_receipt_when_confirmed({"confirmed_by": "sam", "approval_receipt": None})
    # No approval claimed — nothing to enforce.
    _hook_receipt_when_confirmed({"confirmed_by": None, "approval_receipt": None})


def test_persist_hook_refuses_unverified_targets() -> None:
    from api.contracts import _hook_verified_read_only_target

    with pytest.raises(ContractViolation, match="verified_read_only_target"):
        _hook_verified_read_only_target({"read_only_target_verified": False, "database_id": 9})
    _hook_verified_read_only_target({"read_only_target_verified": True, "database_id": 9})


def test_hook_registry_covers_every_declared_hook_name() -> None:
    hooks = genbi_contract_hooks()
    declared = {rule.hook for rule in GENBI_CONTROL_PLANE_RULES}
    assert declared == set(hooks)
