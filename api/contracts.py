"""Executable agent contracts ([Gov] T1, agent-conductor pattern).

The control surface has rules — read-only execution, bounded queries, human
approval with a receipt, a verified persistence target. Before this module
those rules lived in prose and enforcement happened incidentally, wherever a
code path remembered to call it. An agent contract makes the rule a **thing**:
declared, compiled against a real enforcement hook, and evaluated by the
runtime on every promotion — or the control surface refuses to build.

Compile time
------------
``compile_contracts`` maps every declared :class:`ControlPlaneRule` to the
enforcement hook registered for it. A rule whose hook is missing from the
registry, or whose hook is not callable, is **refused at compile time** —
``ContractCompileError``. A rule that cannot be enforced must not exist as a
comfortable lie; refusing to build the service is the fail-closed posture.

Run time
--------
``ContractSet.enforce(stage, context)`` runs every hook bound to that stage,
in declaration order. Any hook failure — a :class:`ContractViolation`, a
guardrail refusal, an unexpected exception — is surfaced as a
:class:`ContractViolation` naming the rule: fail closed, never swallowed.
An unknown stage is itself a violation.

The GenBI promotion surface declares its rules in
``GENBI_CONTROL_PLANE_RULES`` and compiles them against real hooks from
``genbi_contract_hooks`` (guardrails + receipts + policy invariants);
``PromotionService`` evaluates the compiled set on the promote and persist
paths, so the contracts are exercised machinery, not documentation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from api.genbi.guardrails import GuardrailError, ensure_read_only_uri, validate_select_only
from api.genbi.receipts import PROMOTION_TOOL

# Enforcement stages — a rule is bound to exactly one stage of the promote flow.
STAGE_PRE_EXECUTE = "pre-execute"
STAGE_PRE_PERSIST = "pre-persist"
STAGES = (STAGE_PRE_EXECUTE, STAGE_PRE_PERSIST)


class ContractCompileError(Exception):
    """A declared rule cannot be enforced — refused at compile time."""


class ContractViolation(Exception):
    """The runtime refused an operation because a contract failed."""

    def __init__(self, rule: str, reason: str) -> None:
        super().__init__(f"contract {rule}: {reason}")
        self.rule = rule
        self.reason = reason


@dataclass(frozen=True)
class ControlPlaneRule:
    """One declared control-plane rule, bound to the hook that enforces it."""

    name: str
    description: str
    hook: str
    stage: str


@dataclass(frozen=True)
class RuleBinding:
    """A compiled rule: the declaration plus its enforcement hook."""

    rule: ControlPlaneRule
    hook: Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True)
class ContractSet:
    """The compiled, enforceable rule set for a control surface."""

    bindings: tuple[RuleBinding, ...]

    def enforce(self, stage: str, context: Mapping[str, Any]) -> None:
        """Run every hook for ``stage``; the first failure refuses the operation."""
        if stage not in STAGES:
            raise ContractViolation("contract-runtime", f"unknown enforcement stage {stage!r}")
        for binding in self.bindings:
            if binding.rule.stage != stage:
                continue
            try:
                binding.hook(context)
            except ContractViolation:
                raise
            except Exception as exc:
                raise ContractViolation(
                    binding.rule.name, f"enforcement hook failed: {exc}"
                ) from exc

    def describe(self) -> list[dict[str, str]]:
        """The compiled set for the self-audit endpoint (everything here IS enforced)."""
        return [
            {
                "name": binding.rule.name,
                "description": binding.rule.description,
                "stage": binding.rule.stage,
                "hook": binding.rule.hook,
                "enforced": "true",
            }
            for binding in self.bindings
        ]


def compile_contracts(
    rules: tuple[ControlPlaneRule, ...] | list[ControlPlaneRule],
    hooks: Mapping[str, Callable[[Mapping[str, Any]], None]],
) -> ContractSet:
    """Compile declared rules against the enforcement registry — or refuse.

    Every rule MUST resolve to a callable hook; anything else is a compile
    error, because a rule that cannot be enforced is not a rule.
    """
    seen: set[str] = set()
    bindings: list[RuleBinding] = []
    for rule in rules:
        if rule.name in seen:
            raise ContractCompileError(f"duplicate control-plane rule name {rule.name!r}")
        seen.add(rule.name)
        if rule.stage not in STAGES:
            raise ContractCompileError(
                f"rule {rule.name!r} binds unknown stage {rule.stage!r}"
                f" (known: {', '.join(STAGES)})"
            )
        hook = hooks.get(rule.hook)
        if hook is None:
            raise ContractCompileError(
                f"rule {rule.name!r} cannot be enforced: no hook registered for"
                f" {rule.hook!r} — refusing to compile an unenforceable rule"
            )
        if not callable(hook):
            raise ContractCompileError(
                f"rule {rule.name!r} cannot be enforced: hook {rule.hook!r} is not callable"
            )
        bindings.append(RuleBinding(rule=rule, hook=hook))
    return ContractSet(bindings=tuple(bindings))


# --- the GenBI promotion surface's declared rules ----------------------------

GENBI_CONTROL_PLANE_RULES: tuple[ControlPlaneRule, ...] = (
    ControlPlaneRule(
        name="genbi.execution.read_only_uri",
        description="Every GenBI execution runs against the canonical READ_ONLY DuckDB URI.",
        hook="ensure_read_only_uri",
        stage=STAGE_PRE_EXECUTE,
    ),
    ControlPlaneRule(
        name="genbi.execution.select_only",
        description="Only a single SELECT/WITH statement may execute; no forbidden keywords.",
        hook="validate_select_only",
        stage=STAGE_PRE_EXECUTE,
    ),
    ControlPlaneRule(
        name="genbi.execution.row_cap_positive",
        description="Bounded execution requires a positive row cap.",
        hook="require_row_cap_positive",
        stage=STAGE_PRE_EXECUTE,
    ),
    ControlPlaneRule(
        name="genbi.execution.statement_timeout_positive",
        description="Bounded execution requires a positive statement timeout.",
        hook="require_statement_timeout_positive",
        stage=STAGE_PRE_EXECUTE,
    ),
    ControlPlaneRule(
        name="genbi.approval.human_lock_bears_receipt",
        description=(
            "A claimed human approval (confirmed_by) must carry a tool-approval receipt"
            " — an unbound approval is refused."
        ),
        hook="require_receipt_when_confirmed",
        stage=STAGE_PRE_EXECUTE,
    ),
    ControlPlaneRule(
        name="genbi.persistence.verified_read_only_target",
        description=(
            "Persistence writes only to a Superset database whose STORED uri was verified"
            " server-side to be the READ_ONLY analytics connection."
        ),
        hook="require_verified_read_only_target",
        stage=STAGE_PRE_PERSIST,
    ),
)


def genbi_contract_hooks() -> dict[str, Callable[[Mapping[str, Any]], None]]:
    """The enforcement registry: real functions the contracts compile against."""
    return {
        "ensure_read_only_uri": _hook_ensure_read_only_uri,
        "validate_select_only": _hook_validate_select_only,
        "require_row_cap_positive": _hook_row_cap_positive,
        "require_statement_timeout_positive": _hook_statement_timeout_positive,
        "require_receipt_when_confirmed": _hook_receipt_when_confirmed,
        "require_verified_read_only_target": _hook_verified_read_only_target,
    }


def compile_genbi_contracts() -> ContractSet:
    """Compile the GenBI surface's rules — the service builds on nothing else."""
    return compile_contracts(GENBI_CONTROL_PLANE_RULES, genbi_contract_hooks())


# --- hook implementations (fail-closed, context-driven) ----------------------


def _hook_ensure_read_only_uri(context: Mapping[str, Any]) -> None:
    ensure_read_only_uri(context["settings"].duckdb_uri)


def _hook_validate_select_only(context: Mapping[str, Any]) -> None:
    """Fail closed: a refused statement is a contract violation, not a bare guardrail error."""
    try:
        validate_select_only(context["sql"])
    except GuardrailError as exc:
        raise ContractViolation("genbi.execution.select_only", str(exc)) from exc


def _hook_row_cap_positive(context: Mapping[str, Any]) -> None:
    if context["settings"].row_cap <= 0:
        raise ContractViolation(
            "genbi.execution.row_cap_positive", f"row_cap is {context['settings'].row_cap}"
        )


def _hook_statement_timeout_positive(context: Mapping[str, Any]) -> None:
    if context["settings"].statement_timeout_seconds <= 0:
        raise ContractViolation(
            "genbi.execution.statement_timeout_positive",
            f"statement_timeout_seconds is {context['settings'].statement_timeout_seconds}",
        )


def _hook_receipt_when_confirmed(context: Mapping[str, Any]) -> None:
    if context.get("confirmed_by") and not context.get("approval_receipt"):
        raise ContractViolation(
            "genbi.approval.human_lock_bears_receipt",
            f"approval by {context['confirmed_by']!r} has no tool-approval receipt"
            f" for {PROMOTION_TOOL} — an unbound human approval is refused",
        )


def _hook_verified_read_only_target(context: Mapping[str, Any]) -> None:
    if not context.get("read_only_target_verified", False):
        raise ContractViolation(
            "genbi.persistence.verified_read_only_target",
            f"persistence target {context.get('database_id', '?')} was not verified"
            " server-side as the READ_ONLY analytics connection",
        )
