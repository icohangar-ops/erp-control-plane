"""Guardrail fixtures: must-block/must-pass policy cases per prompt-bearing surface.

The bd-coach ``config/dlp/run_tests.py`` pattern, applied to this repo's
prompt-bearing surfaces (the GenBI NL→SQL path and its CHP promotion gate):

- **Fixtures are data** — YAML files under ``config/guardrails/`` declare
  must-pass and must-block cases against a named policy entry point, plus the
  ``surface`` files whose change requires the fixture to change in the same PR.
- **The test file runs the real policy** — ``tests/test_guardrail_fixtures.py``
  executes every case against the live functions (no mocks, no
  reimplementations), so a policy change that alters a verdict fails CI.
- **The rot check closes the loop** — ``python -m guardrail_fixtures.check``
  fails when a declared surface file changes without a same-PR fixture update.
  Policy without fixtures rots; fixtures without a policy change are a no-op
  (that direction is allowed and encouraged — tightening fixtures is cheap).

The loader is fail-closed: a fixture with unknown fields, a missing section,
or an undeclared error class is a fixture error, not a skipped case.
"""
