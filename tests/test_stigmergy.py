"""Stigmergy board + exception agent: typed signals, per-type half-lives, determinism."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from control_plane.stigmergy import (
    BOARD_SCHEMA,
    KIND_HALF_LIVES,
    SALIENCE_FLOOR,
    Signal,
    SignalKind,
    StigmergyBoard,
    load_board,
    salience,
    save_board,
    tombstone_ratio,
)
from orchestration.exceptions import (
    DEFAULT_ALERT_RATIO,
    DISPOSITION_APPLY,
    DISPOSITION_REVIEW,
    DISPOSITION_SKIP_LOUD,
    review_fidelity,
    triage,
)

NOW = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)


def _post_run(board: StigmergyBoard, subject: str, **payload: object) -> Signal:
    return board.post(SignalKind.RECONCILIATION_RUN, subject, emitted_at=NOW, payload=dict(payload))


# --- determinism -------------------------------------------------------------


def test_signal_id_is_deterministic_content_hash() -> None:
    first = _post_run(StigmergyBoard(), "s/e", tombstoned=3)
    second = _post_run(StigmergyBoard(), "s/e", tombstoned=3)
    assert first.signal_id == second.signal_id


def test_post_is_idempotent_and_refreshes_emission() -> None:
    board = StigmergyBoard()
    later = NOW + timedelta(hours=1)
    board.post(SignalKind.RECONCILIATION_RUN, "s/e", emitted_at=NOW, payload={"k": 1})
    refreshed = board.post(SignalKind.RECONCILIATION_RUN, "s/e", emitted_at=later, payload={"k": 1})
    assert len(board.signals) == 1
    (signal,) = board.signals.values()
    assert signal.emitted_at == later
    assert refreshed.signal_id == signal.signal_id


def test_read_order_is_total_and_stable() -> None:
    board = StigmergyBoard()
    for subject in ("b/2", "a/1", "a/2"):
        _post_run(board, subject, tombstoned=0)
    keys = [s.subject for s in board.read(NOW)]
    assert keys == ["a/1", "a/2", "b/2"]
    assert [s.subject for s in board.read(NOW)] == keys  # same order twice


def test_two_boards_fed_the_same_events_are_identical() -> None:
    def feed(board: StigmergyBoard) -> list[Signal]:
        _post_run(board, "s/invoice_lines", tombstoned=1, tombstone_ratio=0.01)
        _post_run(board, "s/items", tombstoned=0, tombstone_ratio=0.0)
        board.post(
            SignalKind.KEY_SCAN_MISSING, "s/vendors", emitted_at=NOW, payload={"detail": "x"}
        )
        return board.read(NOW)

    left = feed(StigmergyBoard())
    right = feed(StigmergyBoard())
    assert [s.signal_id for s in left] == [s.signal_id for s in right]


# --- per-type half-lives -----------------------------------------------------


def test_signal_kinds_carry_distinct_half_lives() -> None:
    assert len({KIND_HALF_LIVES[k] for k in SignalKind}) == len(SignalKind)
    # run noise fades fastest; dispositions live longest
    assert (
        KIND_HALF_LIVES[SignalKind.RECONCILIATION_RUN]
        < KIND_HALF_LIVES[SignalKind.KEY_SCAN_MISSING]
        < KIND_HALF_LIVES[SignalKind.EXCEPTION_DISPOSITION]
    )


def test_salience_halves_each_half_life() -> None:
    board = StigmergyBoard()
    signal = _post_run(board, "s/e", tombstoned=0)
    half_life = timedelta(seconds=KIND_HALF_LIVES[SignalKind.RECONCILIATION_RUN])
    assert salience(signal, NOW) == 1.0
    assert salience(signal, NOW + half_life) == pytest.approx(0.5)
    assert salience(signal, NOW + 2 * half_life) == pytest.approx(0.25)
    # a same-age KEY_SCAN_MISSING signal decays slower than the run signal
    skip = board.post(SignalKind.KEY_SCAN_MISSING, "s/e", emitted_at=NOW, payload={})
    assert salience(skip, NOW + half_life) > salience(signal, NOW + half_life)


def test_sweep_drops_only_signals_below_the_floor() -> None:
    board = StigmergyBoard()
    fresh = _post_run(board, "s/fresh", tombstoned=0)
    dead = _post_run(board, "s/dead", tombstoned=0)
    # Push the dead signal ~11 half-lives into the past: below the 2^-10 floor.
    ancient = NOW - timedelta(seconds=KIND_HALF_LIVES[SignalKind.RECONCILIATION_RUN] * 11)
    board.signals[dead.signal_id] = Signal(
        signal_id=dead.signal_id,
        kind=dead.kind,
        subject=dead.subject,
        payload=dead.payload,
        half_life_seconds=dead.half_life_seconds,
        emitted_at=ancient,
    )
    assert salience(board.signals[dead.signal_id], NOW) < SALIENCE_FLOOR
    swept = board.sweep(NOW)
    assert swept == 1
    assert dead.signal_id not in board.signals
    assert fresh.signal_id in board.signals


def test_min_salience_filter_excludes_stale_but_keeps_fresh() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/e", tombstoned=0)
    cutoff = 0.95
    # 10 minutes old (0.5^(10m/12h) ≈ 0.990): fresh enough.
    assert len(board.read(NOW + timedelta(minutes=10), min_salience=cutoff)) == 1
    # 2 hours old (0.5^(2h/12h) ≈ 0.891): below the cutoff, still on the board.
    assert board.read(NOW + timedelta(hours=2), min_salience=cutoff) == []
    assert len(board.read(NOW + timedelta(hours=2))) == 1  # no cutoff -> still visible


# --- snapshots ---------------------------------------------------------------


def test_save_then_load_roundtrips(tmp_path) -> None:  # type: ignore[no-untyped-def]
    board = StigmergyBoard()
    _post_run(board, "s/invoice_lines", tombstoned=2, tombstone_ratio=0.2)
    board.post(SignalKind.KEY_SCAN_MISSING, "s/vendors", emitted_at=NOW, payload={"detail": "d"})
    path = save_board(board, tmp_path / "board.json", NOW)

    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["schema"] == BOARD_SCHEMA
    reloaded = load_board(path)
    assert set(reloaded.signals) == set(board.signals)
    for signal_id, signal in board.signals.items():
        assert reloaded.signals[signal_id] == signal


def test_load_missing_file_is_an_empty_board(tmp_path) -> None:  # type: ignore[no-untyped-def]
    board = load_board(tmp_path / "absent.json")
    assert isinstance(board, StigmergyBoard)
    assert board.signals == {}


def test_load_rejects_unknown_schema(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "board.json"
    path.write_text(json.dumps({"schema": "some-other/v9", "signals": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        load_board(path)


def test_snapshot_sweeps_dead_signals(tmp_path) -> None:  # type: ignore[no-untyped-def]
    board = StigmergyBoard()
    dead = _post_run(board, "s/e", tombstoned=0)
    ancient = NOW - timedelta(seconds=KIND_HALF_LIVES[SignalKind.RECONCILIATION_RUN] * 11)
    board.signals[dead.signal_id] = Signal(
        signal_id=dead.signal_id,
        kind=dead.kind,
        subject=dead.subject,
        payload=dead.payload,
        half_life_seconds=dead.half_life_seconds,
        emitted_at=ancient,
    )
    snapshot = board.snapshot(NOW)
    assert snapshot["signals"] == []


# --- exception agent ---------------------------------------------------------


def test_clean_run_produces_no_exception_signal() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/items", tombstoned=0, tombstone_ratio=0.0)
    dispositions = triage(board, now=NOW)
    assert dispositions == []
    assert board.read(NOW, kinds=(SignalKind.EXCEPTION_DISPOSITION,)) == []


def test_normal_tombstone_run_is_applied() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/items", tombstoned=2, tombstone_ratio=0.01)
    dispositions = triage(board, now=NOW)
    assert [d.disposition for d in dispositions] == [DISPOSITION_APPLY]
    posted = board.read(NOW, kinds=(SignalKind.EXCEPTION_DISPOSITION,))
    assert [p.payload["disposition"] for p in posted] == [DISPOSITION_APPLY]


def test_mass_tombstone_run_is_held_for_review() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/invoice_lines", tombstoned=50, tombstone_ratio=0.45)
    dispositions = triage(board, now=NOW)
    assert [d.disposition for d in dispositions] == [DISPOSITION_REVIEW]
    assert "suspected feed glitch" in dispositions[0].reason


def test_missing_key_scan_is_skipped_loud() -> None:
    board = StigmergyBoard()
    board.post(SignalKind.KEY_SCAN_MISSING, "s/vendors", emitted_at=NOW, payload={"detail": "d"})
    dispositions = triage(board, now=NOW)
    assert [d.disposition for d in dispositions] == [DISPOSITION_SKIP_LOUD]


def test_triage_respects_a_tuned_alert_ratio() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/items", tombstoned=5, tombstone_ratio=0.12)
    assert triage(board, now=NOW)[0].disposition == DISPOSITION_REVIEW  # default 0.10
    board2 = StigmergyBoard()
    _post_run(board2, "s/items", tombstoned=5, tombstone_ratio=0.12)
    assert triage(board2, now=NOW, alert_ratio=0.25)[0].disposition == DISPOSITION_APPLY


def test_dispositions_are_deterministic_across_runs() -> None:
    def once() -> list[str]:
        board = StigmergyBoard()
        _post_run(board, "a/items", tombstoned=1, tombstone_ratio=0.02)
        _post_run(board, "b/invoice_lines", tombstoned=9, tombstone_ratio=0.3)
        board.post(SignalKind.KEY_SCAN_MISSING, "c/vendors", emitted_at=NOW, payload={})
        return [f"{d.subject}:{d.disposition}" for d in triage(board, now=NOW)]

    assert once() == once()


def test_review_fidelity_scores_flagged_runs() -> None:
    board = StigmergyBoard()
    _post_run(board, "s/e", tombstoned=5, tombstone_ratio=0.5)
    review = triage(board, now=NOW)
    assert review_fidelity(review, false_delete_rate=0.8) == "CORRECT"
    assert review_fidelity(review, false_delete_rate=0.1) == "FALSE_ALARM"
    assert review_fidelity([], false_delete_rate=0.9) == "UNFLAGGED"


def test_default_alert_ratio_matches_exception_agent_constant() -> None:
    # The learner tunes the threshold the exception agent uses; the starting
    # value must be the same constant in both modules.
    assert DEFAULT_ALERT_RATIO == 0.10


# --- tombstone ratio ---------------------------------------------------------


def test_tombstone_ratio_zero_warehouse_is_never_a_signal() -> None:
    assert tombstone_ratio(0, 0) == 0.0
    assert tombstone_ratio(5, 0) == 0.0
    assert tombstone_ratio(3, 30) == pytest.approx(0.1)
