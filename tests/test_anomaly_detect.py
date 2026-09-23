"""Tests for the cron anomaly-detect script's check_anomaly tuning.

The script lives in scripts/ (baked into the image), not the package, so it's
loaded by path. The focus is the ZERO rule: a found-nothing run should only be
flagged for a provider that normally finds documents, not for a legitimately
sparse low-volume provider.
"""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "anomaly-detect.py"


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("anomaly_detect", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _run(created=0, skipped=0, errors=0, status="ok", exit_code=0):
    return {
        "provider": "p",
        "command": "cases",
        "created": created,
        "skipped": skipped,
        "errors": errors,
        "status": status,
        "exit_code": exit_code,
    }


def test_sparse_provider_quiet_day_not_flagged(mod):
    # Baseline that mostly finds nothing (sparse small court) + a 0/0 latest.
    history = [_run() for _ in range(13)] + [_run()]
    assert mod.check_anomaly("p", "cases", history) is None


def test_active_provider_going_silent_is_zero(mod):
    # Baseline reliably finds documents (avg found >= ZERO_MIN_BASELINE), then
    # finds nothing — that is the genuine "went silent" anomaly.
    history = [_run(created=0, skipped=5) for _ in range(13)] + [_run()]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "ZERO"


def test_healthy_duplicate_day_not_low(mod):
    # Active provider that re-saw its whole window (skipped>0, created=0):
    # nothing new, but healthy — must not be flagged LOW.
    history = [_run(created=3, skipped=10) for _ in range(13)] + [
        _run(created=0, skipped=10)
    ]
    assert mod.check_anomaly("p", "cases", history) is None


def test_low_only_when_no_skips(mod):
    # Genuinely fewer documents found (skipped==0, created far below baseline).
    history = [_run(created=10, skipped=0) for _ in range(13)] + [
        _run(created=1, skipped=0)
    ]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "LOW"


def test_crash_flagged(mod):
    history = [_run(created=5) for _ in range(13)] + [_run(exit_code=2)]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "CRASH"


def test_errors_flagged(mod):
    # A run that completed but failed to ingest some docs (court_not_found etc.).
    history = [_run(created=5) for _ in range(13)] + [
        _run(created=0, skipped=0, errors=3, status="partial", exit_code=1)
    ]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "ERRORS"


def test_partial_status_flagged_even_with_zero_errors_field(mod):
    history = [_run(created=5) for _ in range(13)] + [_run(status="partial")]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "ERRORS"


def test_high_burst_flagged(mod):
    history = [_run(created=2, skipped=0) for _ in range(13)] + [
        _run(created=50, skipped=0)
    ]
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "HIGH"


def test_insufficient_baseline_not_flagged(mod):
    history = [_run(), _run()]  # < 3 baseline points
    assert mod.check_anomaly("p", "cases", history) is None


def test_zero_min_baseline_is_tunable(mod, monkeypatch):
    # Below-threshold baseline → suppressed; raising the bar would suppress more.
    history = [_run(created=0, skipped=1) for _ in range(13)] + [_run()]
    monkeypatch.setattr(mod, "ZERO_MIN_BASELINE", 5.0)
    assert mod.check_anomaly("p", "cases", history) is None
    monkeypatch.setattr(mod, "ZERO_MIN_BASELINE", 0.5)
    result = mod.check_anomaly("p", "cases", history)
    assert result is not None and result[0] == "ZERO"


# --- Corpus-level stall -------------------------------------------------
#
# The per-provider checks judge each provider against its own baseline, and
# ZERO_MIN_BASELINE deliberately suppresses providers that are normally quiet.
# Correct in isolation, blind in aggregate: when writes fail globally every
# provider reports a plausible zero while nothing is stored at all.
#
# That is not hypothetical. An SSL redirect made every write hang and the
# corpus did not grow for 26 days, while these checks reported only
# per-provider crashes and the daily mail said nothing about the corpus.


def _dated(days_ago, created, provider="p"):
    """A run entry in the shape ``load_history()`` actually returns.

    Flat list of dicts keyed by ``finished_at`` -- not a dict-of-lists, and not
    a ``date`` field. The first version of these tests invented both, so they
    passed while the check crashed on the first real run with
    ``AttributeError: 'list' object has no attribute 'values'``.
    """
    from datetime import datetime, timedelta, timezone

    day = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return {
        "provider": provider,
        "command": "cases",
        "created": created,
        "skipped": 0,
        "errors": 0,
        "status": "ok",
        "exit_code": 0,
        "finished_at": day.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def test_history_fixture_matches_load_history_shape(mod, tmp_path, monkeypatch):
    """Pin the fixture against the real loader, so it cannot drift again."""
    hist = tmp_path / "history.jsonl"
    import json

    hist.write_text(json.dumps(_dated(1, 5)) + "\n")
    monkeypatch.setattr(mod, "HISTORY_FILE", str(hist))
    loaded = mod.load_history()
    assert isinstance(loaded, list)
    assert loaded[0].keys() == _dated(1, 5).keys()


def test_recent_write_is_not_a_stall(mod):
    assert mod.check_corpus_stall([_dated(5, 3), _dated(0, 2)]) is None


def test_no_write_for_longer_than_threshold_is_flagged(mod):
    history = [_dated(30, 4)] + [_dated(d, 0) for d in range(9)]
    result = mod.check_corpus_stall(history)
    assert result is not None
    assert result[0] == "CORPUS_STALL"
    assert "not growing" in result[1]


def test_the_26_day_outage_would_have_been_caught(mod):
    """Replay the real shape: every provider succeeds, nothing is written."""
    history = []
    for name in ("nrw", "by", "hb", "eu", "ns"):
        history.append(_dated(26, 5, name))
        history += [_dated(d, 0, name) for d in range(26)]
    result = mod.check_corpus_stall(history)
    assert result is not None
    assert result[0] == "CORPUS_STALL"
    assert "26 day" in result[1]


def test_quiet_providers_alone_do_not_trip_it(mod):
    """A sparse provider writing nothing is fine while others still write."""
    history = [_dated(d, 0, "sparse") for d in range(20)] + [_dated(0, 7, "busy")]
    assert mod.check_corpus_stall(history) is None


def test_no_recorded_write_at_all_is_flagged(mod):
    result = mod.check_corpus_stall([_dated(d, 0) for d in range(5)])
    assert result is not None
    assert result[0] == "CORPUS_STALL"


def test_unparseable_timestamp_is_skipped_not_fatal(mod):
    bad = _dated(0, 5)
    bad["finished_at"] = "not-a-timestamp"
    result = mod.check_corpus_stall([bad])
    assert result is not None
    assert result[0] == "CORPUS_STALL"


def test_check_can_be_disabled(mod, monkeypatch):
    monkeypatch.setattr(mod, "CORPUS_STALL_DAYS", 0)
    assert mod.check_corpus_stall([_dated(99, 0)]) is None


def test_check_all_surfaces_the_stall(mod, monkeypatch, capsys):
    """The check must be wired into check_all(), not merely exist."""
    monkeypatch.setattr(mod, "capabilities", lambda: {"cases": ["p"]})
    monkeypatch.setattr(mod, "load_history", lambda: [_dated(40, 3)])
    monkeypatch.setattr(mod, "monitored_providers", lambda: [])

    issues = mod.check_all()

    assert any(t == "CORPUS_STALL" for t, _ in issues), issues
    assert "CORPUS_STALL" in capsys.readouterr().out
