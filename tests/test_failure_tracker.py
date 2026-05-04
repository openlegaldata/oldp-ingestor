"""Tests for the persistent per-doc retry tracker.

Coverage:
  * NullFailureTracker is a true no-op (default Provider behaviour)
  * record_failure persists, increments, and stops at max_retries
  * record_success clears tracking for a doc
  * should_skip flips on at exactly max_retries
  * state survives across instances (the cron-run case)
  * malformed state file is tolerated
  * provider isolation: separate state files per provider name
  * once-per-run skip log (subsequent calls don't re-emit)
"""

import json
import os

import pytest

from oldp_ingestor.providers.failure_tracker import (
    FailureTracker,
    NullFailureTracker,
)


# ---------------------------------------------------------------------------
# NullFailureTracker
# ---------------------------------------------------------------------------


def test_null_tracker_never_skips():
    t = NullFailureTracker()
    assert t.should_skip("anything") is False
    # Even after recording failures, still never skips
    assert t.record_failure("anything", "boom") is False
    assert t.should_skip("anything") is False
    t.record_success("anything")
    assert t.stats() == {"tracked": 0, "exhausted": 0}


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_bad_inputs(tmp_path):
    with pytest.raises(ValueError):
        FailureTracker(state_dir="", provider="x")
    with pytest.raises(ValueError):
        FailureTracker(state_dir=str(tmp_path), provider="")
    with pytest.raises(ValueError):
        FailureTracker(state_dir=str(tmp_path), provider="x", max_retries=-1)


def test_state_file_path_uses_provider_name(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    t.record_failure("doc-1", "x")
    assert (tmp_path / "failures_by.json").exists()


# ---------------------------------------------------------------------------
# record_failure / should_skip / threshold
# ---------------------------------------------------------------------------


def test_record_failure_increments_and_persists(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    t.record_failure("doc-1", "first")
    t.record_failure("doc-1", "second")

    on_disk = json.loads((tmp_path / "failures_by.json").read_text())
    assert on_disk["doc-1"]["count"] == 2
    assert on_disk["doc-1"]["last_error"] == "second"
    assert "first_failure_at" in on_disk["doc-1"]
    assert "last_failure_at" in on_disk["doc-1"]


def test_should_skip_only_after_threshold(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    assert t.should_skip("doc-1") is False
    t.record_failure("doc-1", "1")
    assert t.should_skip("doc-1") is False
    t.record_failure("doc-1", "2")
    assert t.should_skip("doc-1") is False
    just_exhausted = t.record_failure("doc-1", "3")
    assert just_exhausted is True
    assert t.should_skip("doc-1") is True
    # Still True after further (idempotent) failure recordings
    further = t.record_failure("doc-1", "4")
    assert further is False  # only the threshold-crossing call returns True
    assert t.should_skip("doc-1") is True


def test_max_retries_zero_disables_skipping(tmp_path):
    """max_retries=0 means: track failures but never permanently skip."""
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=0)
    for i in range(20):
        t.record_failure("doc-1", str(i))
    assert t.should_skip("doc-1") is False
    assert t.stats() == {"tracked": 1, "exhausted": 0}


def test_record_success_clears_entry(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    t.record_failure("doc-1", "1")
    t.record_failure("doc-1", "2")
    t.record_success("doc-1")
    assert t.should_skip("doc-1") is False
    on_disk = json.loads((tmp_path / "failures_by.json").read_text())
    assert "doc-1" not in on_disk


def test_record_success_unknown_doc_is_noop(tmp_path):
    """Calling record_success for a doc that was never tracked must not error
    or create an empty entry. This matters because providers always call it
    on success, even if the doc never failed before."""
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    t.record_success("never-seen")  # should not raise
    # File may or may not exist yet, but if it does, the doc must not be in it
    if (tmp_path / "failures_by.json").exists():
        on_disk = json.loads((tmp_path / "failures_by.json").read_text())
        assert "never-seen" not in on_disk


# ---------------------------------------------------------------------------
# Persistence across instances (the cron-run case)
# ---------------------------------------------------------------------------


def test_state_persists_across_instances(tmp_path):
    """Simulate two cron runs: first run fails the doc twice, second loads
    the same state and exhausts the threshold."""
    t1 = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    t1.record_failure("doc-1", "run1-fail-1")
    t1.record_failure("doc-1", "run1-fail-2")

    # Brand-new instance reading the same file
    t2 = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    assert t2.should_skip("doc-1") is False
    just_exhausted = t2.record_failure("doc-1", "run2-fail-1")
    assert just_exhausted is True
    assert t2.should_skip("doc-1") is True


def test_provider_isolation(tmp_path):
    """Two providers sharing a state dir keep separate files."""
    by = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=3)
    rii = FailureTracker(state_dir=str(tmp_path), provider="rii", max_retries=3)
    for _ in range(3):
        by.record_failure("same-doc-id", "by-failure")
    assert by.should_skip("same-doc-id") is True
    assert rii.should_skip("same-doc-id") is False


def test_corrupt_state_file_starts_empty(tmp_path):
    """Garbage in the state file must not crash the run."""
    bad_path = tmp_path / "failures_by.json"
    bad_path.write_text("{not valid json")
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    assert t.stats() == {"tracked": 0, "exhausted": 0}
    # And the tracker still works — first failure starts counting at 1
    t.record_failure("doc-1", "ok")
    assert json.loads(bad_path.read_text())["doc-1"]["count"] == 1


def test_non_dict_state_file_starts_empty(tmp_path):
    bad_path = tmp_path / "failures_by.json"
    bad_path.write_text('["a list, not a dict"]')
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    assert t.stats() == {"tracked": 0, "exhausted": 0}


# ---------------------------------------------------------------------------
# Stats + once-per-run skip logging
# ---------------------------------------------------------------------------


def test_stats_counts_tracked_and_exhausted(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=2)
    t.record_failure("a", "x")  # count=1, not exhausted
    t.record_failure("b", "x")  # count=1
    t.record_failure("b", "x")  # count=2, exhausted
    assert t.stats() == {"tracked": 2, "exhausted": 1}


def test_skip_log_emitted_once_per_run(tmp_path, caplog):
    """Hitting should_skip 50 times for the same doc must not produce 50 log
    lines — that's the whole point of bounding retries."""
    import logging

    caplog.set_level(logging.INFO, logger="oldp_ingestor.providers.failure_tracker")
    t = FailureTracker(state_dir=str(tmp_path), provider="by", max_retries=2)
    t.record_failure("doc-1", "1")
    t.record_failure("doc-1", "2")  # exhausted
    caplog.clear()
    for _ in range(50):
        assert t.should_skip("doc-1") is True
    skip_records = [r for r in caplog.records if "Skipping" in r.message]
    assert len(skip_records) == 1


# ---------------------------------------------------------------------------
# Long error message truncation
# ---------------------------------------------------------------------------


def test_long_error_message_is_truncated(tmp_path):
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    huge = "x" * 5000
    t.record_failure("doc-1", huge)
    on_disk = json.loads((tmp_path / "failures_by.json").read_text())
    assert len(on_disk["doc-1"]["last_error"]) == 500


# ---------------------------------------------------------------------------
# state_dir created on first write
# ---------------------------------------------------------------------------


def test_state_dir_is_created_on_first_write(tmp_path):
    nested = tmp_path / "deep" / "nested"
    assert not nested.exists()
    t = FailureTracker(state_dir=str(nested), provider="by")
    t.record_failure("doc-1", "x")
    assert (nested / "failures_by.json").exists()


# ---------------------------------------------------------------------------
# Default Provider behaviour (no real tracker)
# ---------------------------------------------------------------------------


def test_provider_base_default_is_null_tracker():
    """The Provider base class must default to a tracker that never skips,
    so providers without an injected tracker behave exactly as before."""
    from oldp_ingestor.providers.base import Provider

    p = Provider()
    assert isinstance(p.failure_tracker, NullFailureTracker)
    assert p.failure_tracker.should_skip("x") is False


def test_atomic_write_does_not_leave_tmp_file(tmp_path):
    """The save path uses os.replace — no half-written tmp file should remain."""
    t = FailureTracker(state_dir=str(tmp_path), provider="by")
    t.record_failure("doc-1", "x")
    leftover = [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
    assert leftover == []


# ---------------------------------------------------------------------------
# Provider integration: BY with a permanently-corrupt doc
# ---------------------------------------------------------------------------


def test_by_provider_skips_doc_after_repeat_parse_failures(tmp_path, monkeypatch):
    """End-to-end: a BY doc that always XML-parse-fails must hit the retry
    threshold and stop being attempted on subsequent runs."""
    import io
    import zipfile

    from oldp_ingestor.providers.de.by import ByCaseProvider

    # Always-corrupt zip: contains an XML payload that lxml cannot parse.
    bad_zip = io.BytesIO()
    with zipfile.ZipFile(bad_zip, "w") as zf:
        zf.writestr("bad.xml", "this is not xml at all")
    bad_bytes = bad_zip.getvalue()

    page_html = '<a href="/Content/Document/BAD-DOC?hl=true">Doc</a>'
    call_count = {"n": 0}

    def mock_request(self, method, url, **kwargs):
        call_count["n"] += 1

        class Resp:
            status_code = 200
            url = "https://www.gesetze-bayern.de/Search/Hitlist"

            def raise_for_status(self):
                pass

        resp = Resp()
        if "Filter" in url:
            resp.text = "OK"
        elif "Search/Page" in url:
            # First call gets the page once, then empty to terminate
            resp.text = page_html if call_count["n"] <= 2 else ""
        elif "Content/Zip" in url:
            resp.raw = type(
                "raw",
                (),
                {
                    "decode_content": True,
                    "read": staticmethod(lambda: bad_bytes),
                },
            )
        else:
            resp.text = ""
        return resp

    monkeypatch.setattr(ByCaseProvider, "_request_with_retry", mock_request)

    # Cron run 1: tracker has no state, doc fails once, count=1
    provider = ByCaseProvider(request_delay=0, limit=10)
    provider.failure_tracker = FailureTracker(
        state_dir=str(tmp_path), provider="by", max_retries=3
    )
    provider.get_cases()
    state_path = tmp_path / "failures_by.json"
    assert json.loads(state_path.read_text())["BAD-DOC"]["count"] == 1

    # Cron runs 2 + 3 — the doc fails twice more, hitting threshold on run 3
    for _ in range(2):
        call_count["n"] = 0
        provider = ByCaseProvider(request_delay=0, limit=10)
        provider.failure_tracker = FailureTracker(
            state_dir=str(tmp_path), provider="by", max_retries=3
        )
        provider.get_cases()
    assert json.loads(state_path.read_text())["BAD-DOC"]["count"] == 3

    # Cron run 4 — tracker should already short-circuit before any ZIP fetch.
    # Count zip-fetching calls and assert it stays at 0.
    zip_calls = {"n": 0}

    def counting_mock(self, method, url, **kwargs):
        if "Content/Zip" in url:
            zip_calls["n"] += 1
        return mock_request(self, method, url, **kwargs)

    call_count["n"] = 0
    monkeypatch.setattr(ByCaseProvider, "_request_with_retry", counting_mock)

    provider = ByCaseProvider(request_delay=0, limit=10)
    provider.failure_tracker = FailureTracker(
        state_dir=str(tmp_path), provider="by", max_retries=3
    )
    provider.get_cases()
    assert zip_calls["n"] == 0  # tracker fully short-circuited the doc
    # Count is unchanged because we never re-attempted
    assert json.loads(state_path.read_text())["BAD-DOC"]["count"] == 3


def test_by_provider_clears_tracker_on_success(tmp_path, monkeypatch):
    """If a previously-failing doc starts parsing again, its entry is cleared."""
    import io
    import zipfile

    from oldp_ingestor.providers.de.by import ByCaseProvider

    state_path = tmp_path / "failures_by.json"
    # Pre-seed: doc-1 has 2 prior failures (below threshold of 3)
    state_path.write_text(
        json.dumps(
            {
                "GOOD-DOC": {
                    "count": 2,
                    "first_failure_at": "2026-01-01T00:00:00+00:00",
                    "last_failure_at": "2026-01-02T00:00:00+00:00",
                    "last_error": "transient lxml hiccup",
                }
            }
        )
    )

    good_xml = """<?xml version="1.0" encoding="utf-8"?>
<byrecht-rspr><metadaten><aktenzeichen>1 K 1/24</aktenzeichen>
<doktyp>Urt</doktyp><entsch-datum>2024-01-01</entsch-datum>
<gericht><gertyp>LG</gertyp><gerort>Test</gerort></gericht></metadaten>
<textdaten><tenor><body><div><p>OK</p></div></body></tenor>
<gruende><body><div><p>OK</p></div></body></gruende></textdaten></byrecht-rspr>"""

    good_zip = io.BytesIO()
    with zipfile.ZipFile(good_zip, "w") as zf:
        zf.writestr("good.xml", good_xml)
    good_bytes = good_zip.getvalue()

    page_html = '<a href="/Content/Document/GOOD-DOC?hl=true">Doc</a>'
    call_count = {"n": 0}

    def mock_request(self, method, url, **kwargs):
        call_count["n"] += 1

        class Resp:
            status_code = 200
            url = "https://www.gesetze-bayern.de/Search/Hitlist"

            def raise_for_status(self):
                pass

        resp = Resp()
        if "Filter" in url:
            resp.text = "OK"
        elif "Search/Page" in url:
            resp.text = page_html if call_count["n"] <= 2 else ""
        elif "Content/Zip" in url:
            resp.raw = type(
                "raw",
                (),
                {
                    "decode_content": True,
                    "read": staticmethod(lambda: good_bytes),
                },
            )
        else:
            resp.text = ""
        return resp

    monkeypatch.setattr(ByCaseProvider, "_request_with_retry", mock_request)

    provider = ByCaseProvider(request_delay=0, limit=10)
    provider.failure_tracker = FailureTracker(
        state_dir=str(tmp_path), provider="by", max_retries=3
    )
    cases = provider.get_cases()
    assert len(cases) == 1
    state_after = json.loads(state_path.read_text())
    assert "GOOD-DOC" not in state_after
