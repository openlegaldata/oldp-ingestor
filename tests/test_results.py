import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

from oldp_ingestor.results import (
    check_health,
    format_duration,
    format_status_table,
    get_all_expected,
    read_all_results,
    write_result,
)


def test_write_result_creates_file():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 10, 30, tzinfo=timezone.utc)

        write_result(
            d, "cases", "rii", started, finished, created=50, skipped=10, errors=0
        )

        path = os.path.join(d, "cases_rii.json")
        assert os.path.exists(path)

        with open(path) as f:
            data = json.load(f)

        assert data["provider"] == "rii"
        assert data["command"] == "cases"
        assert data["status"] == "ok"
        assert data["created"] == 50
        assert data["skipped"] == 10
        assert data["errors"] == 0
        assert data["duration_seconds"] == 630


def test_write_result_partial_status():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 5, 0, tzinfo=timezone.utc)

        write_result(
            d, "cases", "ris", started, finished, created=10, skipped=5, errors=3
        )

        with open(os.path.join(d, "cases_ris.json")) as f:
            data = json.load(f)

        assert data["status"] == "partial"
        assert data["errors"] == 3


def test_write_result_explicit_error_status():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 0, 5, tzinfo=timezone.utc)

        write_result(
            d,
            "cases",
            "by",
            started,
            finished,
            created=0,
            skipped=0,
            errors=1,
            status="error",
        )

        with open(os.path.join(d, "cases_by.json")) as f:
            data = json.load(f)

        assert data["status"] == "error"


def test_write_result_creates_directory():
    with tempfile.TemporaryDirectory() as d:
        subdir = os.path.join(d, "nested", "results")
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 1, 0, tzinfo=timezone.utc)

        write_result(
            subdir, "laws", "ris", started, finished, created=5, skipped=0, errors=0
        )

        assert os.path.exists(os.path.join(subdir, "laws_ris.json"))


def test_write_result_overwrites_existing():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 1, 0, tzinfo=timezone.utc)

        write_result(
            d, "cases", "rii", started, finished, created=10, skipped=0, errors=0
        )
        write_result(
            d, "cases", "rii", started, finished, created=20, skipped=5, errors=1
        )

        with open(os.path.join(d, "cases_rii.json")) as f:
            data = json.load(f)

        assert data["created"] == 20
        assert data["errors"] == 1


def test_read_all_results_empty_dir():
    with tempfile.TemporaryDirectory() as d:
        results = read_all_results(d)
        assert results == []


def test_read_all_results_nonexistent_dir():
    results = read_all_results("/tmp/nonexistent_dir_xyz_123")
    assert results == []


def test_read_all_results_reads_files():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 10, 0, tzinfo=timezone.utc)

        write_result(
            d, "cases", "rii", started, finished, created=50, skipped=10, errors=0
        )
        write_result(
            d, "cases", "ris", started, finished, created=100, skipped=20, errors=2
        )
        write_result(
            d, "laws", "ris", started, finished, created=30, skipped=5, errors=0
        )

        results = read_all_results(d)
        assert len(results) == 3
        # Sorted by (command, provider)
        assert results[0]["provider"] == "rii"
        assert results[0]["command"] == "cases"
        assert results[1]["provider"] == "ris"
        assert results[1]["command"] == "cases"
        assert results[2]["provider"] == "ris"
        assert results[2]["command"] == "laws"


def test_read_all_results_skips_invalid_json():
    with tempfile.TemporaryDirectory() as d:
        # Write a valid result
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 1, 0, tzinfo=timezone.utc)
        write_result(
            d, "cases", "rii", started, finished, created=5, skipped=0, errors=0
        )

        # Write an invalid JSON file
        with open(os.path.join(d, "bad.json"), "w") as f:
            f.write("not json{{{")

        results = read_all_results(d)
        assert len(results) == 1


def test_read_all_results_ignores_non_json():
    with tempfile.TemporaryDirectory() as d:
        started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 2, 9, 3, 1, 0, tzinfo=timezone.utc)
        write_result(
            d, "cases", "rii", started, finished, created=5, skipped=0, errors=0
        )

        # Write a non-JSON file
        with open(os.path.join(d, "readme.txt"), "w") as f:
            f.write("hello")

        results = read_all_results(d)
        assert len(results) == 1


def test_get_all_expected():
    expected = get_all_expected()
    assert ("cases", "ris") in expected
    assert ("cases", "rii") in expected
    assert ("cases", "juris-bb") in expected
    assert ("cases", "hb") in expected
    assert ("cases", "sn-ovg") in expected
    assert ("cases", "sn") in expected
    assert ("cases", "sn-verfgh") in expected
    assert ("laws", "ris") in expected
    # 20 case providers + 1 law provider = 21
    assert len(expected) == 21


def test_format_duration():
    assert format_duration(0) == "0s"
    assert format_duration(30) == "30s"
    assert format_duration(60) == "1m 00s"
    assert format_duration(90) == "1m 30s"
    assert format_duration(630) == "10m 30s"
    assert format_duration(None) == ""


def test_format_status_table_with_results():
    results = [
        {
            "provider": "ris",
            "command": "cases",
            "started_at": "2026-02-09T03:00:00+00:00",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 312,
            "status": "ok",
            "created": 120,
            "skipped": 45,
            "errors": 0,
        },
    ]
    table = format_status_table(results, stale_hours=168)
    assert "Provider" in table
    assert "ris" in table
    assert "cases" in table
    assert "ok" in table


def test_format_status_table_shows_never_run():
    table = format_status_table([], stale_hours=168)
    assert "(never)" in table
    assert "YES" in table


def test_format_status_table_shows_stale():
    old_time = (datetime.now(timezone.utc) - timedelta(hours=200)).isoformat()
    results = [
        {
            "provider": "ris",
            "command": "cases",
            "started_at": old_time,
            "finished_at": old_time,
            "duration_seconds": 60,
            "status": "ok",
            "created": 10,
            "skipped": 0,
            "errors": 0,
        },
    ]
    table = format_status_table(results, stale_hours=168)
    # The ris cases row should be stale
    lines = table.split("\n")
    # Match "ris " (with trailing space) to avoid matching "juris-*"
    ris_cases_line = [
        line for line in lines if line.lstrip().startswith("ris ") and "cases" in line
    ]
    assert len(ris_cases_line) == 1
    assert "YES" in ris_cases_line[0]


def test_check_health_all_fresh():
    now = datetime.now(timezone.utc)
    results = []
    expected = get_all_expected()
    for command, provider in expected:
        results.append(
            {
                "provider": provider,
                "command": command,
                "started_at": now.isoformat(),
                "finished_at": now.isoformat(),
                "duration_seconds": 60,
                "status": "ok",
                "created": 10,
                "skipped": 0,
                "errors": 0,
            }
        )
    assert check_health(results, stale_hours=168) is True


def test_check_health_missing_provider():
    # Empty results = all providers missing
    assert check_health([], stale_hours=168) is False


def test_check_health_error_status():
    now = datetime.now(timezone.utc)
    results = []
    expected = get_all_expected()
    for command, provider in expected:
        results.append(
            {
                "provider": provider,
                "command": command,
                "started_at": now.isoformat(),
                "finished_at": now.isoformat(),
                "duration_seconds": 60,
                "status": "error" if provider == "rii" else "ok",
                "created": 0 if provider == "rii" else 10,
                "skipped": 0,
                "errors": 1 if provider == "rii" else 0,
            }
        )
    assert check_health(results, stale_hours=168) is False


def test_check_health_stale_provider():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(hours=200)).isoformat()
    results = []
    expected = get_all_expected()
    for command, provider in expected:
        finished = old if provider == "rii" else now.isoformat()
        results.append(
            {
                "provider": provider,
                "command": command,
                "started_at": finished,
                "finished_at": finished,
                "duration_seconds": 60,
                "status": "ok",
                "created": 10,
                "skipped": 0,
                "errors": 0,
            }
        )
    assert check_health(results, stale_hours=168) is False
