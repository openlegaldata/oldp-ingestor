"""Result file writing and reading for production monitoring."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def write_result(
    results_dir,
    command,
    provider,
    started_at,
    finished_at,
    created,
    skipped,
    errors,
    status=None,
):
    """Write a JSON result file atomically via tmp+rename.

    Args:
        results_dir: Directory to write result files to.
        command: CLI command name (e.g. "cases", "laws").
        provider: Provider name (e.g. "ris", "juris-bb").
        started_at: datetime when the run started.
        finished_at: datetime when the run finished.
        created: Number of items created.
        skipped: Number of items skipped (409 duplicates).
        errors: Number of errors encountered.
        status: Override status. If None, auto-determined from errors.
    """
    os.makedirs(results_dir, exist_ok=True)

    if status is None:
        status = "ok" if errors == 0 else "partial"

    duration_seconds = int((finished_at - started_at).total_seconds())

    result = {
        "provider": provider,
        "command": command,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration_seconds,
        "status": status,
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }

    filename = f"{command}_{provider}.json"
    target = os.path.join(results_dir, filename)

    # Atomic write: write to temp file in same dir, then rename
    fd, tmp_path = tempfile.mkstemp(dir=results_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, target)
    except BaseException:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def read_all_results(results_dir):
    """Read all result JSON files from a directory.

    Returns:
        List of result dicts, sorted by (command, provider).
    """
    results = []
    results_path = Path(results_dir)

    if not results_path.is_dir():
        return results

    for entry in results_path.iterdir():
        if entry.suffix == ".json" and entry.is_file():
            try:
                with open(entry) as f:
                    data = json.load(f)
                results.append(data)
            except (json.JSONDecodeError, OSError):
                continue

    results.sort(key=lambda r: (r.get("command", ""), r.get("provider", "")))
    return results


# All known providers with their supported commands
ALL_PROVIDERS = {
    "cases": [
        "ris",
        "rii",
        "by",
        "nrw",
        "ns",
        "eu",
        "hb",
        "sn-ovg",
        "sn",
        "sn-verfgh",
        "juris-bb",
        "juris-hh",
        "juris-mv",
        "juris-rlp",
        "juris-sa",
        "juris-sh",
        "juris-bw",
        "juris-sl",
        "juris-he",
        "juris-th",
    ],
    "laws": [
        "ris",
    ],
}


def get_all_expected():
    """Return set of (command, provider) tuples for all known providers."""
    expected = set()
    for command, providers in ALL_PROVIDERS.items():
        for provider in providers:
            expected.add((command, provider))
    return expected


def format_duration(seconds):
    """Format duration in seconds to human-readable string."""
    if seconds is None:
        return ""
    minutes, secs = divmod(seconds, 60)
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def format_status_table(results, stale_hours=168):
    """Format results into a status table string.

    Args:
        results: List of result dicts from read_all_results().
        stale_hours: Hours after which a result is considered stale (default 168 = 7 days).

    Returns:
        Formatted table string.
    """
    now = datetime.now(timezone.utc)

    # Build lookup of actual results
    result_map = {}
    for r in results:
        key = (r.get("command", ""), r.get("provider", ""))
        result_map[key] = r

    # Build rows: all expected providers + any extra results
    expected = get_all_expected()
    all_keys = expected | set(result_map.keys())

    rows = []
    for command, provider in sorted(all_keys):
        r = result_map.get((command, provider))
        if r is None:
            rows.append(
                {
                    "provider": provider,
                    "command": command,
                    "last_run": "(never)",
                    "duration": "",
                    "status": "",
                    "created": "",
                    "skipped": "",
                    "errors": "",
                    "stale": "YES",
                }
            )
        else:
            finished = r.get("finished_at", "")
            stale = False
            last_run = ""
            if finished:
                try:
                    dt = datetime.fromisoformat(finished)
                    last_run = dt.strftime("%Y-%m-%d %H:%M")
                    age_hours = (now - dt).total_seconds() / 3600
                    if age_hours > stale_hours:
                        stale = True
                except (ValueError, TypeError):
                    last_run = finished

            rows.append(
                {
                    "provider": provider,
                    "command": command,
                    "last_run": last_run,
                    "duration": format_duration(r.get("duration_seconds")),
                    "status": r.get("status", ""),
                    "created": str(r.get("created", "")),
                    "skipped": str(r.get("skipped", "")),
                    "errors": str(r.get("errors", "")),
                    "stale": "YES" if stale else "",
                }
            )

    if not rows:
        return "No results found."

    # Calculate column widths
    headers = {
        "provider": "Provider",
        "command": "Command",
        "last_run": "Last Run",
        "duration": "Duration",
        "status": "Status",
        "created": "Created",
        "skipped": "Skipped",
        "errors": "Errors",
        "stale": "Stale",
    }
    col_order = list(headers.keys())
    widths = {col: len(headers[col]) for col in col_order}
    for row in rows:
        for col in col_order:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    # Format header
    header_line = "  ".join(headers[col].ljust(widths[col]) for col in col_order)
    separator = "  ".join("\u2500" * widths[col] for col in col_order)

    # Format rows
    lines = [header_line, separator]
    for row in rows:
        line = "  ".join(str(row.get(col, "")).ljust(widths[col]) for col in col_order)
        lines.append(line)

    return "\n".join(lines)


def check_health(results, stale_hours=168):
    """Check if all providers are healthy.

    Returns:
        True if all providers have recent, successful results.
    """
    now = datetime.now(timezone.utc)

    result_map = {}
    for r in results:
        key = (r.get("command", ""), r.get("provider", ""))
        result_map[key] = r

    expected = get_all_expected()

    for key in expected:
        r = result_map.get(key)
        if r is None:
            return False  # never run
        if r.get("status") == "error":
            return False
        finished = r.get("finished_at", "")
        if finished:
            try:
                dt = datetime.fromisoformat(finished)
                age_hours = (now - dt).total_seconds() / 3600
                if age_hours > stale_hours:
                    return False
            except (ValueError, TypeError):
                return False

    return True
