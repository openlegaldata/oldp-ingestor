#!/usr/bin/env python3
"""Anomaly detection for OLDP ingestor runs.

Checks the latest run for a provider against a rolling baseline from
ingestion-history.jsonl. Exits 0 if normal, 1 if anomaly detected.

Usage:
    anomaly-detect.py <provider> <command>
    anomaly-detect.py ris cases
    anomaly-detect.py --check-all          # check all providers for staleness

Environment variables:
    ANOMALY_LOW_THRESHOLD     — flag if created < baseline * threshold (default: 0.2)
    ANOMALY_HIGH_THRESHOLD    — flag if created > baseline * threshold (default: 5.0)
    ANOMALY_ZERO_MIN_BASELINE — min baseline avg (created+skipped) for a
                                found-nothing run to count as ZERO (default: 1.0);
                                below this the provider is treated as sparse
    STALE_HOURS_HTTP          — HTTP provider stale after N hours (default: 36)
    STALE_HOURS_PLAYWRIGHT    — Playwright provider stale after N hours (default: 192)
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HISTORY_FILE = "/app/state/ingestion-history.jsonl"
BASELINE_POINTS = 14

LOW_THRESHOLD = float(os.environ.get("ANOMALY_LOW_THRESHOLD", "0.2"))
HIGH_THRESHOLD = float(os.environ.get("ANOMALY_HIGH_THRESHOLD", "5.0"))
STALE_HOURS_HTTP = int(os.environ.get("STALE_HOURS_HTTP", "36"))
STALE_HOURS_PLAYWRIGHT = int(os.environ.get("STALE_HOURS_PLAYWRIGHT", "192"))
# A run that found nothing (created=0 AND skipped=0) is only an anomaly for a
# provider that normally finds documents. If the baseline average of
# (created+skipped) is below this, the provider is treated as legitimately
# sparse — a small court or an empty rolling window — and a quiet day is not
# flagged. Most OLDP providers are low-volume, so without this they false-alarm.
ZERO_MIN_BASELINE = float(os.environ.get("ANOMALY_ZERO_MIN_BASELINE", "1.0"))

# Law providers monitored for staleness. The case roster is derived from the
# ingestor package (see monitored_providers); laws are listed explicitly here
# because gii/eurlex run weekly/ad-hoc and a daily staleness check would
# false-alarm — that is a scheduling choice, not a provider capability.
MONITORED_LAW_PROVIDERS = ["ris"]

_CAPABILITIES = None


def load_capabilities():
    """Provider capabilities from the ingestor package — the single source of
    truth for which providers exist and whether each is Playwright-based.

    Shells out to ``oldp-ingestor providers`` (JSON) and flattens it to
    ``{(command, provider): {"kind", "date_from"}}``. Returns ``{}`` if the
    CLI is unavailable; callers treat that as a hard error rather than
    silently monitoring nothing.
    """
    try:
        out = subprocess.run(
            ["oldp-ingestor", "providers"],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        ).stdout
        caps = json.loads(out)
    except Exception:
        return {}
    flat = {}
    for command, providers in caps.items():
        for provider, entry in providers.items():
            flat[(command, provider)] = entry
    return flat


def capabilities():
    global _CAPABILITIES
    if _CAPABILITIES is None:
        _CAPABILITIES = load_capabilities()
    return _CAPABILITIES


def is_playwright(provider, command):
    return capabilities().get((command, provider), {}).get("kind") == "playwright"


def monitored_providers():
    """(command, provider) pairs to staleness-check: all case providers from
    the package, plus the explicitly-chosen law providers."""
    cases = sorted(
        (cmd, prov) for (cmd, prov) in capabilities() if cmd == "cases"
    )
    laws = [("laws", prov) for prov in MONITORED_LAW_PROVIDERS]
    return laws + cases


def load_history():
    entries = []
    if not os.path.exists(HISTORY_FILE):
        return entries
    with open(HISTORY_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


def get_provider_history(entries, provider, command):
    return [
        e for e in entries
        if e.get("provider") == provider and e.get("command") == command
    ]


def check_anomaly(provider, command, history):
    """Check latest run against rolling baseline. Returns (anomaly_type, message) or None.

    Counts mean: ``created`` = new cases ingested, ``skipped`` = cases found
    but already in OLDP (409 dedup). In the rolling-window-with-dedup model a
    healthy provider in steady state re-sees its window each run, so
    ``created`` is naturally 0 most days while ``skipped`` > 0; only genuinely
    new decisions bump ``created``. Anomalies are judged against ``found`` =
    ``created + skipped`` (did it see anything at all?) so quiet, low-volume
    providers are not mistaken for broken ones.
    """
    runs = get_provider_history(history, provider, command)
    if not runs:
        return None

    latest = runs[-1]
    created = latest.get("created", 0)
    skipped = latest.get("skipped", 0)
    found = created + skipped
    status = latest.get("status", "unknown")
    exit_code = latest.get("exit_code", 0)

    # Crash
    if exit_code == 2:
        return ("CRASH", f"{command}/{provider}: crashed (exit code 2)")

    baseline_runs = runs[-(BASELINE_POINTS + 1):-1]  # exclude latest
    if len(baseline_runs) < 3:
        return None  # not enough data to tell "sparse" from "broken"

    # ZERO — the provider found nothing this run. Only an anomaly if it
    # normally finds documents; a genuinely sparse provider (small court,
    # empty rolling window) is expected to be quiet, so suppress it. Returns
    # here either way so a sparse quiet day never falls through to LOW.
    if found == 0 and status == "ok":
        baseline_found = sum(
            r.get("created", 0) + r.get("skipped", 0) for r in baseline_runs
        ) / len(baseline_runs)
        if baseline_found >= ZERO_MIN_BASELINE:
            return (
                "ZERO",
                f"{command}/{provider}: found nothing (created=0, skipped=0) "
                f"but baseline avg found {baseline_found:.1f}",
            )
        return None

    avg = sum(r.get("created", 0) for r in baseline_runs) / len(baseline_runs)
    if avg == 0:
        return None  # can't compute ratio

    # LOW — fewer *documents* than usual. Only when skipped==0: a run that
    # creates 0 but skips many just re-saw its window (nothing new), which is
    # healthy, not a drop.
    if skipped == 0 and created < avg * LOW_THRESHOLD:
        return ("LOW", f"{command}/{provider}: created={created} < baseline avg {avg:.0f} * {LOW_THRESHOLD} = {avg * LOW_THRESHOLD:.0f}")

    if created > avg * HIGH_THRESHOLD:
        return ("HIGH", f"{command}/{provider}: created={created} > baseline avg {avg:.0f} * {HIGH_THRESHOLD} = {avg * HIGH_THRESHOLD:.0f}")

    return None


def check_staleness(provider, command, history):
    """Check if provider hasn't run recently enough."""
    runs = get_provider_history(history, provider, command)
    if not runs:
        return ("NEVER_RUN", f"{command}/{provider}: never run")

    latest = runs[-1]
    finished_at = latest.get("finished_at", "")
    try:
        last_time = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return ("PARSE_ERROR", f"{command}/{provider}: cannot parse finished_at '{finished_at}'")

    now = datetime.now(timezone.utc)
    hours_ago = (now - last_time).total_seconds() / 3600

    threshold = (
        STALE_HOURS_PLAYWRIGHT
        if is_playwright(provider, command)
        else STALE_HOURS_HTTP
    )

    if hours_ago > threshold:
        return ("STALE", f"{command}/{provider}: last run {hours_ago:.0f}h ago (threshold: {threshold}h)")

    return None


def check_single(provider, command):
    history = load_history()
    anomaly = check_anomaly(provider, command, history)
    if anomaly:
        print(f"ANOMALY [{anomaly[0]}]: {anomaly[1]}")
        return 1
    print(f"OK: {command}/{provider}")
    return 0


def check_all():
    if not capabilities():
        print(
            "ERROR: could not load provider capabilities from "
            "'oldp-ingestor providers' — cannot determine what to monitor",
            file=sys.stderr,
        )
        sys.exit(2)

    history = load_history()
    issues = []

    for command, provider in monitored_providers():
        anomaly = check_anomaly(provider, command, history)
        if anomaly:
            issues.append(anomaly)

        stale = check_staleness(provider, command, history)
        if stale:
            issues.append(stale)

    if issues:
        print(f"ISSUES FOUND: {len(issues)}")
        for issue_type, message in issues:
            print(f"  [{issue_type}] {message}")
        return issues
    else:
        print("ALL OK: no anomalies or stale providers")
        return []


def main():
    parser = argparse.ArgumentParser(description="OLDP ingestor anomaly detection")
    parser.add_argument("provider", nargs="?", help="Provider name")
    parser.add_argument("command", nargs="?", default="cases", help="Command (default: cases)")
    parser.add_argument("--check-all", action="store_true", help="Check all providers for staleness and anomalies")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    if args.check_all:
        issues = check_all()
        if args.json:
            print(json.dumps([{"type": t, "message": m} for t, m in issues], indent=2))
        sys.exit(1 if issues else 0)
    elif args.provider:
        sys.exit(check_single(args.provider, args.command))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
