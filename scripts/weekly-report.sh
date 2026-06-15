#!/usr/bin/env bash
# Weekly summary report: sends an overview of all provider runs from the past week.
#
# Intended to run via cron on Sunday 09:00 UTC.

set -euo pipefail

HISTORY_FILE="/app/state/ingestion-history.jsonl"

echo "=== OLDP Ingestor Weekly Report $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

if [[ ! -f "$HISTORY_FILE" ]]; then
    echo "No history file found, skipping report."
    exit 0
fi

# Generate report via Python (inline for simplicity)
REPORT=$(python3 -c "
import json
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)
week_ago = now - timedelta(days=7)

runs = []
with open('$HISTORY_FILE') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            finished = entry.get('finished_at', '')
            dt = datetime.fromisoformat(finished.replace('Z', '+00:00'))
            if dt >= week_ago:
                runs.append(entry)
        except (json.JSONDecodeError, ValueError):
            continue

if not runs:
    print('No runs in the past 7 days.')
else:
    total_created = sum(r.get('created', 0) for r in runs)
    total_skipped = sum(r.get('skipped', 0) for r in runs)
    total_errors = sum(r.get('errors', 0) for r in runs)
    crashes = [r for r in runs if r.get('exit_code', 0) == 2]
    failures = [r for r in runs if r.get('exit_code', 0) != 0]

    print(f'Runs: {len(runs)}')
    print(f'Created: {total_created}')
    print(f'Skipped: {total_skipped}')
    print(f'Errors: {total_errors}')
    print(f'Crashes: {len(crashes)}')
    print(f'Failures: {len(failures)}')
    print()
    print('Per-provider breakdown:')
    print(f'{\"Provider\":<25} {\"Runs\":>5} {\"Created\":>8} {\"Skipped\":>8} {\"Errors\":>7} {\"Status\":<10}')
    print('-' * 70)

    by_provider = {}
    for r in runs:
        key = f\"{r.get('command','?')}/{r.get('provider','?')}\"
        if key not in by_provider:
            by_provider[key] = []
        by_provider[key].append(r)

    for key in sorted(by_provider):
        prov_runs = by_provider[key]
        c = sum(r.get('created', 0) for r in prov_runs)
        s = sum(r.get('skipped', 0) for r in prov_runs)
        e = sum(r.get('errors', 0) for r in prov_runs)
        any_crash = any(r.get('exit_code', 0) == 2 for r in prov_runs)
        status = 'CRASH' if any_crash else 'OK'
        print(f'{key:<25} {len(prov_runs):>5} {c:>8} {s:>8} {e:>7} {status:<10}')
")

echo "$REPORT"

SUBJECT="[OLDP Ingestor] Weekly Report ($(date -u +%Y-%m-%d))"
BODY="OLDP Ingestor Weekly Summary — $(date -u +%Y-%m-%d)

${REPORT}

---
Full history: /app/state/ingestion-history.jsonl
Results: /app/results/"

python3 /app/send-alert.py --subject "$SUBJECT" --body "$BODY"

echo "=== Weekly report sent ==="
