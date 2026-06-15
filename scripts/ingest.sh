#!/usr/bin/env bash
# Container-adapted ingestor wrapper for cron jobs.
#
# Usage: ingest.sh <command> <provider> [extra_args...]
#   e.g.: ingest.sh cases rii
#         ingest.sh cases juris-bb --limit 100
#         ingest.sh laws ris

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <command> <provider> [extra_args...]"
    exit 1
fi

COMMAND="$1"
PROVIDER="$2"
shift 2

STATE_DIR="/app/state"
RESULTS_DIR="/app/results"
LOG_DIR="/app/logs"
HISTORY_FILE="${STATE_DIR}/ingestion-history.jsonl"
STATE_FILE="${STATE_DIR}/.ingest-state-${COMMAND}-${PROVIDER}"

REQUEST_DELAY="${REQUEST_DELAY:-0.2}"
PROXY="${INGESTOR_PROXY:-}"
TIMEOUT_HTTP="${TIMEOUT_HTTP:-4h}"
TIMEOUT_PLAYWRIGHT="${TIMEOUT_PLAYWRIGHT:-4h}"
INGEST_LIMIT="${INGEST_LIMIT:-1000}"

# --- Provider capabilities — derived, never hardcoded ---
# Browser kind (http/playwright) and incremental --date-from support are
# intrinsic provider facts owned by the ingestor package. `oldp-ingestor
# providers` introspects the provider classes and emits them as JSON; we read
# the two we need for this (command, provider). Adding a provider therefore
# never requires editing this wrapper. Safe fallback if the CLI is
# unavailable: treat as http with no date window (full fetch).
CAPS_LINE="$(oldp-ingestor providers --command "$COMMAND" 2>/dev/null \
    | COMMAND="$COMMAND" PROVIDER="$PROVIDER" python3 -c '
import json, os, sys
try:
    caps = json.load(sys.stdin)
except Exception:
    caps = {}
entry = caps.get(os.environ["COMMAND"], {}).get(os.environ["PROVIDER"], {})
print(entry.get("kind", "http"), "true" if entry.get("date_from") else "false")
' 2>/dev/null || true)"
PROVIDER_KIND="${CAPS_LINE%% *}"
PROVIDER_DATE_FROM="${CAPS_LINE##* }"
[[ "$PROVIDER_KIND" == "playwright" ]] || PROVIDER_KIND="http"
[[ "$PROVIDER_DATE_FROM" == "true" ]] || PROVIDER_DATE_FROM="false"
echo "Capabilities (${COMMAND}/${PROVIDER}): kind=${PROVIDER_KIND} date_from=${PROVIDER_DATE_FROM}"

# --- Pre-flight: check SSH proxy ---
if [[ -n "$PROXY" ]]; then
    PROXY_HOST=$(echo "$PROXY" | sed -E 's|.*://([^:]+):.*|\1|')
    PROXY_PORT=$(echo "$PROXY" | sed -E 's|.*:([0-9]+)$|\1|')
    if ! nc -z "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null; then
        echo "ERROR: Proxy ${PROXY_HOST}:${PROXY_PORT} unreachable, skipping ${COMMAND} ${PROVIDER}"
        exit 2
    fi
fi

# --- Pre-flight: check OLDP API ---
API_URL="${OLDP_API_URL:-}"
if [[ -n "$API_URL" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 10 --max-time 15 "${API_URL}/api/" 2>/dev/null || echo "000")
    if [[ "$HTTP_CODE" == "000" ]]; then
        echo "ERROR: OLDP API ${API_URL} unreachable, skipping ${COMMAND} ${PROVIDER}"
        exit 2
    fi
fi

# --- Determine timeout ---
TIMEOUT="$TIMEOUT_HTTP"
if [[ "$PROVIDER_KIND" == "playwright" ]]; then
    TIMEOUT="$TIMEOUT_PLAYWRIGHT"
fi

# --- Build CLI args ---
# Top-level flags must come before the subcommand
TOP_ARGS=(-v --results-dir "$RESULTS_DIR")

if [[ -n "$PROXY" ]]; then
    TOP_ARGS+=(--proxy "$PROXY")
fi

ARGS=("${TOP_ARGS[@]}" "$COMMAND" --provider "$PROVIDER" --request-delay "$REQUEST_DELAY" --limit "$INGEST_LIMIT")

# Add --date-from for supported providers.
# Always use at least a 7-day rolling window to catch late-published cases
# (many courts publish days or weeks after the decision date). Dedup relies
# on the API returning 409 for existing cases, counted as "skipped".
if [[ "$PROVIDER_DATE_FROM" == "true" ]]; then
    WINDOW_DAYS="${INGEST_WINDOW_DAYS:-7}"
    ROLLING_FROM=$(date -u -d "${WINDOW_DAYS} days ago" +%Y-%m-%d)
    if [[ -f "$STATE_FILE" ]]; then
        STATE_DATE="$(cat "$STATE_FILE")"
        # Use the earlier of state (backfill) or rolling window
        if [[ "$STATE_DATE" < "$ROLLING_FROM" ]]; then
            DATE_FROM="$STATE_DATE"
            echo "Backfill: ${COMMAND} ${PROVIDER} from ${DATE_FROM}"
        else
            DATE_FROM="$ROLLING_FROM"
            echo "Rolling ${WINDOW_DAYS}-day window: ${COMMAND} ${PROVIDER} from ${DATE_FROM}"
        fi
    else
        DATE_FROM="$ROLLING_FROM"
        echo "First run (${WINDOW_DAYS}-day window): ${COMMAND} ${PROVIDER} from ${DATE_FROM}"
    fi
    ARGS+=(--date-from "$DATE_FROM")
else
    echo "Full fetch: ${COMMAND} ${PROVIDER} (no date filtering)"
fi

# Append any extra args
ARGS+=("$@")

# --- Rotate log if it exceeds the size cap ---
# tee -a appends forever, so rotate before each run to keep the active log
# capped at LOG_MAX_BYTES (default 10 MB). Rotated logs get an immutable
# timestamped name (cases-ns.log.<UTC>) so they never get renamed — safe to
# sync to an object store (S3) where objects shouldn't be rewritten. The
# newest LOG_RETAIN backups are kept; older ones are pruned.
LOG_FILE="${LOG_DIR}/${COMMAND}-${PROVIDER}.log"
LOG_MAX_BYTES="${LOG_MAX_BYTES:-10485760}"  # 10 MB
LOG_RETAIN="${LOG_RETAIN:-10}"
if [[ -f "$LOG_FILE" ]]; then
    LOG_SIZE=$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)
    if [[ "$LOG_SIZE" -ge "$LOG_MAX_BYTES" ]]; then
        ROTATED="${LOG_FILE}.$(date -u +%Y%m%dT%H%M%SZ)"
        mv -f "$LOG_FILE" "$ROTATED"
        echo "Rotated ${LOG_FILE} -> ${ROTATED} (${LOG_SIZE} bytes >= ${LOG_MAX_BYTES})"
        # Prune: keep the newest LOG_RETAIN timestamped backups, drop the rest.
        # Timestamps sort lexicographically = chronologically (oldest first).
        ls -1 "${LOG_FILE}".*Z 2>/dev/null | sort | head -n "-${LOG_RETAIN}" | xargs -r rm -f
    fi
fi

# --- Run ---
START_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "[${START_TS}] Starting: oldp-ingestor ${ARGS[*]}"

EXIT_CODE=0
timeout "$TIMEOUT" oldp-ingestor "${ARGS[@]}" 2>&1 | tee -a "$LOG_FILE" || EXIT_CODE=$?

END_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- Post-run ---

# Parse created/skipped/errors from result file
RESULT_FILE="${RESULTS_DIR}/${COMMAND}_${PROVIDER}.json"
CREATED=0; SKIPPED=0; ERRORS=0; STATUS="unknown"
if [[ -f "$RESULT_FILE" ]]; then
    CREATED=$(python3 -c "import json; d=json.load(open('${RESULT_FILE}')); print(d.get('created',0))" 2>/dev/null || echo 0)
    SKIPPED=$(python3 -c "import json; d=json.load(open('${RESULT_FILE}')); print(d.get('skipped',0))" 2>/dev/null || echo 0)
    ERRORS=$(python3 -c "import json; d=json.load(open('${RESULT_FILE}')); print(d.get('errors',0))" 2>/dev/null || echo 0)
    STATUS=$(python3 -c "import json; d=json.load(open('${RESULT_FILE}')); print(d.get('status','unknown'))" 2>/dev/null || echo unknown)
fi

# Append to history
echo "{\"provider\":\"${PROVIDER}\",\"command\":\"${COMMAND}\",\"finished_at\":\"${END_TS}\",\"created\":${CREATED},\"skipped\":${SKIPPED},\"errors\":${ERRORS},\"status\":\"${STATUS}\",\"exit_code\":${EXIT_CODE}}" >> "$HISTORY_FILE"

# Update state file on success OR partial success.
# Skip if the limit was hit — more cases remain in the same date range,
# so the next run should re-fetch from the same --date-from.
TOTAL_PROCESSED=$((CREATED + SKIPPED))
if [[ $EXIT_CODE -eq 0 || $EXIT_CODE -eq 1 ]]; then
    if [[ "$PROVIDER_DATE_FROM" == "true" ]]; then
        if [[ "$TOTAL_PROCESSED" -ge "$INGEST_LIMIT" ]]; then
            echo "Limit reached (${TOTAL_PROCESSED} >= ${INGEST_LIMIT}), NOT advancing state — more to fetch"
        else
            date +%Y-%m-%d > "$STATE_FILE"
            echo "State updated: $(cat "$STATE_FILE") (exit ${EXIT_CODE})"
        fi
    fi
fi

# Kill leftover Chromium processes after Playwright providers
if [[ "$PROVIDER_KIND" == "playwright" ]]; then
    pkill -f chromium 2>/dev/null || true
fi

# Run anomaly detection
if [[ -f /app/anomaly-detect.py ]]; then
    python3 /app/anomaly-detect.py "$PROVIDER" "$COMMAND" || {
        ANOMALY_MSG=$(python3 /app/anomaly-detect.py "$PROVIDER" "$COMMAND" 2>&1 || true)
        python3 /app/send-alert.py \
            --subject "[OLDP Ingestor] ANOMALY: ${COMMAND}/${PROVIDER}" \
            --body "$ANOMALY_MSG" 2>/dev/null || true
    }
fi

# Alert on crash
if [[ $EXIT_CODE -eq 2 ]]; then
    CRASH_MSG="CRASH: ${COMMAND}/${PROVIDER} exited with code 2 at ${END_TS}

Last 20 lines of log:
$(tail -20 "${LOG_DIR}/${COMMAND}-${PROVIDER}.log" 2>/dev/null || echo '(no log)')"
    python3 /app/send-alert.py \
        --subject "[OLDP Ingestor] CRASH: ${COMMAND}/${PROVIDER}" \
        --body "$CRASH_MSG" 2>/dev/null || true
fi

echo "[${END_TS}] Finished: ${COMMAND} ${PROVIDER} (exit ${EXIT_CODE}, created=${CREATED}, skipped=${SKIPPED}, errors=${ERRORS})"

# --- Back up logs/results/state to the HF object store ---
# Incremental sync of the logs (active + rotated timestamped backups),
# results, and state dirs after each run. hf lives in the isolated venv set
# up by bootstrap.sh; HF_TOKEN comes from the env_file. Non-fatal — a sync
# failure must never fail the ingestion run. --delete is off, so the bucket
# retains files even after local rotation prunes them.
HF_BIN="${HF_BIN:-/app/cache/hf-venv/bin/hf}"
if [[ -n "${HF_BUCKET_BASE:-}" && -x "$HF_BIN" ]]; then
    HF_BASE="${HF_BUCKET_BASE%/}"
    hf_sync() {
        local src="$1" sub="$2"
        if "$HF_BIN" buckets sync "$src" "${HF_BASE}/${sub}/" >/dev/null 2>&1; then
            echo "Synced ${sub} -> ${HF_BASE}/${sub}/"
        else
            echo "WARN: HF sync failed for ${sub}"
        fi
    }
    hf_sync "$LOG_DIR"     ingestor-logs
    hf_sync "$RESULTS_DIR" ingestor-results
    hf_sync "$STATE_DIR"   ingestor-state
fi

exit $EXIT_CODE
