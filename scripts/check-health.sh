#!/usr/bin/env bash
# Daily health check: runs anomaly detection on all providers,
# sends an alert email if any issues are found.
#
# Intended to run via cron at 08:00 UTC daily.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== OLDP Ingestor Health Check $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

# Check SSH proxy
PROXY="${INGESTOR_PROXY:-}"
if [[ -n "$PROXY" ]]; then
    PROXY_HOST=$(echo "$PROXY" | sed -E 's|.*://([^:]+):.*|\1|')
    PROXY_PORT=$(echo "$PROXY" | sed -E 's|.*:([0-9]+)$|\1|')
    if ! nc -z "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null; then
        BODY="CRITICAL: SSH proxy ${PROXY_HOST}:${PROXY_PORT} is unreachable.

No ingestion jobs can run until the proxy is restored."
        python3 /app/send-alert.py \
            --subject "[OLDP Ingestor] CRITICAL: SSH proxy down" \
            --body "$BODY"
        echo "CRITICAL: SSH proxy down"
        exit 2
    fi
    echo "SSH proxy: OK"
fi

# Check OLDP API (dev-app)
API_URL="${OLDP_API_URL:-}"
if [[ -n "$API_URL" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 10 --max-time 15 "${API_URL}/api/" 2>/dev/null || echo "000")
    if [[ "$HTTP_CODE" == "000" ]]; then
        BODY="CRITICAL: OLDP API at ${API_URL} is unreachable (connection failed).

Ingestor jobs will crash with ConnectTimeout until the app is restored.

Check: docker compose ps dev-app"
        python3 /app/send-alert.py \
            --subject "[OLDP Ingestor] CRITICAL: OLDP API unreachable" \
            --body "$BODY"
        echo "CRITICAL: OLDP API unreachable (${API_URL})"
    elif [[ "$HTTP_CODE" -ge 500 ]]; then
        echo "WARNING: OLDP API returned HTTP ${HTTP_CODE}"
    else
        echo "OLDP API: OK (HTTP ${HTTP_CODE})"
    fi
fi

# Run anomaly detection on all providers
OUTPUT=$(python3 /app/anomaly-detect.py --check-all 2>&1)
EXIT_CODE=$?

echo "$OUTPUT"

if [[ $EXIT_CODE -ne 0 ]]; then
    SUBJECT="[OLDP Ingestor] ALERT: issues detected ($(date -u +%Y-%m-%d))"
    BODY="OLDP Ingestor Health Report — $(date -u +%Y-%m-%d)

${OUTPUT}

---
Check logs: docker compose logs ingestor-cron
Dashboard: docker compose exec ingestor-cron oldp-ingestor status --results-dir /app/results"

    python3 /app/send-alert.py --subject "$SUBJECT" --body "$BODY"
fi

echo "=== Health check complete ==="
