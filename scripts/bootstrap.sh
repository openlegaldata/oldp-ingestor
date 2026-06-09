#!/bin/sh
# Container bootstrap: ensure the Hugging Face CLI is available before the
# cron daemon starts, then hand off to supercronic.
#
# The oldp-ingestor image does not ship `hf`, and we don't want to rebuild it
# just for log backups. Instead we install huggingface_hub into an isolated
# venv inside the persisted cache volume (/app/cache, bind-mounted to
# docker/data/ingestor-cache). Isolation keeps hf's deps from shadowing the
# ingestor's own Python packages. The venv survives restarts, so the install
# only happens once (or after the cache is cleared).
set -e

HF_VENV=/app/cache/hf-venv

if [ ! -x "$HF_VENV/bin/hf" ]; then
    echo "[bootstrap] installing huggingface_hub[hf_xet] into ${HF_VENV} ..."
    if python3 -m venv "$HF_VENV" \
        && "$HF_VENV/bin/pip" install -q --no-cache-dir "huggingface_hub[hf_xet]"; then
        echo "[bootstrap] hf $("$HF_VENV/bin/hf" version 2>/dev/null) ready"
    else
        echo "[bootstrap] WARN: hf install failed; log sync will be skipped"
    fi
fi

exec supercronic -no-reap "$@"
