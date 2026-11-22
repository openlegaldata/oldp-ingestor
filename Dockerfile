FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv

WORKDIR /build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/

# Runtime dependencies come from uv.lock, the same versions CI tests, and not
# from a fresh resolve at build time. A fresh resolve once pulled an untested
# Playwright and pymupdf 2.x into a release. --locked fails the build if
# uv.lock is out of date with pyproject.toml. To update dependencies, run
# `uv lock --upgrade` and open a PR.
RUN uv export --locked --no-dev --no-emit-project --format requirements-txt \
      -o requirements.txt \
    && uv build --wheel

# ---

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libxml2 libxslt1.1 netcat-openbsd curl procps libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# supercronic: cron daemon that logs to stdout (ideal for Docker)
ARG TARGETARCH=amd64
RUN curl -fsSL "https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-${TARGETARCH}" \
      -o /usr/local/bin/supercronic \
    && chmod +x /usr/local/bin/supercronic

COPY --from=builder /build/requirements.txt /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt \
    && pip install --no-cache-dir --no-deps /tmp/*.whl \
    && pip check \
    && rm /tmp/*.whl /tmp/requirements.txt

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN playwright install --with-deps chromium

# Cron orchestration scripts (bootstrap, ingest wrapper, health/anomaly/report).
# Baked into the image at /app/ so any host running this image gets them — the
# deployment only bind-mounts the crontab (schedule) and the data dirs over
# these. Provider capabilities the scripts need are read at runtime from
# `oldp-ingestor providers`, so no provider rosters are duplicated here.
COPY scripts/ /app/
RUN chmod +x /app/*.sh /app/*.py

RUN useradd --create-home ingestor
USER ingestor

ENTRYPOINT ["oldp-ingestor"]
