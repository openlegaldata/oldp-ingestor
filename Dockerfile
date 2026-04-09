FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/

RUN uv build --wheel

# ---

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libxml2 libxslt1.1 netcat-openbsd curl procps \
    && rm -rf /var/lib/apt/lists/*

# supercronic: cron daemon that logs to stdout (ideal for Docker)
ARG TARGETARCH=amd64
RUN curl -fsSL "https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-${TARGETARCH}" \
      -o /usr/local/bin/supercronic \
    && chmod +x /usr/local/bin/supercronic

COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN playwright install --with-deps chromium

RUN useradd --create-home ingestor
USER ingestor

ENTRYPOINT ["oldp-ingestor"]
