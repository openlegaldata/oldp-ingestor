FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/

RUN uv build --wheel

# ---

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libxml2 libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

RUN playwright install --with-deps chromium

RUN useradd --create-home ingestor
USER ingestor

ENTRYPOINT ["oldp-ingestor"]
