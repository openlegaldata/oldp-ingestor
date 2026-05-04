# Politeness and rate limiting

The RIS API enforces a rate limit of **600 requests per minute** per client IP.
The ingestor implements several mechanisms to stay well within this limit and
behave as a good API citizen.

## Request pacing

Every HTTP request is preceded by a configurable delay (default **0.2 seconds**,
i.e. max ~300 req/min — 50 % of the rate limit).  This can be overridden via
`--request-delay`:

```bash
# Slower pacing for shared environments
oldp-ingestor cases --provider ris --request-delay 0.5
```

## User-Agent

The session sends a descriptive `User-Agent` header so the API operator can
identify the client:

```
oldp-ingestor/0.1.0 (+https://github.com/openlegaldata)
```

## Retry with backoff

When the API returns `429 Too Many Requests` or `503 Service Unavailable`, or
when a `ConnectionError` occurs, the ingestor retries up to **5 times** with
exponential backoff:

| Attempt | Default delay |
|---------|---------------|
| 1       | 1 s           |
| 2       | 2 s           |
| 3       | 4 s           |
| 4       | 8 s           |
| 5       | 16 s          |

If the response includes a `Retry-After` header, its value is used instead of
the exponential backoff (with a minimum of 1 second).

## Error handling

- **Rate-limited responses** (429, 503) are retried automatically.
- **Connection errors** are retried with the same backoff.
- **Other HTTP errors** (4xx, 5xx) are raised immediately — the provider or CLI
  handles them (skip and log).
- **Failed HTML fetches** for individual cases cause the case to be skipped, not
  the entire run.
- **Failed detail fetches** cause the case to be ingested without an abstract.

## Per-document retry tracking

Some upstreams hand us individual documents that are permanently corrupt
(truncated XML, missing PDFs, deleted-but-still-listed entries). Without state,
every cron run re-discovers the same broken doc, fails to parse it, logs a
warning, and tries again next run forever — masking "gone" as recurring noise
and wasting upstream requests.

The `FailureTracker` (`providers/failure_tracker.py`) persists a per-provider
JSON file mapping a stable `doc_id` (e.g. BY's document ID, EU's CELEX,
NRW's case URL) to attempt count + last error. After `max_retries` consecutive
failures, the tracker's `should_skip(doc_id)` returns `True` and the provider
short-circuits before re-fetching. A successful parse of the same doc in any
later run clears the entry (so a transient upstream regression auto-heals).

### Enable

```bash
oldp-ingestor --state-dir /var/lib/oldp/state cases --provider by
# or env vars:
export OLDP_STATE_DIR=/var/lib/oldp/state
export OLDP_MAX_DOC_RETRIES=5  # default
```

When `--state-dir` is unset (the default), the tracker is a no-op — providers
behave exactly as before.

### What is and isn't counted

Network errors, HTTP 5xx, and known-transient signals (EUR-Lex `202 Accepted`,
WAF challenge pages) are **not** counted toward the retry budget. Only structural
failures — XML/HTML parse errors, missing required fields, content shorter than
the minimum threshold — increment the counter, because those are the failure
modes that won't fix themselves on the next run.

### State files

```
<state-dir>/
  failures_by.json
  failures_rii.json
  failures_eu.json
  ...
```

Each file is a flat JSON object keyed by `doc_id`. Wiping a file resets all
retry counters for that provider.

### Wired-in providers

`by`, `rii`, `nrw`, `ns`, `eu`, `sn-ovg`, `sn-verfgh`. Other providers inherit
the no-op default and can be wired in with the same three-call pattern:
`should_skip()` before fetch, `record_failure()` on permanent parse error,
`record_success()` on successful parse.

## Cron operation

For production use, wrap the ingestor in a cron-friendly script that tracks
the last successful run date:

```bash
# See dev-deployment/ingest-ris-cases.sh
0 3 * * * /path/to/dev-deployment/ingest-ris-cases.sh >> /var/log/ingest-cases.log 2>&1
```

This ensures only **new decisions** are fetched on each run (typically a handful
per day), keeping request volume minimal.
