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

## Cron operation

For production use, wrap the ingestor in a cron-friendly script that tracks
the last successful run date:

```bash
# See dev-deployment/ingest-ris-cases.sh
0 3 * * * /path/to/dev-deployment/ingest-ris-cases.sh >> /var/log/ingest-cases.log 2>&1
```

This ensures only **new decisions** are fetched on each run (typically a handful
per day), keeping request volume minimal.
