# GII — Gesetze im Internet

[Gesetze im Internet](https://www.gesetze-im-internet.de) is the official
free portal published by the Federal Ministry of Justice (BMJ) and operated
by juris GmbH. It provides the consolidated text of essentially all German
federal laws and ordinances (~6 800 entries).

## Portal

- **URL**: <https://www.gesetze-im-internet.de>
- **Name**: Gesetze im Internet (BMJ / juris)
- **Coverage**: all federal laws (`Bundesrecht`); no state law (`Landesrecht`)
- **Authentication**: none required
- **robots.txt**: wide open, no `Crawl-delay`

## Architecture

`HttpBaseClient` -> `GiiLawProvider` (a `LawProvider`).

Two-stage fetch:

1. **TOC**: a single `gii-toc.xml` file lists every law with `<title>` and
   `<link>` (the URL slug, e.g. `bgb`).
2. **Per-law archive**: each law has a single `xml.zip` next to its HTML
   landing page. The zip contains one `*.xml` file conforming to the
   `gii-norm` DTD (one root `<dokumente>` with N `<norm>` children — the
   first is the book header, the rest are sections).

There is no per-entry timestamp in the TOC. Change detection therefore
relies on HTTP-level metadata.

## Change detection

Each zip is fetched with conditional `GET` using
`If-Modified-Since: <stored Last-Modified>`. The server returns:

- **`304 Not Modified`** — skip without parsing or upload.
- **`200 OK`** — re-download, parse, and decide whether the parsed
  `revision_date` overtakes what OLDP currently has. If it does, the book
  + sections are uploaded as a new revision; OLDP's `LawBookCreator` flips
  the previous revision's `latest=False` in the same transaction.

The HTTP `Last-Modified` value matches the inner XML's
`<dokumente builddate>` to within a few seconds, so it's a faithful
proxy for the actual revision date.

`--full` skips `If-Modified-Since` and refetches every zip
unconditionally; useful for forced re-syncs but rarely needed.

## Cache directory

The provider requires `--cache-dir <path>`. Layout:

```
<cache_dir>/
├── url_slug_to_jurabk.json   # {"bgb": "BGB", "sgb_5": "SGB 5", ...}
├── done.json                 # per-slug last_modified + last oldp revision
└── zips/<url_slug>.zip       # last fetched zip, used to re-parse for laws
```

The cache is what makes runs **resumable** — a crash mid-run leaves the
state files consistent (each save is atomic via `os.replace`), and on the
next run any zip the server reports unchanged is read straight from disk
to re-emit its sections without a second download.

State is persisted every 100 TOC entries during the sweep, plus on
generator close (normal exit, `--limit`, or interrupt).

## Field mappings (gii-norm XML → OLDP)

### LawBook (header `<norm>`)

| XML field | OLDP field | Notes |
|---|---|---|
| `<jurabk>` | `code` | E.g. `BGB`, `SGB 5` (preserves whitespace) |
| `<langue>` | `title` | Long title; truncated to 250 chars by CLI |
| Latest `<standangabe standtyp="Stand">` date | `revision_date` | Falls back to root `<dokumente builddate>` |

### Law (each non-header `<norm>`)

| XML field | OLDP field | Notes |
|---|---|---|
| `<enbez>` | `section` | E.g. `§ 5`, `Art 1`, `Inhaltsübersicht` |
| `<titel>` | `title` | Truncated to 200 chars |
| `<text format="XML"><Content>` | `content` | Serialised as HTML |
| `<fussnoten><Content>` | `footnotes` | JSON-stringified |
| `doknr=` (norm attribute) | `doknr` | Source document number |
| `<amtabk>` | `amtabk` | Optional |
| `<kurzue>` | `kurzue` | Optional |

Norms whose `<Content>` is empty after stripping (e.g. table-of-contents
norms with only an `<enbez>`) are skipped — OLDP rejects blank-content
laws.

Image and binary attachments inside the zip are **out of scope** in v1;
text content only.

## Usage

```bash
# Initial cold-run ingest (≈6 800 laws; resumable via the cache directory)
oldp-ingestor laws --provider gii --cache-dir /var/cache/oldp-gii

# Subsequent incremental runs reuse the cache; only changed zips are
# downloaded and uploaded as new revisions.
oldp-ingestor laws --provider gii --cache-dir /var/cache/oldp-gii

# Forced full re-sync (ignores If-Modified-Since)
oldp-ingestor laws --provider gii --cache-dir /var/cache/oldp-gii --full

# Override the default TOC URL (rarely needed)
oldp-ingestor laws --provider gii --cache-dir /var/cache/oldp-gii \
    --toc-url https://example.test/gii-toc.xml

# Cap entries for testing
oldp-ingestor laws --provider gii --cache-dir /var/cache/oldp-gii --limit 5
```

## Request volume estimates

Each **incremental run** costs:

- 1 TOC fetch (~500 KB)
- ~6 800 conditional `GET`s (304 if unchanged, payload only if modified)
- N additional zip downloads + book + section uploads, where N is the
  number of laws actually changed since the last run (typically 0–30/day)

| Scenario | Total items | Est. requests | Time at 0.05 s delay |
|---|---|---|---|
| Cold full ingest | ~6 800 books | ~13 600 requests + ~95 000 OLDP POSTs | ~1–2 h |
| Daily incremental | ~6 800 HEADs | ~6 800 (mostly 304) | ~6 min |
| Forced re-sync (`--full`) | ~6 800 books | ~13 600 requests | ~1 h |

## Continuous ingestion

The `--cache-dir` makes the provider **idempotent** and **incremental**:
re-running the same command picks up only the laws the BMJ has rebuilt
since the last run. The recommended pattern is therefore to schedule a
plain re-run on a cadence with cron, systemd-timer, or a similar
scheduler. No state file or `--date-from` is needed; the cache directory
is the state.

### Cron + flock wrapper

A minimal wrapper that prevents overlapping runs (a cold pass takes ~1–2
hours; daily incrementals are minutes — but the lock keeps that safe):

```bash
#!/usr/bin/env bash
# /opt/oldp-ingestor/ingest-gii.sh
set -euo pipefail
cd /opt/oldp-ingestor

# Provide OLDP_API_URL / OLDP_API_TOKEN via environment or .env
source ./.env

exec flock -n /var/lock/oldp-gii.lock \
    .venv/bin/oldp-ingestor laws --provider gii \
        --cache-dir /var/cache/oldp-gii \
        --write-delay 0.05
```

Schedule daily at 04:00:

```cron
0 4 * * *  /opt/oldp-ingestor/ingest-gii.sh >> /var/log/oldp/ingest-gii.log 2>&1
```

`flock -n` exits silently if a previous run is still in progress, so
overlapping cron firings are safe.

### systemd timer

Equivalent setup as a systemd service + timer:

```ini
# /etc/systemd/system/oldp-ingest-gii.service
[Unit]
Description=Ingest German federal laws from gesetze-im-internet.de
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/oldp-ingestor
EnvironmentFile=/opt/oldp-ingestor/.env
ExecStart=/opt/oldp-ingestor/.venv/bin/oldp-ingestor laws --provider gii \
    --cache-dir /var/cache/oldp-gii --write-delay 0.05
```

```ini
# /etc/systemd/system/oldp-ingest-gii.timer
[Unit]
Description=Daily GII law ingest

[Timer]
OnCalendar=*-*-* 04:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
systemctl enable --now oldp-ingest-gii.timer
```

systemd serialises invocations of a `oneshot` service automatically, so
no external lock is required.

### Recovery / manual interventions

- A run that crashes mid-upload leaves `done.json` and the zip cache
  consistent — the next run reconciles by cross-checking each cached zip
  against OLDP's `?latest=true` snapshot and re-uploads any book OLDP is
  missing without re-downloading.
- To force a full refresh once (e.g. after a corpus-wide schema change
  upstream), pass `--full` for a single run.
- To wipe local state without losing OLDP data, delete the cache
  directory; the next run will rebuild it via fresh GETs (no 304s, ~1 h).
