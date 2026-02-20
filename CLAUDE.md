# oldp-ingestor

CLI tool that fetches German legal data (laws and court decisions) from external
sources and pushes them into an OLDP instance via its REST API.

## Quick reference

```bash
make install          # install into venv
make test             # run tests (100 tests, target coverage >= 90%)
make test-cov         # run with coverage report
make lint             # ruff check
make format           # ruff format
```

## Project layout

```
src/oldp_ingestor/
├── settings.py               # env config: OLDP_API_URL, OLDP_API_TOKEN, OLDP_API_HTTP_AUTH
├── client.py                 # OLDPClient — authenticated HTTP client for OLDP REST API
├── cli.py                    # argparse CLI: info, laws, cases, status subcommands
├── results.py                # result file writing and status dashboard
├── court_analysis.py         # court gap analysis from logs
├── providers/
│   ├── base.py               # Provider → LawProvider, CaseProvider (abstract bases)
│   ├── http_client.py        # HttpBaseClient — session, retry, pacing
│   ├── scraper_common.py     # ScraperBaseClient — HTML scraping helpers
│   ├── playwright_client.py  # PlaywrightBaseClient — browser automation
│   ├── de/                   # German providers
│   │   ├── ris_common.py     # RISBaseClient, extract_body, constants (shared HTTP)
│   │   ├── ris.py            # RISProvider(RISBaseClient, LawProvider) — legislation
│   │   ├── ris_cases.py      # RISCaseProvider(RISBaseClient, CaseProvider) — case law
│   │   ├── rii.py            # RiiCaseProvider — federal courts (rechtsprechung-im-internet.de)
│   │   ├── by.py             # ByCaseProvider — Bayern (gesetze-bayern.de)
│   │   ├── nrw.py            # NrwCaseProvider — NRW (NRWE database)
│   │   ├── ns.py             # NsCaseProvider — Niedersachsen (NI-VORIS)
│   │   ├── eu.py             # EuCaseProvider — EU courts (EUR-Lex)
│   │   ├── hb.py             # BremenCaseProvider — Bremen (5 SixCMS portals + PDF)
│   │   ├── sn_ovg.py         # SnOvgCaseProvider — Sachsen OVG Bautzen (PHP + PDF)
│   │   ├── sn.py             # SnCaseProvider — Sachsen ESAMOSplus (Playwright + PDF)
│   │   ├── sn_verfgh.py      # SnVerfghCaseProvider — Sachsen VerfGH (AJAX + PDF)
│   │   └── juris.py          # JurisCaseProvider + 10 state variants (juris-hosted portals)
│   └── dummy/                # Dummy providers (test/dev, not country-specific)
│       ├── dummy_laws.py     # DummyLawProvider (laws from Django fixture JSON)
│       └── dummy_cases.py    # DummyCaseProvider (cases from Django fixture JSON)
└── sinks/
    ├── base.py               # Sink ABC — write_law_book(), write_law(), write_case()
    ├── api.py                # ApiSink — wraps OLDPClient, delegates to .post()
    └── json_file.py          # JSONFileSink — writes JSON files to directory tree

tests/
├── test_providers.py          # provider unit tests (largest test file)
├── test_cli.py                # CLI integration tests
├── test_client.py             # OLDPClient tests
├── test_sinks.py              # sink unit tests

docs/
├── architecture.md            # class hierarchy, data flow, file layout
├── sinks.md                   # sink concept, CLI examples, custom sinks
├── politeness.md              # rate limiting, retry, User-Agent, cron
└── providers/
    ├── de/                    # German provider docs
    │   ├── ris.md             # RIS API endpoints, field mappings, request volumes
    │   ├── rii.md             # RII federal courts (rechtsprechung-im-internet.de)
    │   ├── by.md              # Bayern (gesetze-bayern.de)
    │   ├── nrw.md             # NRW (NRWE database)
    │   ├── ns.md              # Niedersachsen (NI-VORIS)
    │   ├── eu.md              # EUR-Lex (EU courts)
    │   ├── hb.md              # Bremen (5 SixCMS portals + PDF)
    │   ├── sn_ovg.md          # Sachsen OVG Bautzen (PHP + PDF)
    │   ├── sn.md              # Sachsen ESAMOSplus (Playwright + PDF)
    │   ├── sn_verfgh.md       # Sachsen VerfGH (AJAX + PDF)
    │   └── juris.md           # Juris state portals (10 variants, Playwright)
    └── dummy/
        └── dummy.md           # Dummy providers (test/dev fixture JSON)
```

## Provider class hierarchy

```
Provider                          (base.py — common root)
├── LawProvider                   get_law_books() -> list[dict], get_laws(code, date) -> list[dict]
│   ├── DummyLawProvider           loads from Django fixture JSON
│   └── RISProvider               fetches legislation from RIS API
└── CaseProvider                  get_cases() -> list[dict]
    ├── DummyCaseProvider          loads from Django fixture JSON
    ├── RISCaseProvider            fetches federal case law from RIS API
    ├── RiiCaseProvider            federal courts via rechtsprechung-im-internet.de
    ├── ByCaseProvider             Bavarian courts via gesetze-bayern.de
    ├── NrwCaseProvider            NRW courts via NRWE database
    ├── NsCaseProvider             Lower Saxony via NI-VORIS
    ├── EuCaseProvider             EU courts via EUR-Lex
    ├── BremenCaseProvider         Bremen (5 SixCMS court portals + PDF)
    ├── SnOvgCaseProvider          Sachsen OVG Bautzen (PHP site + PDF)
    ├── SnCaseProvider             Sachsen ESAMOSplus ordinary courts (Playwright + PDF)
    ├── SnVerfghCaseProvider       Sachsen VerfGH constitutional court (AJAX + PDF)
    └── JurisCaseProvider          juris-hosted state portals (BB, BW, HE, HH, MV, RLP, SA, SH, SL, TH)

HttpBaseClient                    (providers/http_client.py — session, retry, pacing)
├── RISBaseClient                 (providers/de/ris_common.py) RIS API base URL pre-configured
│   ├── RISProvider(RISBaseClient, LawProvider)
│   └── RISCaseProvider(RISBaseClient, CaseProvider)
├── ScraperBaseClient             (providers/scraper_common.py) HTML scraping helpers
│   ├── RiiCaseProvider, ByCaseProvider, NrwCaseProvider, NsCaseProvider, EuCaseProvider
│   ├── BremenCaseProvider, SnOvgCaseProvider, SnVerfghCaseProvider
└── PlaywrightBaseClient          (providers/playwright_client.py) browser-based scraping
    ├── JurisCaseProvider + 10 state variants
    └── SnCaseProvider
```

## Sink class hierarchy

```
Sink                              (sinks/base.py — ABC)
├── ApiSink                       wraps OLDPClient, delegates to .post()
└── JSONFileSink                  writes JSON files to directory tree
```

Sinks are the write-side counterpart of providers. The CLI uses `--sink` to
select the output target (`api` default, `json-file` for local export).

## RIS API

- **Base URL**: `https://testphase.rechtsinformationen.bund.de` (trial/test-phase)
- **Auth**: None required
- **Rate limit**: 600 req/min per IP. Ingestor uses 0.2s delay (~300 req/min)

### Key endpoints

| Endpoint | Returns |
|---|---|
| `GET /v1/legislation?size=300&pageIndex=0` | Hydra collection of legislation (paginated) |
| `GET /v1/legislation/eli/...` | Expression detail (articles + encoding URLs) |
| `GET /v1/case-law?size=300&pageIndex=0` | Hydra collection of case law (paginated) |
| `GET /v1/case-law/{documentNumber}` | Case detail (abstract fields) |
| `GET /v1/case-law/{documentNumber}.html` | Full HTML decision text |
| `GET /v1/case-law/courts` | **Plain JSON list** (NOT hydra) of `{id, label, count}` |

### Gotcha: courts endpoint

The `/v1/case-law/courts` endpoint returns a **plain JSON list**, not a hydra
collection. Each item has `id` (court code) and `label` (full name), not
`courtCode`/`courtLabel`. This differs from all other RIS endpoints.

### Pagination

- Uses `size` (1-300) and `pageIndex` (0-indexed)
- Check `view.next` to determine if more pages exist
- Do NOT use `len(members) < MAX_PAGE_SIZE` as a stop condition for cases —
  the API can return fewer items than page size even when more pages exist

### Query parameters

**Legislation**: `searchTerm`, `dateFrom`, `dateTo`, `sort`
**Case law**: `courtType`, `decisionDateFrom`, `decisionDateTo`

## Field mappings (RIS to OLDP)

### Laws

| RIS | OLDP | Notes |
|---|---|---|
| `abbreviation` | `code` | Law book code (e.g. BGB) |
| `name` | `title` | Full title |
| `legislationDate` | `revision_date` | Consolidation date |
| Article `name` | `section`, `title` | Parsed via `_parse_article_name()` |
| Article HTML body | `content` | `<body>` extracted |
| Article `eId` | `slug` | Slugified |

### Cases

| RIS | OLDP | Notes |
|---|---|---|
| `courtName` | `court_name` | Resolved to full label via `/v1/case-law/courts` |
| `fileNumbers[0]` | `file_number` | First element of array |
| `decisionDate` | `date` | ISO 8601 |
| HTML body | `content` | `<body>` extracted from `.html` endpoint |
| `documentType` | `type` | Optional |
| `ecli` | `ecli` | Optional |
| `headline` | `title` | Optional |
| `guidingPrinciple` / `headnote` / `otherHeadnote` / `tenor` | `abstract` | First non-empty wins, truncated to 50000 chars |

## Politeness and retry

- **User-Agent**: `oldp-ingestor/0.1.0 (+https://github.com/openlegaldata)`
- **Request pacing**: 0.2s between requests (configurable via `--request-delay`)
- **Retry**: 429 and 503 trigger exponential backoff (1s, 2s, 4s, 8s, 16s)
- **Retry-After**: header respected when present (minimum 1s)
- **ConnectionError**: retried with same backoff schedule
- **Other HTTP errors**: raised immediately (not retried)

## CLI error handling

- **409 Conflict** from OLDP: logged as "already exists", skipped
- **Other HTTP errors**: logged with detail, does not abort the run
- **Failed HTML fetch** for individual cases: case skipped
- **Failed detail fetch**: case ingested without abstract
- **Field sanitisation**: titles/sections truncated to API max lengths, `None` values removed

## Testing notes

- All tests use `monkeypatch` to mock HTTP calls — no network access in tests
- Test coverage is 98% (target was 90%)
- `-v` flag must come BEFORE the subcommand: `oldp-ingestor -v cases ...`
- Dummy providers load from Django fixture JSON format

## Cron operation

Both RIS providers support `--date-from` / `--date-to` for incremental fetching.
Wrapper scripts track the last successful run date in a state file:

| Script | Provider | State file |
|---|---|---|
| `dev-deployment/ingest-ris.sh` | `laws --provider ris` | `.ris-ingest-last-run` |
| `dev-deployment/ingest-ris-cases.sh` | `cases --provider ris` | `.ris-ingest-cases-last-run` |

## Production operations

### Result files and monitoring

The CLI writes JSON result files when `--results-dir` is set (or `OLDP_RESULTS_DIR` env):

```bash
oldp-ingestor --results-dir results cases --provider rii
# writes results/cases_rii.json
```

Result files contain: provider, command, timestamps, duration, status, created/skipped/errors counts.

Status values: `ok` (no errors), `partial` (some errors, run completed), `error` (crash).

Exit codes: 0=ok, 1=partial failure, 2=crash.

### Status dashboard

```bash
oldp-ingestor status --results-dir results          # table view
oldp-ingestor status --results-dir results --json   # JSON output
oldp-ingestor status --results-dir results --stale-hours 72
```

Shows all 15 providers with last run time, duration, status, counts, and staleness flag.
Exit code 0 if all healthy, 1 if any stale/failed/never-run.

### Deployment scripts (dev-deployment/)

| Script | Purpose |
|---|---|
| `ingest.sh <cmd> <provider> [args]` | Run one provider with state tracking + result writing |
| `ingest-all.sh [args]` | Run all 15 providers sequentially, report failures |
| `ingest-ris.sh` | Legacy: RIS laws only |
| `ingest-ris-cases.sh` | Legacy: RIS cases only |
| `crontab.example` | Suggested cron schedule (daily HTTP + weekly Playwright) |

### Makefile targets (dev-deployment/)

```bash
make status                        # status dashboard
make ingest CMD=cases PROV=rii     # run single provider
make ingest-all                    # run all providers
```

## Common pitfalls

1. **Courts endpoint format**: Returns plain list `[{id, label}]`, not hydra collection
2. **Pagination stop condition**: Only use `view.next` for case law; do not check member count
3. **`_retry_delay` is module-level**: Defined outside `RISBaseClient` class in `de/ris_common.py`
4. **`_extract_body` alias**: `de/ris.py` has `_extract_body = extract_body` for backward compat (tests import it)
5. **Abstract field priority**: `guidingPrinciple` > `headnote` > `otherHeadnote` > `tenor`
6. **Content threshold**: Cases with `<body>` content shorter than 10 chars are skipped

## Import rules

- Verify code changes by running "make lint", "make format", and "make test". If needed also do not hesitate to run other commands for verification including commands that send remote requests, talk to APIs or send emails.
- Avoid duplicated code and use shared functions and abstractions
- When doing a git push always verify if the CI passes

