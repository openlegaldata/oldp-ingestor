# Architecture

## Overview

The oldp-ingestor is a CLI tool that fetches legal data (laws and court
decisions) from external sources and pushes them into an OLDP instance via its
REST API.  The design separates **providers** (data sources) from the
**CLI layer** (orchestration, error handling, API calls) so that adding a new
data source only requires implementing a provider class.

## Provider class hierarchy

```
Provider                          # common base (providers/base.py)
├── LawProvider                   # interface: get_law_books(), get_laws()
│   ├── DummyLawProvider           # loads from Django fixture JSON (providers/dummy/)
│   └── RISProvider               # fetches legislation from RIS API (providers/de/)
└── CaseProvider                  # interface: get_cases()
    ├── DummyCaseProvider          # loads from Django fixture JSON (providers/dummy/)
    ├── RISCaseProvider            # fetches federal case law from RIS API (providers/de/)
    ├── RiiCaseProvider            # federal courts via rechtsprechung-im-internet.de (providers/de/)
    ├── ByCaseProvider             # Bavarian courts via gesetze-bayern.de (providers/de/)
    ├── NrwCaseProvider            # NRW courts via NRWE database (providers/de/)
    ├── NsCaseProvider             # Lower Saxony via NI-VORIS (providers/de/)
    ├── EuCaseProvider             # EU courts via EUR-Lex (providers/de/)
    └── JurisCaseProvider          # juris-hosted state portals (10 variants, providers/de/):
        ├── BbCaseProvider         #   Berlin-Brandenburg
        ├── BwCaseProvider         #   Baden-Württemberg
        ├── HeCaseProvider         #   Hessen
        ├── HhCaseProvider         #   Hamburg
        ├── MvCaseProvider         #   Mecklenburg-Vorpommern
        ├── RlpCaseProvider        #   Rheinland-Pfalz
        ├── SaCaseProvider         #   Sachsen-Anhalt
        ├── ShCaseProvider         #   Schleswig-Holstein
        ├── SlCaseProvider         #   Saarland
        └── ThCaseProvider         #   Thüringen
```

`RISProvider` and `RISCaseProvider` both inherit from `RISBaseClient` (via
multiple inheritance) for shared HTTP infrastructure:

```
HttpBaseClient                    # shared HTTP: session, retry, pacing
├── RISBaseClient                 # RIS API base URL pre-configured
│   ├── RISProvider(RISBaseClient, LawProvider)
│   └── RISCaseProvider(RISBaseClient, CaseProvider)
├── ScraperBaseClient             # HTML scraping helpers (tag stripping, date parsing)
│   ├── RiiCaseProvider           # XML-based federal courts
│   ├── ByCaseProvider            # XML-based Bavarian courts
│   ├── NrwCaseProvider           # HTML scraping for NRW
│   ├── NsCaseProvider            # HTML scraping for Lower Saxony
│   └── EuCaseProvider            # SOAP/XML for EUR-Lex
└── PlaywrightBaseClient          # browser-based scraping (requires Playwright)
    └── JurisCaseProvider         # juris-hosted state law portals (10 variants)
```

### Provider base class

`Provider` is intentionally minimal — it serves as a shared type anchor so that
all providers share a common root.  It can be extended later with shared
behaviour such as a `name` property or logging setup.

### RISBaseClient

Encapsulates all HTTP communication with the RIS API:

- **Session management** — a `requests.Session` with a descriptive
  `User-Agent` header (`oldp-ingestor/0.1.0`).
- **Request pacing** — configurable delay between requests (default 0.2 s,
  i.e. max ~300 req/min, well under the 600 req/min rate limit).
- **Retry with backoff** — automatic retry on 429 and 503 responses or
  connection errors, with exponential backoff (1 s, 2 s, 4 s, 8 s, 16 s).
  Respects the `Retry-After` header when present.
- **Convenience methods** — `_get_json(path, **params)` and `_get_text(path)`
  for the two main response types.

## CLI layer

The CLI (`cli.py`) is built with `argparse` and provides three subcommands:

| Command | Description |
|---------|-------------|
| `info`  | Print API endpoint info from the OLDP instance |
| `laws`  | Ingest laws (provider: `dummy` or `ris`) |
| `cases` | Ingest cases (provider: `dummy`, `ris`, `rii`, `by`, `nrw`, `ns`, `eu`, `juris-*`) |
| `status` | Show status dashboard for all providers |
| `analyze-courts` | Analyse missing courts from ingestor logs |

The CLI handles:

- **Provider construction** — `_make_law_provider()` / `_make_case_provider()`
  instantiate the correct provider class based on `--provider`.
- **Field sanitisation** — truncation to API max lengths, empty title fallback,
  removal of `None` values.
- **Duplicate handling** — 409 Conflict from OLDP is logged and skipped.
- **Error resilience** — other HTTP errors are logged but do not abort the run.

## Sink class hierarchy

```
Sink                              # abstract base (sinks/base.py)
├── ApiSink                       # wraps OLDPClient, delegates to .post()
└── JSONFileSink                  # writes JSON files to directory tree
```

Sinks are the write-side counterpart of providers.  The CLI selects a sink via
`--sink` (`api` by default, `json-file` for local export).

## Data flow

```
External API ──► Provider.get_*() ──► list[dict] ──► CLI ──► Sink.write_*()
                                                              │
                                                    ┌─────────┴──────────┐
                                                    ▼                    ▼
                                              ApiSink              JSONFileSink
                                           (OLDP REST API)        (local files)
```

1. The provider fetches data from the external source (RIS API or fixture file)
   and returns a list of plain dicts.
2. The CLI iterates over the dicts, applies field sanitisation, and writes each
   one through the selected sink.
3. `ApiSink` delegates to `OLDPClient.post()` (authentication via token +
   optional HTTP basic auth).  `JSONFileSink` writes pretty-printed JSON files
   to a directory tree.

## Cron / incremental operation

Both RIS providers support `--date-from` and `--date-to` for incremental
fetching.  Wrapper scripts in `dev-deployment/` track the last successful run
date in a state file:

| Script | Provider | State file |
|--------|----------|------------|
| `ingest-ris.sh` | `laws --provider ris` | `.ris-ingest-last-run` |
| `ingest-ris-cases.sh` | `cases --provider ris` | `.ris-ingest-cases-last-run` |

On success the script writes today's date to the state file.  The next run
passes it as `--date-from` so only new items are fetched.

## File layout

```
src/oldp_ingestor/
├── __init__.py
├── settings.py               # env-based config (OLDP_API_URL, etc.)
├── client.py                 # OLDPClient — HTTP client for OLDP API
├── cli.py                    # argparse CLI: info, laws, cases, status
├── results.py                # result file writing and status dashboard
├── court_analysis.py         # court gap analysis from logs
├── providers/
│   ├── __init__.py
│   ├── base.py               # Provider, LawProvider, CaseProvider
│   ├── http_client.py        # HttpBaseClient — session, retry, pacing
│   ├── scraper_common.py     # ScraperBaseClient — HTML scraping helpers
│   ├── playwright_client.py  # PlaywrightBaseClient — browser automation
│   ├── de/                   # German providers
│   │   ├── __init__.py
│   │   ├── ris_common.py     # RISBaseClient, extract_body, constants
│   │   ├── ris.py            # RISProvider (federal legislation)
│   │   ├── ris_cases.py      # RISCaseProvider (federal case law)
│   │   ├── rii.py            # RiiCaseProvider (rechtsprechung-im-internet.de)
│   │   ├── by.py             # ByCaseProvider (Bayern)
│   │   ├── nrw.py            # NrwCaseProvider (NRW)
│   │   ├── ns.py             # NsCaseProvider (Niedersachsen)
│   │   ├── eu.py             # EuCaseProvider (EUR-Lex)
│   │   ├── hb.py             # BremenCaseProvider (Bremen)
│   │   ├── sn_ovg.py         # SnOvgCaseProvider (Sachsen OVG)
│   │   ├── sn.py             # SnCaseProvider (Sachsen ESAMOSplus)
│   │   ├── sn_verfgh.py      # SnVerfghCaseProvider (Sachsen VerfGH)
│   │   └── juris.py          # JurisCaseProvider + 10 state variants
│   └── dummy/                # Dummy providers (test/dev)
│       ├── __init__.py
│       ├── dummy_laws.py     # DummyLawProvider (laws from fixture)
│       └── dummy_cases.py    # DummyCaseProvider (cases from fixture)
└── sinks/
    ├── __init__.py
    ├── base.py               # Sink ABC
    ├── api.py                # ApiSink (OLDP REST API)
    └── json_file.py          # JSONFileSink (local files)
```

## Provider documentation

Each provider has a dedicated doc file under `docs/providers/`:

| Provider | Doc |
|----------|-----|
| RIS (laws + cases) | [providers/de/ris.md](providers/de/ris.md) |
| RII (federal courts) | [providers/de/rii.md](providers/de/rii.md) |
| Bayern | [providers/de/by.md](providers/de/by.md) |
| NRW | [providers/de/nrw.md](providers/de/nrw.md) |
| Niedersachsen | [providers/de/ns.md](providers/de/ns.md) |
| EUR-Lex (EU) | [providers/de/eu.md](providers/de/eu.md) |
| Bremen | [providers/de/hb.md](providers/de/hb.md) |
| Sachsen OVG | [providers/de/sn_ovg.md](providers/de/sn_ovg.md) |
| Sachsen ESAMOSplus | [providers/de/sn.md](providers/de/sn.md) |
| Sachsen VerfGH | [providers/de/sn_verfgh.md](providers/de/sn_verfgh.md) |
| Juris (10 states) | [providers/de/juris.md](providers/de/juris.md) |
| Dummy (test/dev) | [providers/dummy/dummy.md](providers/dummy/dummy.md) |
