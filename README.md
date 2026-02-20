# oldp-ingestor

Ingesting legal data like laws and court decisions via OLDP API.

Data sources:

| CLI provider | Type | Source |
|---|---|---|
| `ris` | laws + cases | Rechtsinformationssystem des Bundes (RIS) |
| `rii` | cases | Rechtsprechung im Internet (RII) — federal courts |
| `by` | cases | Gesetze Bayern — Bavarian courts |
| `nrw` | cases | NRWE Rechtsprechungsdatenbank — NRW courts |
| `ns` | cases | NI-VORIS Niedersachsen |
| `eu` | cases | EUR-Lex — EU court decisions |
| `juris-bb` | cases | Landesrecht Berlin-Brandenburg |
| `juris-bw` | cases | Landesrecht Baden-Württemberg |
| `juris-he` | cases | Landesrecht Hessen |
| `juris-hh` | cases | Landesrecht Hamburg |
| `juris-mv` | cases | Landesrecht Mecklenburg-Vorpommern |
| `juris-rlp` | cases | Landesrecht Rheinland-Pfalz |
| `juris-sa` | cases | Landesrecht Sachsen-Anhalt |
| `juris-sh` | cases | Landesrecht Schleswig-Holstein |
| `juris-sl` | cases | Landesrecht Saarland |
| `juris-th` | cases | Landesrecht Thüringen |
| `dummy` | laws + cases | Django fixture JSON files (for testing) |

## Installation

```bash
pip install oldp-ingestor
```

Some providers require Playwright browsers. Install them after pip:

```bash
playwright install chromium
```

For development, clone the repo and use Make (auto-detects `uv` or falls back
to `pip`):

```bash
git clone https://github.com/openlegaldata/oldp-ingestor.git
cd oldp-ingestor
make install
```

## Configuration

Set the following environment variables (or add them to a `.env` file):

| Variable | Description |
|---|---|
| `OLDP_API_URL` | Base URL of the OLDP instance (e.g. `http://localhost:8000`) |
| `OLDP_API_TOKEN` | API authentication token |
| `OLDP_API_HTTP_AUTH` | Optional HTTP basic auth in `user:password` format |

## Usage

### Show API info

```bash
oldp-ingestor info
```

### Ingest laws

#### From the RIS API (rechtsinformationen.bund.de)

```bash
# Ingest all available legislation
oldp-ingestor laws --provider ris

# Search for specific legislation
oldp-ingestor laws --provider ris --search-term "EinbTestV"

# Limit the number of law books to ingest
oldp-ingestor laws --provider ris --limit 5

# Combine search and limit
oldp-ingestor laws --provider ris --search-term "BGB" --limit 1
```

#### Incremental fetching and request pacing

```bash
# Only fetch legislation adopted since a given date
oldp-ingestor laws --provider ris --date-from 2025-12-01

# Fetch legislation within a date range
oldp-ingestor laws --provider ris --date-from 2025-01-01 --date-to 2025-06-30

# Override the default request delay (0.2s) for slower pacing
oldp-ingestor laws --provider ris --request-delay 0.5
```

For automated cron usage, see `dev-deployment/ingest-ris.sh` (laws) and
`dev-deployment/ingest-ris-cases.sh` (cases) which track the last successful
run date in a state file and pass it as `--date-from` on subsequent runs.

#### From a JSON fixture file (dummy provider)

```bash
oldp-ingestor laws --provider dummy --path /path/to/fixture.json
```

### Ingest cases

#### From the RIS API (rechtsinformationen.bund.de)

```bash
# Ingest all cases from all federal courts
oldp-ingestor cases --provider ris

# Filter by court and date range
oldp-ingestor cases --provider ris --court BGH --date-from 2026-01-01

# Limit for testing
oldp-ingestor cases --provider ris --limit 10 -v
```

#### From a JSON fixture file (dummy provider)

```bash
oldp-ingestor cases --provider dummy --path /path/to/fixture.json

# Limit the number of cases to ingest
oldp-ingestor cases --provider dummy --path /path/to/fixture.json --limit 10
```

The fixture file should contain Django fixture entries with `courts.court` and
`cases.case` models. Court foreign keys are resolved to `court_name` strings
for the OLDP cases API.

### Output sinks

By default, data is written to the OLDP REST API. Use `--sink json-file` to
write JSON files to disk instead:

```bash
# Export laws to local files
oldp-ingestor --sink json-file --output-dir /tmp/export \
    laws --provider ris --search-term BGB --limit 1

# Export cases to local files
oldp-ingestor --sink json-file --output-dir /tmp/export \
    cases --provider ris --court BGH --limit 5
```

See [docs/sinks.md](docs/sinks.md) for details on directory structure and
implementing custom sinks.

## Architecture

The ingestor uses a provider-based architecture. Each data source implements a
provider class (`LawProvider` or `CaseProvider`), and shared RIS HTTP logic
(retry, pacing, User-Agent) lives in `RISBaseClient`. Output is routed through
a **sink** (`ApiSink` or `JSONFileSink`).

```
Provider
├── LawProvider   →  DummyLawProvider, RISProvider
└── CaseProvider  →  DummyCaseProvider, RISCaseProvider,
                     RiiCaseProvider, ByCaseProvider,
                     NrwCaseProvider, NsCaseProvider,
                     EuCaseProvider, JurisCaseProvider (10 state variants)

Sink
├── ApiSink        →  OLDP REST API (default)
└── JSONFileSink   →  local JSON files
```

See [docs/architecture.md](docs/architecture.md) for the full design.

## Politeness and rate limiting

The RIS API allows 600 req/min. The ingestor stays under this with:
- **Request pacing** — 0.2 s delay between requests (configurable)
- **Retry with backoff** — exponential backoff on 429/503, respects `Retry-After`
- **Descriptive User-Agent** — `oldp-ingestor/0.1.0`

See [docs/politeness.md](docs/politeness.md) for details.

## Further documentation

- [docs/architecture.md](docs/architecture.md) — class hierarchy, data flow, file layout
- [docs/sinks.md](docs/sinks.md) — sink concept, CLI examples, custom sinks
- [docs/politeness.md](docs/politeness.md) — rate limiting, retry logic, cron operation

### Provider docs

| Provider | Doc |
|----------|-----|
| RIS (laws + cases) | [docs/providers/de/ris.md](docs/providers/de/ris.md) |
| RII (federal courts) | [docs/providers/de/rii.md](docs/providers/de/rii.md) |
| Bayern | [docs/providers/de/by.md](docs/providers/de/by.md) |
| NRW | [docs/providers/de/nrw.md](docs/providers/de/nrw.md) |
| Niedersachsen | [docs/providers/de/ns.md](docs/providers/de/ns.md) |
| EUR-Lex (EU) | [docs/providers/de/eu.md](docs/providers/de/eu.md) |
| Bremen | [docs/providers/de/hb.md](docs/providers/de/hb.md) |
| Sachsen OVG | [docs/providers/de/sn_ovg.md](docs/providers/de/sn_ovg.md) |
| Sachsen ESAMOSplus | [docs/providers/de/sn.md](docs/providers/de/sn.md) |
| Sachsen VerfGH | [docs/providers/de/sn_verfgh.md](docs/providers/de/sn_verfgh.md) |
| Juris (10 states) | [docs/providers/de/juris.md](docs/providers/de/juris.md) |
| Dummy (test/dev) | [docs/providers/dummy/dummy.md](docs/providers/dummy/dummy.md) |

## Development

```bash
# Run tests
make test

# Run tests with coverage
make test-cov

# Lint
make lint

# Auto-format
make format
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development setup, how to
add new providers, and pull request guidelines.
