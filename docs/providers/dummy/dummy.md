# Dummy Providers (Test/Development)

## Overview

The dummy providers load legal data from Django fixture JSON files. They are
used for testing and development when no external API or portal access is
needed.

Both providers share the same CLI name `dummy` and are selected via the
`laws` or `cases` subcommand.

## Architecture

- `DummyLawProvider` — implements `LawProvider`
- `DummyCaseProvider` — implements `CaseProvider`

Neither provider inherits from `HttpBaseClient` — no HTTP requests are made.

## DummyLawProvider

Loads law books and laws from a Django fixture JSON file.

### Fixture Format

The JSON file contains Django fixture entries with model types:

- `laws.lawbook` — law book entries (code, title, revision_date, etc.)
- `laws.law` — individual law entries linked to law books by `pk`

### Methods

- `get_law_books()` — returns all `laws.lawbook` entries
- `get_laws(code, revision_date)` — returns `laws.law` entries matching the
  given book code and revision date (resolved via pk lookup)

## DummyCaseProvider

Loads cases from a Django fixture JSON file.

### Fixture Format

The JSON file contains Django fixture entries with model types:

- `courts.court` — court entries (used to build a pk -> court name lookup)
- `cases.case` — case entries with a court foreign key

### Methods

- `get_cases()` — returns all `cases.case` entries with court pk resolved to
  `court_name` string

## Usage

```bash
# Ingest laws from fixture
oldp-ingestor laws --provider dummy --path /path/to/fixture.json

# Ingest cases from fixture
oldp-ingestor cases --provider dummy --path /path/to/fixture.json

# Limit results
oldp-ingestor cases --provider dummy --path /path/to/fixture.json --limit 10
```

## Known Quirks

- **Fixture path required**: The `--path` argument is mandatory. There is no
  default fixture file.
- **Django fixture format**: The JSON must follow Django's fixture format with
  `model`, `pk`, and `fields` keys per entry.
- **Court resolution**: Case entries reference courts by foreign key (`pk`).
  The provider builds a lookup table from `courts.court` entries to resolve
  these to `court_name` strings.
