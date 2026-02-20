# Sinks

## Overview

Sinks are the write-side counterpart of **providers**.  A provider fetches data
from an external source; a sink writes that data to a target.  The CLI routes
data through a sink selected via the `--sink` flag.

```
Provider.get_*() ──► list[dict] ──► CLI (sanitise) ──► Sink.write_*()
```

## Available sinks

### ApiSink (default)

Writes each entity to the OLDP REST API via `OLDPClient.post()`.  This is the
existing behaviour and the default when `--sink` is omitted.

```bash
# Explicit (same as default):
oldp-ingestor --sink api laws --provider dummy --path fixtures.json

# Implicit (--sink api is the default):
oldp-ingestor laws --provider dummy --path fixtures.json
```

### JSONFileSink

Writes each entity as a pretty-printed JSON file in a directory tree.  Requires
`--output-dir`.

```bash
oldp-ingestor --sink json-file --output-dir /tmp/export \
    laws --provider ris --search-term BGB --limit 1

oldp-ingestor --sink json-file --output-dir /tmp/export \
    cases --provider ris --court BGH --limit 5
```

#### Directory structure

```
output_dir/
├── law_books/{code}.json
├── laws/{book_code}/{slug}.json
└── cases/{file_number_sanitized}.json
```

Filenames are sanitised: characters like `/\<>:"|?*` and whitespace are replaced
with underscores; consecutive underscores are collapsed.  Files are always
overwritten if they already exist.

## CLI reference

| Flag | Description |
|------|-------------|
| `--sink {api,json-file}` | Output target (default: `api`) |
| `--output-dir DIR` | Directory for `json-file` sink (required when `--sink json-file`) |

Both flags are top-level (before the subcommand):

```bash
oldp-ingestor --sink json-file --output-dir ./out laws --provider ris
```

## Implementing a custom sink

Create a subclass of `Sink` and implement the three abstract methods:

```python
from oldp_ingestor.sinks.base import Sink

class MySink(Sink):
    def write_law_book(self, book: dict) -> None:
        ...

    def write_law(self, law: dict) -> None:
        ...

    def write_case(self, case: dict) -> None:
        ...
```

Then wire it into `_make_sink()` in `cli.py` or use it directly in scripts.

## Notes

- The JSONFileSink output format is not directly compatible with the Django
  fixture JSON format used by `DummyLawProvider`.  A future `JSONFileProvider`
  could bridge this for round-tripping.
- All writes from `JSONFileSink` count as "created" in the CLI summary (there
  is no 409-equivalent for file writes).
