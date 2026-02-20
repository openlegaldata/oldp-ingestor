# Contributing to oldp-ingestor

Thank you for your interest in contributing to **oldp-ingestor**! This document
explains how to set up a development environment, run tests, and submit changes.

## Prerequisites

- Python 3.12 or later
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Git

For providers that use browser automation (the `juris-*` family), you also need
[Playwright](https://playwright.dev/python/) browsers installed:

```bash
playwright install chromium
```

## Development setup

```bash
# Clone the repository
git clone https://github.com/openlegaldata/oldp-ingestor.git
cd oldp-ingestor

# Install the project and dev dependencies into a virtual environment
make install

# Copy the environment template and fill in your values
cp .env.example .env
```

The `.env` file is gitignored and never committed. You need a running OLDP
instance to use the `api` sink; for local development the `json-file` sink works
without one:

```bash
oldp-ingestor --sink json-file --output-dir /tmp/export \
    laws --provider ris --limit 1
```

## Running tests

```bash
make test           # run the full test suite
make test-cov       # run with coverage report (target: >= 90%)
make test-real      # run integration tests that hit real APIs (skipped by default)
```

All unit tests use `monkeypatch` to mock HTTP calls — no network access is
needed. Tests marked with `@pytest.mark.real` are skipped unless you pass
`--run-real`.

## Linting and formatting

The project uses [Ruff](https://docs.astral.sh/ruff/) for both linting and
formatting:

```bash
make lint           # check for lint errors and formatting issues
make format         # auto-fix lint errors and reformat
```

Please run `make lint` before submitting a pull request. CI will reject
unformatted code.

## Project structure

```
src/oldp_ingestor/
    cli.py              # CLI entry point (argparse)
    client.py           # OLDPClient — authenticated HTTP client
    providers/          # data source implementations (one file per source)
        base.py         # Provider, LawProvider, CaseProvider (abstract bases)
    sinks/              # output targets (API, JSON files)
tests/                  # pytest test suite
docs/                   # additional documentation
```

See [docs/architecture.md](docs/architecture.md) for the full class hierarchy
and data flow.

## Adding a new provider

1. **Create a provider module** in `src/oldp_ingestor/providers/`. Subclass
   `LawProvider` or `CaseProvider` from `base.py` and implement the required
   methods (`get_law_books()`/`get_laws()` or `get_cases()`).

2. **Set `SOURCE`** on your provider class — a dict with `name` and `homepage`
   keys. The CLI injects this into every record for attribution.

3. **Register the provider** in `cli.py`:
   - Add the provider name to the `--provider` choices list.
   - Add a branch in `_make_law_provider()` or `_make_case_provider()` to
     instantiate it.

4. **Write tests** in `tests/test_providers.py`. Mock all HTTP calls with
   `monkeypatch` — tests must not make real network requests.

5. **Update documentation**:
   - `README.md` — add a row to the data sources table.
   - `docs/architecture.md` — add the class to the provider hierarchy.
   - `CLAUDE.md` — update the provider class hierarchy and project layout.

### HTTP client base classes

Pick the right base class for your provider's data source:

| Base class | When to use |
|---|---|
| `RISBaseClient` | RIS API endpoints (already configured with base URL) |
| `ScraperBaseClient` | HTML/XML scraping with `requests` (includes tag stripping, date parsing helpers) |
| `PlaywrightBaseClient` | Sites that require JavaScript rendering (browser automation) |

All three inherit from `HttpBaseClient` which provides session management,
request pacing (`--request-delay`), and retry with exponential backoff.

## Submitting a pull request

1. **Fork** the repository and create a feature branch from `main`:
   ```bash
   git checkout -b my-feature
   ```

2. **Make your changes.** Keep commits focused — one logical change per commit.

3. **Run the checks** before pushing:
   ```bash
   make lint
   make test
   ```

4. **Push** your branch and open a pull request against `main`.

5. In the PR description, explain **what** changed and **why**. If you added a
   new provider, include a short example of how to use it.

## Reporting issues

Open an issue at <https://github.com/openlegaldata/oldp-ingestor/issues>. When
reporting a bug, include:

- The command you ran (redact tokens/credentials).
- The full error output.
- Python version (`python --version`) and OS.

## Code style

- Follow the existing patterns in the codebase.
- Ruff enforces formatting — don't fight it, just run `make format`.
- Keep functions short and focused. Prefer clarity over cleverness.
- Add docstrings to public classes and methods.
- Use type hints for function signatures.

## License

By contributing you agree that your contributions will be licensed under the
project's [MIT License](LICENSE).
