# NS — Niedersachsen (Lower Saxony) Case Provider

## Portal

- **URL**: https://voris.wolterskluwer-online.de
- **Name**: Niedersaechsisches Vorschrifteninformationssystem (NI-VORIS)
- **Technology**: Wolters Kluwer platform, Drupal 11 (server-rendered HTML)
- **Authentication**: None required for public case law

## Architecture

`ScraperBaseClient` -> `NsCaseProvider` (HTTP-based, no Playwright needed)

## Search

GET `/search` with query parameters:

| Parameter | Value |
|---|---|
| `query` | `*` (all results) |
| `publicationtype` | `publicationform-ats-filter!ATS_Rechtsprechung` |
| `page` | 0-indexed page number |

Returns 12 results per page with document links in `/browse/document/<UUID>` format.

## Field Mappings

Metadata is in `<section class="wkde-bibliography"><dl>` with `<dt>/<dd>` pairs:

| Portal Field (`<dt>`) | OLDP Field | Notes |
|---|---|---|
| `Gericht` | `court_name` | e.g. "OVG Niedersachsen" |
| `Datum` | `date` | DD.MM.YYYY -> YYYY-MM-DD via `parse_german_date()` |
| `Aktenzeichen` | `file_number` | e.g. "1 LA 2/26" |
| `Entscheidungsform` | `type` | e.g. "Beschluss", "Urteil" |
| `ECLI` | `ecli` | Skipped if `[keine Angabe]` (wrapped in `<span class="wkde-empty">`) |

Content is extracted from `<section class="wkde-document-body">`.

## Pagination

- 0-indexed (`page=0`, `page=1`, ...)
- 12 results per page (fixed by portal)
- Stops after 2 consecutive empty pages (same pattern as NRW)

## Usage

```bash
oldp-ingestor -v cases --provider ns --limit 10
```
