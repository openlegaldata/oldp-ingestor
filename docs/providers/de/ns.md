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

## Date Filtering

Date filtering is server-side via GET query parameters:

- `date` — start date (YYYY-MM-DD)
- `end_date_range` — end date (YYYY-MM-DD)

Results are also sorted by `sort_order=date_asc`. The server returns only
results matching the requested date range.

## Usage

```bash
oldp-ingestor -v cases --provider ns --limit 10
```

## Known Quirks

- **Cloudflare 403 on detail URLs**: `voris.wolterskluwer-online.de`
  fronts every `/browse/document/<uuid>` with Cloudflare bot management.
  Specific UUIDs are persistently 403-blocked from individual egress
  IPs regardless of User-Agent or header fingerprint (verified
  2026-06-04 with a full Chrome-fingerprint probe — still 403 while
  `/search` returns 200). HTTP 4xx responses are now fed into the
  failure tracker so a stuck UUID drops out of the retry budget instead
  of producing repeat warnings every run. HTTP 5xx and network errors
  stay transient.
- **Pagination cap**: the portal caps pagination at `page=200` and
  returns 404 (not an empty result page) past that boundary. The
  provider treats a 4xx on the search endpoint as end-of-pagination
  and logs at INFO instead of WARNING.
