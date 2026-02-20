# SN-OVG — Sächsisches Oberverwaltungsgericht Bautzen

## Portal

- **URL**: https://www.justiz.sachsen.de/ovgentschweb
- **Name**: Entscheidungsdatenbank des Sächsischen Oberverwaltungsgerichts
- **Technology**: PHP-based search, server-rendered HTML, PDF content
- **Authentication**: None required

## Architecture

`ScraperBaseClient` -> `SnOvgCaseProvider`

The provider POSTs a search form, parses document IDs from JavaScript popup
calls, fetches detail pages, and extracts case content from linked PDFs via
pymupdf.

## Search

POST to `/searchlist.phtml` with form data:

| Parameter | Value |
|---|---|
| `aktenzeichen` | `*` (all results) |
| `zeitraum` | Date range (optional) |

Date format uses German notation: `DD.MM.YYYY` for single dates, or
`DD.MM.YYYY-DD.MM.YYYY` for ranges (e.g. `1.1.2025-31.12.2025`).

All matching results are returned on a single page (no server-side pagination).

## Document IDs

Document IDs are extracted from JavaScript `popupDocument('ID')` calls via regex
in the search result page.

## Detail Page

```
GET /document.phtml?id={doc_id}
```

### Field Mappings

| Source | OLDP Field | Notes |
|---|---|---|
| Header div (`td.schattiert.gross`) | `court_name` | First line of header |
| Header div | `file_number` | Third line (after case type) |
| Header right-aligned TD | `date` | DD.MM.YYYY -> YYYY-MM-DD |
| Header div, second line | `type` | Case type (Urteil, Beschluss, etc.) |
| Table cell "Leitsatz:" | `abstract` | Optional |
| PDF link | `content` | Extracted via pymupdf, wrapped as `<p>` per page |

PDF links are found via XPath: `//a[contains(@href, "documents/")]` ending
with `.pdf`.

## Date Conversion

ISO dates are converted to German format for the search form by stripping
leading zeros: `2025-01-01` -> `1.1.2025`.

## Usage

```bash
# Fetch 10 Sachsen OVG cases
oldp-ingestor -v cases --provider sn-ovg --limit 10

# Filter by date
oldp-ingestor -v cases --provider sn-ovg --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases sn-ovg
```

## Known Quirks

- **Single-page results**: The search returns all matching results on a single
  page. There is no server-side pagination.
- **Client-side date filtering**: Date filtering is applied post-fetch when
  the search form's date range parameter doesn't match the exact `--date-from`
  / `--date-to` format.
- **PDF-only content**: All case content is in PDF files. The provider uses
  pymupdf to extract text, wrapping each page in `<p>` tags.
- **Content threshold**: Cases with content shorter than 10 characters are skipped.
- **German date format**: The search form expects German-style dates without
  leading zeros (1.1.2025 not 01.01.2025).
