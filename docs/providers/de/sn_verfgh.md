# SN-VerfGH — Verfassungsgerichtshof des Freistaates Sachsen

## Portal

- **URL**: https://www.justiz.sachsen.de/esaver
- **Name**: Entscheidungssammlung des Verfassungsgerichtshofes des Freistaates Sachsen
- **Technology**: AJAX POST returning HTML fragment, PDF content
- **Authentication**: None required

## Architecture

`ScraperBaseClient` -> `SnVerfghCaseProvider`

The provider POSTs a search request that returns an HTML fragment via AJAX,
parses case metadata from `<h4>` headings using multiple regex patterns, and
extracts case content from linked PDFs via pymupdf.

## Search

POST to `/answers.php` with form data:

| Parameter | Value |
|---|---|
| `funkt` | `search` |
| Date range | ISO format dates (optional) |

All results are returned in a single response (no pagination). Results are in
`//table[@id='tEntschList']//tr` rows.

## Metadata Parsing

Case metadata is extracted from `<h4>` elements within each table row. Multiple
legacy heading formats are supported:

```
SächsVerfGH, TYPE vom DD. MONTH YYYY - FILE
TYPE des SächsVerfGH vom DD. MONTH YYYY - FILE
TYPE vom DD. MONTH YYYY - FILE
```

German month names (Januar, Februar, März, April, Mai, Juni, Juli, August,
September, Oktober, November, Dezember) are converted to numeric months.

## Field Mappings

| Source | OLDP Field | Notes |
|---|---|---|
| Hardcoded | `court_name` | "Verfassungsgerichtshof des Freistaates Sachsen" |
| `<h4>` regex | `date` | DD. MONTH YYYY -> YYYY-MM-DD |
| `<h4>` regex | `file_number` | Court file number |
| `<h4>` regex | `type` | Case type (e.g. Urteil, Beschluss) |
| `//p[@id^="lst_"]` | `abstract` | Leitsatz paragraph, or other `<p>` without links/bold |
| PDF link | `content` | Extracted via pymupdf, wrapped as `<p>` per page |

PDF links are found via XPath: `//a[contains(@href, ".pdf")]`.

## Usage

```bash
# Fetch 10 Sachsen VerfGH cases
oldp-ingestor -v cases --provider sn-verfgh --limit 10

# Filter by date
oldp-ingestor -v cases --provider sn-verfgh --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases sn-verfgh
```

## Known Quirks

- **No pagination**: All results are returned in a single AJAX response.
- **Multiple H4 formats**: The portal uses at least 3 different heading formats
  across its history. The provider tries all regex patterns in sequence.
- **German month names**: Dates use German month names that must be mapped to
  numeric values (e.g. "März" -> "03").
- **PDF-only content**: All case content is in PDF files. The provider uses
  pymupdf to extract text.
- **Content threshold**: Cases with content shorter than 10 characters are skipped.
- **Limit applied early**: The `--limit` flag is applied to the entry list
  before fetching detail/PDF content, saving unnecessary HTTP requests.
