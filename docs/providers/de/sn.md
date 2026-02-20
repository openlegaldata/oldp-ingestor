# SN — Sachsen ESAMOSplus (Saxon Ordinary Courts)

## Portal

- **URL**: https://www.justiz.sachsen.de/esamosplus
- **Name**: ESAMOSplus — Entscheidungssammlung der sächsischen ordentlichen Gerichtsbarkeit
- **Technology**: ASP.NET WebForms with ViewState/PostBack, PDF content
- **Authentication**: None required

## Architecture

`PlaywrightBaseClient` -> `SnCaseProvider`

The provider uses Playwright browser automation to interact with the ASP.NET
WebForms application. Search involves selecting courts from dropdowns, filling
date fields, clicking buttons, and waiting for PostBack round-trips. Case
content is extracted from PDFs downloaded via Playwright's download interception.

## Courts

The provider covers 15 Saxon ordinary courts:

| Court | Dropdown Value |
|---|---|
| OLG Dresden | `DV1_C39` value mapping |
| 3 Landgerichte | LG Chemnitz, LG Dresden, LG Leipzig |
| 11 Amtsgerichte | Various AG courts across Sachsen |

Court names and their dropdown values are stored in a hardcoded `COURTS` dict.

## Search

1. Navigate to `/pages/suchen.aspx`
2. Select court filter: `select_option("#DV1_C39", value)`
3. Set date range: `fill("#DV1_C34", "DD.MM.YYYY")`, `fill("#DV1_C35", ...)`
4. Click search: `click("#DV1_C24")`
5. Wait for `networkidle`

Date format uses German notation: `DD.MM.YYYY`.

## Results Table

Results appear in `DV13_Table` (search results) or `DV16_Table` (latest
decisions). Table rows with 5+ cells contain:

| Cell | Content |
|---|---|
| Cell[1] (Col0) | Date (DD.MM.YYYY, validated via regex) |
| Cell[2] (Col1) | File number; `title` attribute contains abstract (strip "Leitsatz:" prefix) |
| Cell[3] (Col2) | Court name |
| Cell[4] (Col3) | Document button (`<input type="submit">`) |

Data elements are extracted from `<span>` or `<input type="submit">` elements
within each cell.

## Document Download

Content is retrieved by:
1. Clicking the document button (submit input)
2. Intercepting the PDF download via `expect_download()`
3. Extracting text from the PDF via pymupdf
4. Wrapping each page's text in `<p>` tags

## Field Mappings

| Source | OLDP Field | Notes |
|---|---|---|
| Cell[3] | `court_name` | From results table |
| Cell[1] | `date` | DD.MM.YYYY -> YYYY-MM-DD |
| Cell[2] | `file_number` | From results table |
| Cell[2] `title` attr | `abstract` | Optional, strip "Leitsatz:" prefix |
| PDF text | `content` | Extracted via pymupdf |

## Pagination

- "Vorwärts" (forward) button navigates to the next page
- Pagination stops when the forward button is disabled
- PostBack-based: each page turn triggers a server round-trip

## Usage

```bash
# Fetch 10 Saxon court cases
oldp-ingestor -v cases --provider sn --limit 10

# Filter by date
oldp-ingestor -v cases --provider sn --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases sn
```

## Known Quirks

- **Playwright required**: The ASP.NET WebForms application relies on
  ViewState and PostBack, making it impossible to scrape with plain HTTP.
- **PDF-only content**: All case content is in PDF files. No HTML full-text
  is available on the portal.
- **Two result tables**: Search results use `DV13_Table`, while "latest
  decisions" use `DV16_Table`. The provider checks both.
- **Content threshold**: Cases with content shorter than 10 characters are skipped.
- **Request delay**: Default 0.5s between page interactions (Playwright default).
- **Court iteration**: The provider iterates all 15 courts sequentially by
  changing the dropdown selection.
