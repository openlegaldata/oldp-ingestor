# HB — Justiz Bremen (Bremen Courts)

## Portal

- **Name**: Justiz Bremen — Entscheidungssuche
- **Technology**: SixCMS content management, HTML listings with PDF-only content
- **Authentication**: None required

### Court Portals

The provider covers 5 separate Bremen court portals:

| Court | URL |
|---|---|
| OLG Bremen | oberlandesgericht.bremen.de |
| OVG Bremen | oberverwaltungsgericht.bremen.de |
| VG Bremen | verwaltungsgericht.bremen.de |
| LAG Bremen | landesarbeitsgericht.bremen.de |
| StGH Bremen | staatsgerichtshof.bremen.de |

## Architecture

`ScraperBaseClient` -> `BremenCaseProvider`

The provider iterates over all 5 court portals, scrapes paginated HTML listing
pages, and extracts case metadata from table rows. Case content is extracted
from PDF files via pymupdf.

## Search

GET request to each court's search path:

```
{path}?max=100&skip={skip}
```

Each portal has a specific base URL and search path. Results are paginated by
`skip` parameter in increments of 100 (the `max` parameter).

## Listing Page Structure

Results are in `tr.search-result` table rows with a two-column layout:

**Left TD** (metadata, separated by `<br>` tags):
1. Date
2. File number
3. Referenced norms
4. Legal area
5. Decision type

**Right TD** (document links):
- PDF link: `/sixcms/media.php/NNN.pdf`
- Title (from PDF link text)
- Detail page link: `detail.php?gsid=...`

Each row also has a `data-date` attribute (YYYY-MM-DD) used for date filtering.

## Field Mappings

| Source | OLDP Field | Notes |
|---|---|---|
| Left TD, field 2 | `file_number` | Court file number |
| `data-date` attribute | `date` | YYYY-MM-DD |
| Current court config | `court_name` | From portal configuration |
| PDF link text | `title` | Optional, stripped of "(pdf, NNN KB)" suffix |
| Left TD, field 5 | `type` | Urteil, Beschluss, or Sonstiges |
| Detail page Leitsatz | `abstract` | Optional (fetched from detail page) |
| PDF text | `content` | Extracted via pymupdf, wrapped as `<p>` per page |

## Pagination

- Page size: 100 results (`max=100`)
- Offset via `skip` parameter (0, 100, 200, ...)
- Stops when fewer than 100 results are returned

## Date Filtering

Date filtering is done client-side using the `data-date` row attribute after
fetching. The `--date-from` and `--date-to` CLI flags are applied against this
attribute.

## Usage

```bash
# Fetch 10 Bremen court cases
oldp-ingestor -v cases --provider hb --limit 10

# Filter by date
oldp-ingestor -v cases --provider hb --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases hb
```

## Known Quirks

- **5 separate portals**: The provider iterates all 5 court portals sequentially,
  combining results into a single output stream.
- **PDF-only content**: All case content is in PDF files. There is no HTML
  full-text on the portal. The provider uses pymupdf to extract text.
- **Content as HTML**: PDF text is converted to HTML by wrapping each page's text
  in `<p>` tags.
- **Content threshold**: Cases with content shorter than 10 characters are skipped.
- **SixCMS row structure**: Metadata fields in the left TD are separated by `<br>`
  tags. The provider must serialize inner HTML and split on `<br>` to extract
  fields (using `text_content()` alone would lose the separators).
- **Detail page for abstract**: The Leitsatz (abstract) is only available on the
  detail page, which requires a separate HTTP request per case.
