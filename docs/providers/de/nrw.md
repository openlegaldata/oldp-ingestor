# NRW — NRWE Rechtsprechungsdatenbank (North Rhine-Westphalia)

## Portal

- **URL**: https://nrwesuche.justiz.nrw.de
- **Name**: NRWE Rechtsprechungsdatenbank
- **Technology**: PHP, server-rendered HTML, POST-based search
- **Authentication**: None required

## Architecture

`ScraperBaseClient` -> `NrwCaseProvider`

The provider uses POST-based form search with session cookies, then scrapes
individual case HTML pages for metadata and content.

## Search

POST to `/index.php` with form data:

| Parameter | Value |
|---|---|
| `gerichtstyp` | Court type filter (optional) |
| `von` | Date from (optional) |
| `bis` | Date to (optional) |
| `q` | Search query (default `*`) |
| `method` | `stem` |
| `sortieren_nach` | Sort field |

Returns 100 results per page. Case URLs are extracted from `.einErgebnis a`
CSS selector.

## Field Mappings

Metadata is extracted from the case detail page (div/text structure):

| Portal Field | OLDP Field | Notes |
|---|---|---|
| `Gericht:` | `court_name` | e.g. "OLG Düsseldorf" |
| `Datum:` | `date` | DD.MM.YYYY -> YYYY-MM-DD |
| `Aktenzeichen:` | `file_number` | Court file number |
| `ECLI:` | `ecli` | Optional |
| `Entscheidungsart:` | `type` | Optional (e.g. Urteil, Beschluss) |

Content is extracted from `p.absatzLinks` parent's inner HTML. Tenor is
prepended to the content. Section headlines are marked with `<h2>` tags via
regex processing.

## Pagination

- 100 results per page
- Maximum 1600 pages
- Session-based with cookies

## Usage

```bash
# Fetch 10 NRW court cases
oldp-ingestor -v cases --provider nrw --limit 10

# Filter by date
oldp-ingestor -v cases --provider nrw --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases nrw
```

## Known Quirks

- **Validation**: Cases missing `court_name` or `file_number` are skipped.
- **Section headlines**: The provider uses regex to detect and wrap section
  headlines (like "Tenor", "Tatbestand", "Entscheidungsgründe") in `<h2>`
  tags for better structure.
- **Content assembly**: The Tenor section is extracted separately and prepended
  to the main content body.
