# RII — Rechtsprechung im Internet (Federal Courts)

## Portal

- **URL**: https://www.rechtsprechung-im-internet.de
- **Name**: Rechtsprechung im Internet (RII)
- **Technology**: Server-rendered HTML with ZIP/XML document archives
- **Authentication**: None required
- **Courts**: BVerfG, BGH, BVerwG, BFH, BAG, BSG, BPatG (7 federal courts)

## Architecture

`ScraperBaseClient` -> `RiiCaseProvider`

The provider scrapes search result pages for document IDs, then downloads
per-document ZIP archives containing XML with structured metadata and content.

## Search

GET requests to the portal with URL-based pagination. Document IDs are extracted
from search result links via regex (`doc.id=XXX`).

## Document Retrieval

Each document is a ZIP archive downloaded from:

```
/jportal/docs/bsjrs/{doc_id}.zip
```

The ZIP contains an XML file with the full decision. The XML is parsed via lxml
with XPath queries against `//dokument/*` tags.

## Field Mappings

| XML Field | OLDP Field | Notes |
|---|---|---|
| `gertyp` + `gerort` | `court_name` | Concatenated, e.g. "BGH" |
| `entsch-datum` | `date` | YYYYMMDD -> YYYY-MM-DD |
| `aktenzeichen` | `file_number` | Court file number |
| `doktyp` | `type` | Optional (e.g. Urteil, Beschluss) |
| `ecli` | `ecli` | Optional |
| `leitsatz` | `abstract` | Optional (guiding principle) |

Content is assembled from multiple XML sections:

| XML Tag | Content |
|---|---|
| `tenor` | Operative part |
| `tatbestand` | Facts |
| `entscheidungsgruende` | Reasoning |
| `gruende` | Grounds (alternative) |
| `abwmeinung` | Dissenting opinion |
| `sonstlt` | Other long text |

## Pagination

- URL-based page navigation through search results
- Stops after **2 consecutive empty pages**

## Usage

```bash
# Fetch 10 federal court cases
oldp-ingestor -v cases --provider rii --limit 10

# Via deployment script
bash dev-deployment/ingest.sh cases rii
```

## Date Filtering

Date filtering is server-side via the Playwright extended search form. When
`--date-from` / `--date-to` CLI flags are provided, the provider:

1. Opens the search page with Playwright
2. Clicks "Entscheidungssuche", then "Erweiterte Suche" to reveal date fields
3. Fills `#dateFrom` / `#dateTo` inputs (DD.MM.YYYY format, converted from ISO)
4. Submits the form — the portal returns only matching results

When no dates are set, falls back to the fast HTTP-only per-court listing
(no Playwright needed).

## Known Quirks

- **Access filtering**: Non-public documents (`accessRights != "public"`) are
  skipped silently.
- **Empty content**: Cases with no content sections are skipped.
- **ZIP format**: Each ZIP contains exactly one XML file; the provider extracts
  the first entry.
