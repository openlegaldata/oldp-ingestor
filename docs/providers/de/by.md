# BY — Gesetze Bayern (Bavarian Courts)

## Portal

- **URL**: https://www.gesetze-bayern.de
- **Name**: Gesetze Bayern — Bayerische Rechtsprechung
- **Technology**: Server-rendered HTML with session-based search, ZIP/XML archives
- **Authentication**: None required

## Architecture

`ScraperBaseClient` -> `ByCaseProvider`

The provider navigates a session-based search interface, downloads per-document
ZIP archives containing XML with structured metadata and content.

## Search

1. POST to `/Search/Filter/DOKTYP/rspr` to initialise a search session (cookies)
2. GET `/Search/Page/{page}` for paginated result pages
3. Extract document IDs via regex: `/Content/Document/(.*?)`

## Document Retrieval

Each document is a ZIP archive downloaded from:

```
/Content/Zip/{doc_id}
```

The ZIP contains an XML file encoded as **ISO-8859-1**. The provider decodes it
to UTF-8 and updates the XML encoding declaration before parsing with lxml.

## Field Mappings

Metadata from `//metadaten/`, content from `//textdaten/`:

| XML Field | OLDP Field | Notes |
|---|---|---|
| `gericht/gertyp` + `gerort` | `court_name` | Concatenated |
| `doktyp` | `type` | Expanded: Bes->Beschluss, Urt->Urteil, Ent->Entscheidung |
| `entsch-datum` | `date` | Already YYYY-MM-DD format |
| `aktenzeichen` | `file_number` | Court file number |
| `leitsatz` | `abstract` | Optional (guiding principle) |
| `titelzeile` | `title` | Optional (tags stripped) |

Content is assembled from the same XML sections as RII:

| XML Tag | Content |
|---|---|
| `tenor` | Operative part |
| `tatbestand` | Facts |
| `entschgruende` | Reasoning |
| `gruende` | Grounds (alternative) |
| `abwmeinung` | Dissenting opinion |
| `sonstlt` | Other long text |

## Pagination

- 1-indexed pages (`/Search/Page/1`, `/Search/Page/2`, ...)
- Session-based: the search session must be initialised first via POST
- Stops when a page returns no document IDs

## Usage

```bash
# Fetch 10 Bavarian court cases
oldp-ingestor -v cases --provider by --limit 10

# Via deployment script
bash dev-deployment/ingest.sh cases by
```

## Known Quirks

- **ISO-8859-1 encoding**: The XML inside the ZIP is ISO-8859-1 encoded. The
  provider must re-encode to UTF-8 and patch the `encoding="..."` declaration
  before parsing. Failure to do this causes lxml errors on German special
  characters (umlauts, eszett).
- **Session cookies**: Search requires an active session. The provider POSTs to
  the filter endpoint first to establish cookies.
- **Empty content**: Cases with no text content sections are skipped.
