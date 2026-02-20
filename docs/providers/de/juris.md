# Juris — eJustiz Bürgerservice State Law Portals

## Portal

- **Name**: eJustiz Bürgerservice / Landesrecht portals (hosted by Juris)
- **Technology**: React SPA with path-based document URLs
- **Authentication**: None required

### State Portals

The base `JurisCaseProvider` is subclassed for 10 German states:

| CLI Provider | State | Portal URL |
|---|---|---|
| `juris-bb` | Berlin-Brandenburg | gesetze.berlin.de/bsbe |
| `juris-bw` | Baden-Württemberg | landesrecht-bw.de/bsbw |
| `juris-he` | Hessen | lareda.hessenrecht.hessen.de/bshe |
| `juris-hh` | Hamburg | landesrecht-hamburg.de/bsha |
| `juris-mv` | Mecklenburg-Vorpommern | landesrecht-mv.de/bsmv |
| `juris-rlp` | Rheinland-Pfalz | landesrecht.rlp.de/bsrp |
| `juris-sa` | Sachsen-Anhalt | landesrecht.sachsen-anhalt.de/bsst |
| `juris-sh` | Schleswig-Holstein | gesetze-rechtsprechung.sh.juris.de/bssh |
| `juris-sl` | Saarland | recht.saarland.de/bssl |
| `juris-th` | Thüringen | landesrecht.thueringen.de/bsth |

## Architecture

`PlaywrightBaseClient` -> `JurisCaseProvider` -> 10 state-specific subclasses

All state variants inherit from `JurisCaseProvider`, which implements all
scraping logic. Each subclass only defines the portal's `base_url` and
`portal_path`. Playwright is required because the portal is a React SPA.

## Search

Navigate to the search portlet with query parameters:

```
/{portal_path}/js_peid/Suchportlet1/media-type/html?eventSubmit_doSearch=suchen&query=*&...
```

**Page 1**: Basic search parameters
**Page 2+**: Additional `currentNavigationPosition` (offset) and `numberofresults=15000`

Document IDs are extracted via regex: `/document/([A-Z0-9]+)/`

Wait selectors: `.result-list-entry`, `.docLayoutText`, `.jportal-content`
(timeout 15s).

## Case Detail

Navigate to the document page:

```
/{portal_path}/document/{doc_id}
```

### Info Table Parsing

Metadata is extracted from an info table with support for both old and new
markup formats:

- **Old format**: `<td class="TD30">` label / value pairs
- **New format**: `<th class="TD30">` label / value pairs
- **Fallback**: div-based layout with `fieldLabel` class

| Portal Field | OLDP Field | Notes |
|---|---|---|
| `Gericht` | `court_name` | Court name |
| `Entscheidungsdatum` | `date` | DD.MM.YYYY -> YYYY-MM-DD |
| `Aktenzeichen` | `file_number` | Court file number |
| `Dokumenttyp` | `type` | Optional |
| `ECLI` | `ecli` | Optional |

### Content Extraction

Content is extracted from one of these selectors (first match wins):
- `.docLayoutText`
- `.documentText`
- `.content`

Before serialisation, Juris-specific UI artifacts are removed:
- Permalink buttons
- `/jportal/` images
- `data-juris-*` attributes
- HTML comments
- `.unsichtbar` spans
- `.doclinks` elements

## Pagination

- Results are paginated via URL offset parameters
- Stops after **2 consecutive empty pages**

## Usage

```bash
# Fetch 10 cases from Berlin-Brandenburg
oldp-ingestor -v cases --provider juris-bb --limit 10

# Fetch Hessen cases with date filter
oldp-ingestor -v cases --provider juris-he --date-from 2025-01-01

# Via deployment script
bash dev-deployment/ingest.sh cases juris-bb
bash dev-deployment/ingest.sh cases juris-bw
# ... etc. for each state
```

## Known Quirks

- **Playwright required**: The portal is a React SPA that requires JavaScript
  rendering. Plain HTTP requests receive empty pages.
- **Multiple markup versions**: The info table uses at least 3 different HTML
  structures (`<td>`, `<th>`, `<div>`) across portal versions. The provider
  attempts all three.
- **UI artifact cleanup**: The content contains Juris-specific UI elements
  (permalink buttons, tracking attributes, hidden spans) that must be stripped
  before storing.
- **Content threshold**: Cases with content shorter than 10 characters are skipped.
- **Request delay**: Default 0.5s between page loads (Playwright default).
- **Validation**: Cases missing `court_name` are skipped.
- **15-second timeout**: Page loads time out after 15 seconds if the expected
  selector is not found.
