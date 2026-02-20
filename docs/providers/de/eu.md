# EUR-Lex Provider

Fetches case law from the European Court of Justice via EUR-Lex.

## Authentication

No authentication required. The provider uses the public CELLAR SPARQL endpoint
for ECLI search, and public EUR-Lex pages for case details and content.

The `EURLEX_USER` / `EURLEX_PASSWORD` settings exist for CLI compatibility but
are not used by the current implementation.

## Data Pipeline

The provider uses a three-step pipeline:

1. **SPARQL search** — GET to `https://publications.europa.eu/webapi/rdf/sparql`
   with a SPARQL query. Returns paginated ECLI identifiers with dates.
2. **XML detail** — GET `https://eur-lex.europa.eu/legal-content/DE/TXT/XML/?uri=ECLI:{ecli}`
   to extract metadata (date, title, file number, case type).
3. **HTML content** — GET `https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=ECLI:{ecli}`
   to extract the full decision text.

## SPARQL Query

The CELLAR SPARQL endpoint is queried for ECLI identifiers using the CDM ontology:

```sparql
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?ecli ?date
WHERE {
  ?work cdm:case-law_ecli ?ecli .
  ?work cdm:work_date_document ?date .
  FILTER(?date >= "2024-01-01"^^xsd:date)
  FILTER(?date <= "2024-12-31"^^xsd:date)
}
ORDER BY DESC(?date)
LIMIT 100
OFFSET 0
```

Date filters are added only when `--date-from` / `--date-to` are provided.
Pagination uses `LIMIT` / `OFFSET` with a page size of 100.

## Field Mappings (EUR-Lex to OLDP)

| EUR-Lex | OLDP | Notes |
|---------|------|-------|
| ECLI (from SPARQL) | `ecli` | e.g. `ECLI:EU:C:2024:123` |
| `WORK_DATE_DOCUMENT/VALUE` | `date` | YYYY-MM-DD |
| `EXPRESSION_TITLE/VALUE` (DEU) | `title` | First part before `#` |
| Title regex / `SAMEAS` fallback | `file_number` | e.g. `C-42/24` |
| `RESOURCE_LEGAL_ID_CELEX/VALUE` | `type` | Via CELEX type mapping |
| HTML `<body>` content | `content` | Links processed |
| Fixed | `court_name` | Always `Europäischer Gerichtshof` |

## CELEX Type Mapping

CELEX numbers in sector 6 (case law) map to German case type names:

| Code | Type |
|------|------|
| CJ, TJ, FJ | Urteil |
| CO, TO, FO, CX | Beschluss |
| CC, TC | Schlussantrag des Generalanwalts |
| CD | Entscheidung |
| CV | Gutachten |
| CS | Pfändung |
| CT, TT, FT | Drittwiderspruch |
| CP | Stellungnahme |

## Usage

```bash
# Fetch 10 cases
oldp-ingestor -v cases --provider eu --limit 10

# Incremental fetch by date range
oldp-ingestor -v cases --provider eu --date-from 2024-01-01 --date-to 2024-12-31

# Via deployment script
bash dev-deployment/ingest.sh cases eu
```

## Known Quirks

- **File number extraction**: File numbers (e.g. `C-42/24`) are extracted from the title
  via regex. When the title contains no match, the provider falls back to the
  `SAMEAS/URI` element with `TYPE=case`.
- **Unicode dash**: EUR-Lex uses Unicode non-breaking hyphen (U+2011) in file numbers.
  The provider replaces these with ASCII dashes.
- **Title splitting**: Titles contain `#`-separated parts. Only the first part is used.
- **Content threshold**: Cases with `<body>` content shorter than 10 characters are skipped.
- **XML declarations in HTML**: Some EUR-Lex HTML pages include `<?xml encoding=...?>`
  declarations that break `lxml.html.fromstring()`. The provider strips these before parsing.
- **404 on recent ECLIs**: Very recent cases may have ECLI entries in CELLAR but no
  XML/HTML pages on EUR-Lex yet. These are skipped with a warning.
