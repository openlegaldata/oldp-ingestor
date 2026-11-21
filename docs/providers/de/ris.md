# RIS — Rechtsinformationssystem des Bundes

The [Rechtsinformationsportal des Bundes](https://testphase.rechtsinformationen.bund.de)
(RIS) is the German federal legal information portal.  The oldp-ingestor uses
its REST API to fetch both **legislation** and **case law**.

API docs: <https://docs.rechtsinformationen.bund.de/>

## Base URL

```
https://testphase.rechtsinformationen.bund.de
```

> Note: This is the trial/test-phase URL.  It may change when the service goes
> into production.

## Authentication

No authentication is required.

## Rate limiting

- **600 requests per minute** per client IP.
- Exceeding the limit returns `503 Service Unavailable` (sometimes `429`).
- The `Retry-After` header may be present on rate-limited responses.
- **Recommendation**: keep request delay at 0.2 s (~300 req/min) and implement
  exponential backoff.  The ingestor does both.

## Legislation endpoints

### List legislation

```
GET /v1/legislation?size=300&pageIndex=0
```

Returns a `hydra:Collection` with paginated `member[]` items.  Each member
wraps an expression-level `item`: its `@id` is the expression detail URL and
the work-level data (including `legislationDate`) sits in the nested
`exampleOfWork`.  (Until late 2026 the item was work-level with a
`workExample` reference to the expression; the provider still understands
that legacy shape.)

**Query parameters:**

| Parameter | Description |
|-----------|-------------|
| `size` | Results per page (1–300, default 100) |
| `pageIndex` | 0-indexed page number |
| `searchTerm` | Full-text filter (AND logic; quote for exact phrase) |
| `dateFrom` / `dateTo` | Adoption date range (ISO 8601) |
| `sort` | Sort field: `date`, `-date`, `temporalCoverageFrom`, `legislationIdentifier` |

**Pagination:** `view.next` / `view.previous` contain links; `totalItems` gives
the total count.

### Expression detail

```
GET /v1/legislation/eli/{jurisdiction}/{agent}/{year}/{naturalId}/{pointInTime}/{version}/{language}
```

Returns `hasPart` (article list) and `encoding` (HTML/XML/ZIP manifestation
URLs) at the top level.  Each `hasPart` entry carries the section label in
`name` (`"§ 1"`) and the title in `headline` (`"Anwendungsbereich"`);
unlabelled parts such as the preamble have an empty `name`.

### Article HTML

```
GET /v1/legislation/eli/.../{pointInTimeManifestation}/{subtype}/{articleEid}.html
```

Returns a full HTML page.  The ingestor extracts the `<body>` content.

## Case law endpoints

> **Endpoint rename (October 2026).** All `/v1/case-law…` paths now answer
> `410 Gone` with a pointer to `/v1/rechtsprechung…` (prod `cases-ris`
> failed daily from 2026-10-07 until the provider was updated). Filter
> parameters and detail field names changed at the same time — see below.
> The full API spec is served at `/openapi.json`.

### List case law

```
GET /v1/rechtsprechung?size=300&pageIndex=0
```

Returns a `hydra:Collection` with paginated `member[]` items, newest first.
`totalItems` is capped at 10 000.

**Query parameters:**

| Parameter | Description |
|-----------|-------------|
| `size` | Results per page (1–300, default 100) |
| `pageIndex` | 0-indexed page number |
| `court` | Filter by court code (e.g. `BGH`, `BVerfG`); was `courtType` |
| `dateFrom` / `dateTo` | Decision date range (ISO 8601), honoured server-side; was `decisionDateFrom` / `decisionDateTo` (which the old endpoint ignored) |
| `searchTerm` | Full-text search (used by citation lookup) |

**Response item fields:**

| Field | Type | Description |
|-------|------|-------------|
| `documentNumber` | string | Unique document identifier (e.g. `KORE702312026`) |
| `courtType` | string | Court code (e.g. `BGH`) |
| `courtName` | string | Court code (same as `courtType`, e.g. `BGH` — no seat suffix since the rename) |
| `fileNumbers` | string[] | Court file numbers (e.g. `["VIa ZR 712/22"]`) |
| `decisionDate` | string | ISO 8601 date (e.g. `2026-02-03`) |
| `documentType` | string | Decision type (e.g. `Urteil`, `Beschluss`) |
| `ecli` | string | European Case Law Identifier |
| `headline` | string | Short headline (e.g. `BGH, 03.02.2026, VIa ZR 712/22`) |

**Pagination:** Same as legislation — check `view.next`.

### Case detail

```
GET /v1/rechtsprechung/{documentNumber}
```

Returns the full decision metadata with **German field names** (the legacy
English names are still read as fallbacks). Abstract-like fields:

| Field (legacy name) | Description |
|-------|-------------|
| `leitsatz` (`guidingPrinciple`) | Leitsatz (guiding principle) — preferred for abstract |
| `orientierungssatz` (`headnote`) | Orientierungssatz (headnote) |
| `sonstigerOrientierungssatz` (`otherHeadnote`) | Alternative headnote |
| `tenor` | Tenor (operative part) — last-resort abstract |

The ingestor uses the first non-empty field as the case abstract.
`lookup_fetch` also reads `gericht`, `aktenzeichenListe`, `datum`,
`dokumenttyp` and `kurztitel` from the detail payload.

### Case HTML

```
GET /v1/rechtsprechung/{documentNumber}.html
```

Returns the full HTML decision text.  The ingestor extracts the `<body>`
content.

### Court list

```
GET /v1/rechtsprechung/courts
```

Returns a **plain JSON list** (not a hydra collection) of court objects:

```json
[
  {"id": "BGH", "count": 35768, "label": "Bundesgerichtshof"},
  {"id": "BVerwG", "count": 10480, "label": "BVerwG"},
  ...
]
```

The ingestor caches this on first use to resolve court codes to full names.

**Bare-code overrides** (since the rename): some labels are just the
abbreviation (`BVerwG`, `BPatG`, `BVerfG`), which OLDP cannot resolve.
`_RIS_COURT_LABEL_OVERRIDES` therefore maps all seven federal court codes
to their canonical OLDP names.

**Seat-city suffix override** (legacy endpoint): every federal-court entry shipped with the
court's seat city appended to both `id` and `label` (verified 2026-06-04;
applies to BGH, BFH, BVerwG, BPatG, BAG, BSG, BVerfG). OLDP only
recognises the canonical institutional name, so posting the API label
verbatim yielded 400 `court_not_found` in prod
(2026-05-28 `cases-ris` error on `III ZR 56/25`). The provider
short-circuits these labels via `_RIS_COURT_LABEL_OVERRIDES` in
`ris_cases.py` and emits the bare court name (e.g. `Bundesgerichtshof`).

## Field mapping: RIS to OLDP

### Laws

| RIS field | OLDP field | Notes |
|-----------|------------|-------|
| `abbreviation` | `code` | Law book code (e.g. `BGB`) |
| `name` | `title` | Full title |
| `exampleOfWork.legislationDate` | `revision_date` | Consolidation date (legacy: top-level `legislationDate`) |
| Article `name` / `headline` | `section`, `title` | `name` = section, `headline` = title; legacy combined `name` parsed via `_parse_article_name()` |
| Article HTML body | `content` | `<body>` extracted |
| Article `eId` | `slug` | Slugified |

### Cases

| RIS field | OLDP field | Notes |
|-----------|------------|-------|
| `courtName` | `court_name` | Override table, else label from `/v1/rechtsprechung/courts` |
| `fileNumbers[0]` | `file_number` | First element of array |
| `decisionDate` | `date` | Already ISO 8601 |
| HTML body | `content` | `<body>` extracted from `.html` endpoint |
| `documentType` | `type` | Optional (e.g. `Urteil`) |
| `ecli` | `ecli` | Optional |
| `headline` | `title` | Optional |
| `leitsatz` / `orientierungssatz` / `sonstigerOrientierungssatz` / `tenor` | `abstract` | First non-empty wins, truncated to 50 000 chars |

## Date Filtering

Both legislation and case law endpoints support server-side date filtering via
query parameters:

- **Legislation**: `dateFrom` / `dateTo` (adoption date range, ISO 8601)
- **Case law**: `dateFrom` / `dateTo` (decision date range, ISO 8601)

When `--date-from` / `--date-to` CLI flags are provided, the corresponding query
parameters are included in the API request, so the server returns only matching
results. The case provider additionally re-checks dates client-side and stops
paginating at the first decision older than `--date-from` (the legacy
`/v1/case-law` endpoint ignored its date parameters).

## Request volume estimates

Each **law book** costs: 1 list page request + 1 expression detail + N article
HTML requests (one per article).

Each **case** costs: 1 list page request (amortised over page size) + 1 HTML
request + 1 detail request = **~3 requests per case**.

| Scenario | Total items | Est. requests | Time at 0.2 s delay |
|----------|-------------|---------------|----------------------|
| All legislation | ~6 000 books | ~50 000+ | hours |
| All case law | ~10 000 cases | ~20 000 | ~67 min |
| Daily incremental cases | ~10–50 | ~30–150 | < 1 min |
