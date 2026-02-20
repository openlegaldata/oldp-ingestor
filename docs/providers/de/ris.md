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
wraps a work-level `item` plus a `workExample` reference.

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
URLs).

### Article HTML

```
GET /v1/legislation/eli/.../{pointInTimeManifestation}/{subtype}/{articleEid}.html
```

Returns a full HTML page.  The ingestor extracts the `<body>` content.

## Case law endpoints

### List case law

```
GET /v1/case-law?size=300&pageIndex=0
```

Returns a `hydra:Collection` with paginated `member[]` items.

**Query parameters:**

| Parameter | Description |
|-----------|-------------|
| `size` | Results per page (1–300, default 100) |
| `pageIndex` | 0-indexed page number |
| `courtType` | Filter by court code (e.g. `BGH`, `BVerfG`) |
| `decisionDateFrom` / `decisionDateTo` | Decision date range (ISO 8601) |

**Response item fields:**

| Field | Type | Description |
|-------|------|-------------|
| `documentNumber` | string | Unique document identifier (e.g. `KORE702312026`) |
| `courtType` | string | Court code (e.g. `BGH`) |
| `courtName` | string | Court code (same as `courtType` in practice) |
| `fileNumbers` | string[] | Court file numbers (e.g. `["VIa ZR 712/22"]`) |
| `decisionDate` | string | ISO 8601 date (e.g. `2026-02-03`) |
| `documentType` | string | Decision type (e.g. `Urteil`, `Beschluss`) |
| `ecli` | string | European Case Law Identifier |
| `headline` | string | Short headline (e.g. `BGH, 03.02.2026, VIa ZR 712/22`) |

**Pagination:** Same as legislation — check `view.next`.

### Case detail

```
GET /v1/case-law/{documentNumber}
```

Returns the full decision metadata including abstract-like fields:

| Field | Description |
|-------|-------------|
| `guidingPrinciple` | Leitsatz (guiding principle) — preferred for abstract |
| `headnote` | Headnote |
| `otherHeadnote` | Alternative headnote |
| `tenor` | Tenor (operative part) — last-resort abstract |

The ingestor uses the first non-empty field as the case abstract.

### Case HTML

```
GET /v1/case-law/{documentNumber}.html
```

Returns the full HTML decision text.  The ingestor extracts the `<body>`
content.

### Court list

```
GET /v1/case-law/courts
```

Returns a **plain JSON list** (not a hydra collection) of court objects:

```json
[
  {"id": "BGH", "count": 33541, "label": "Bundesgerichtshof"},
  {"id": "BFH", "count": 11379, "label": "Bundesfinanzhof"},
  ...
]
```

The ingestor caches this on first use to resolve court codes to full names.

## Field mapping: RIS to OLDP

### Laws

| RIS field | OLDP field | Notes |
|-----------|------------|-------|
| `abbreviation` | `code` | Law book code (e.g. `BGB`) |
| `name` | `title` | Full title |
| `legislationDate` | `revision_date` | Consolidation date |
| Article `name` | `section`, `title` | Parsed via `_parse_article_name()` |
| Article HTML body | `content` | `<body>` extracted |
| Article `eId` | `slug` | Slugified |

### Cases

| RIS field | OLDP field | Notes |
|-----------|------------|-------|
| `courtName` | `court_name` | Resolved to full label via `/v1/case-law/courts` |
| `fileNumbers[0]` | `file_number` | First element of array |
| `decisionDate` | `date` | Already ISO 8601 |
| HTML body | `content` | `<body>` extracted from `.html` endpoint |
| `documentType` | `type` | Optional (e.g. `Urteil`) |
| `ecli` | `ecli` | Optional |
| `headline` | `title` | Optional |
| `guidingPrinciple` / `headnote` / `tenor` | `abstract` | First non-empty wins, truncated to 50 000 chars |

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
