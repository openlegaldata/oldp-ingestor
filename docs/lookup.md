# Targeted citation-based lookup

The `oldp-ingestor lookup` subcommand group is a small set of typed tools
designed for an AI agent (e.g. Claude Code) to fetch a specific decision
when OLDP could not resolve an extracted citation. Unlike the streaming
`cases` flow, each lookup call performs **one** upstream request and
returns JSON with a three-status contract.

The agent owns citation parsing, court → provider routing, hit
adjudication (date proximity, AZ normalization, court synonyms). The CLI
exposes the four mechanical primitives: list providers, search, fetch
one document, ingest one document.

## Output contract

Every command writes one JSON object to stdout:

```json
{ "status": "ok",        "result": ...,  "candidates": [...], "case": ... }
{ "status": "not_found", "reason": "..." }
{ "status": "error",     "reason": "...", "details": ... }
```

| status | exit code | meaning |
|---|---|---|
| `ok` | 0 | upstream returned data |
| `not_found` | 1 | upstream ran successfully but yielded zero hits |
| `error` | 2 | bad arguments or upstream/network failure |

This lets a calling agent distinguish "the index does not have this
citation" from "the query was malformed" without parsing stderr.

## Commands

### `lookup providers`

Lists every lookup-capable provider with the keys it accepts, a coarse
`cost` hint, and (unless `--no-resolve-courts`) the live OLDP courts the
provider covers. Court coverage is resolved by joining each provider's
declared `court_filter` against the public OLDP courts API at
`OLDP_PROD_API_URL` (defaults to `https://de.openlegaldata.io`, **no
auth header sent** — so it works from a dev shell with no OLDP token).

```bash
oldp-ingestor lookup providers
```

Sample output (truncated):

```json
{
  "status": "ok",
  "result": {
    "ris": {
      "lookup_keys": ["file_number", "ecli", "court"],
      "cost": "low",
      "court_filter": {"court_types": ["BGH","BFH","BVerwG","BAG","BSG","BPatG","BVerfG"]},
      "courts": [{"id": 6, "name": "Bundesgerichtshof", "code": "BGH", ...}, ...],
      "courts_total": 7,
      "source": {"name": "...", "homepage": "..."}
    },
    "nrw": {
      "lookup_keys": ["file_number"],
      "cost": "medium",
      "court_filter": {"state_ids": [12]},
      "courts": [...],
      "courts_total": 227
    },
    "juris-rlp": {
      "lookup_keys": ["file_number"],
      "cost": "high",
      "court_filter": {"state_ids": [13]},
      ...
    }
  }
}
```

The `cost` hint:

- `low` — single API request, no headless browser.
- `medium` — single SSR request (HTTP scrape).
- `high` — Playwright (Chromium) launch; ~1-2s cold start.

### `lookup search`

Searches one provider for candidates matching a citation. Returns a
list of lightweight candidate dicts (no full content) so the agent can
choose before paying for a fetch.

```bash
oldp-ingestor lookup search --provider ris --ecli "ECLI:DE:BGH:2024:120524UVIZR12722.0"
oldp-ingestor lookup search --provider ris --file-number "VI ZR 127/22" --court-hint BGH
oldp-ingestor lookup search --provider juris-rlp --file-number "5 T 16/26"
oldp-ingestor lookup search --provider nrw --file-number "23 A 416/25.A"
oldp-ingestor lookup search --provider ns --file-number "10 B 1105/26"
```

Each candidate:

```json
{
  "doc_id":      "KORE615402026",
  "court_name":  "Bundesgerichtshof Karlsruhe",
  "file_number": "VIa ZR 482/23",
  "date":        "2026-06-02",
  "ecli":        "ECLI:DE:BGH:2026:020626BVIAZR482.23.0",
  "type":        "Beschluss",
  "snippet":     "BGH, 02.06.2026, VIa ZR 482/23"
}
```

`doc_id` is opaque to the agent — it's passed back to `fetch` / `ingest`
verbatim. Format varies per provider (RIS document number, juris
SPA id, NRW absolute URL, voris path).

Capability rules:

- `--ecli` is honoured only by RIS. NRW / voris / juris raise
  `error: provider does not support ecli`.
- `--court-hint` is honoured by RIS when it matches a known federal
  court code (`BGH`, `BFH`, `BVerwG`, `BAG`, `BSG`, `BPatG`, `BVerfG`).
  Other providers ignore it (their portal scope is already
  jurisdiction-specific).
- `--date` is currently a hint only — the candidate list is not
  pre-filtered by date. The agent reads `date` from each candidate.
- `--limit` defaults to 10. All providers filter candidates to *exact*
  file-number / ECLI matches before truncating, so the typical result
  is one candidate.

### `lookup fetch`

Fetches the full case dict for `(provider, doc_id)` — same shape as
the streaming `cases` flow yields, including HTML `content`.

```bash
oldp-ingestor lookup fetch --provider ris --doc-id KORE615402026
```

Use this when the agent wants to inspect content before ingesting
(e.g. to verify the citation matches the body text).

### `lookup ingest`

Fetches the case and POSTs it to the OLDP API in one step. Returns the
OLDP case id (when the backend supplies one) and a slim echo of the
case metadata (without `content` to keep the response small).

```bash
oldp-ingestor lookup ingest --provider ris --doc-id KORE615402026
```

Idempotency:

- A 409 from OLDP (case already exists) is **not** an error — the
  response is `status: "ok"` with `already_exists: true`. A retrying
  agent that doesn't track its own state can safely re-call
  `ingest` for the same `doc_id`.

## Agent loop

A typical agent flow when OLDP signals "couldn't resolve this citation":

1. `lookup providers` (once per session, cache locally) — agent learns
   which provider covers which court.
2. Parse citation → `(court, file_number, date)` or `ecli`.
3. Pick provider by mapping court → providers' `courts` array.
4. `lookup search` against the chosen provider; pick a candidate by
   date proximity and exact AZ.
5. `lookup ingest` for the picked `doc_id`.

If step 4 returns `not_found`, the agent can fall back to a second
provider (e.g. RIS → juris federal mirror) or report back to OLDP.

## Provider coverage (slices 1–3)

| Provider | Cost | Keys | Covered courts |
|---|---|---|---|
| `ris` | low | `file_number`, `ecli`, `court` | 7 federal courts (BGH/BFH/BVerwG/BAG/BSG/BPatG/BVerfG) |
| `nrw` | medium | `file_number` | all Nordrhein-Westfalen courts (227) |
| `ns` | medium | `file_number` | all Niedersachsen courts (129) |
| `juris-bb` | high | `file_number` | Berlin + Brandenburg |
| `juris-hh` | high | `file_number` | Hamburg |
| `juris-mv` | high | `file_number` | Mecklenburg-Vorpommern |
| `juris-rlp` | high | `file_number` | Rheinland-Pfalz |
| `juris-sa` | high | `file_number` | Sachsen-Anhalt |
| `juris-sh` | high | `file_number` | Schleswig-Holstein |
| `juris-bw` | high | `file_number` | Baden-Württemberg |
| `juris-sl` | high | `file_number` | Saarland |
| `juris-he` | high | `file_number` | Hessen |
| `juris-th` | high | `file_number` | Thüringen |

Court totals are resolved at runtime against the OLDP courts API and
will track whatever OLDP currently knows. Providers that aren't listed
(by, hb, sn, sn-ovg, sn-verfgh, rii, gii, eu) don't yet implement
`LookupMixin` and are silently excluded from the `lookup providers`
output — they can be added in follow-up slices when their upstream
search supports a meaningful AZ filter.

## Adding a new provider

1. Have the provider class inherit from `LookupMixin` (declared in
   `src/oldp_ingestor/providers/lookup.py`).
2. Declare `LOOKUP_CAPABILITY = LookupCapability(...)` with the
   keys it accepts and either a `court_types` list or a `state_ids`
   list (or both) describing the OLDP court coverage.
3. Implement `lookup_search` — one upstream request, return candidate
   dicts with `doc_id`, `court_name`, `file_number`, `date`, `ecli`,
   `type`, `snippet`.
4. Implement `lookup_fetch` — one upstream request, return a case dict
   identical in shape to what `iter_cases` yields (so `lookup ingest`
   can reuse the existing OLDP sink).
5. Add an entry to `_LOOKUP_PROVIDERS` in `cli_lookup.py`.
6. Tests: parser hermetic test + capability validation + at least one
   `lookup_search` exact-match path.
