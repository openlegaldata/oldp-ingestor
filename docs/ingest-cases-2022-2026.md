# Backfill Strategy: Cases Oct 2022 – Feb 2026

Production (de.openlegaldata.io) has not ingested new cases since October 2022.
Some providers may have failed even earlier. This document describes how to
identify the gaps, backfill missing cases, and set up continuous ingestion going
forward.

## Prerequisites

```bash
# 1. Environment pointing at production
export OLDP_API_URL=https://de.openlegaldata.io
export OLDP_API_TOKEN=<prod-token-with-cases:write>
# Optional — only if prod is behind HTTP Basic auth:
# export OLDP_API_HTTP_AUTH=user:password

# 2. EUR-Lex credentials (only for `eu` provider)
export EURLEX_USER=...
export EURLEX_PASSWORD=...

# 3. Working ingestor installation
cd /srv/openlegaldata/dev/oldp-ingestor-dev
make install
```

> **Important**: All cases created via the API are set to
> `review_status="pending"`. They will not be publicly visible until an admin
> approves them (see Phase 5).

---

## Implementation Changes Required

All required ingestor changes have been implemented on branch
`feat/prod-integration` (commit `e81e160`). The OLDP Django app changes
are in PR [openlegaldata/oldp#154](https://github.com/openlegaldata/oldp/pull/154).

### oldp-ingestor changes (all done)

#### 1. Write-side pacing + 429 retry for OLDP API — DONE

`OLDPClient` now has retry with exponential backoff on 429/503/ConnectionError
(same pattern as `HttpBaseClient`). A new `--write-delay` CLI flag (default
`0.0`) paces POST requests to the OLDP API. Use `--write-delay 0.1` during
backfill to avoid overwhelming the server.

**Changed files**: `client.py`, `cli.py`, `tests/test_client.py`, `tests/test_cli.py`

#### 2. Missing providers added to deployment scripts — DONE

`ingest-all.sh` now runs all 21 providers (was 17). `ingest.sh`
`DATE_PROVIDERS` now includes `ns`, `hb`, `sn-ovg`, `sn-verfgh`, `sn`.

**Changed files**: `dev-deployment/ingest-all.sh`, `dev-deployment/ingest.sh`

#### 3. NS provider client-side date filtering — DONE

`NsCaseProvider.get_cases()` now applies `_is_within_date_range()` after
parsing each case. The NI-VORIS search API still fetches all results (no
server-side date params available), but cases outside the date range are
skipped before being sent to the sink.

**Changed files**: `providers/base.py`, `providers/de/ns.py`, `tests/test_providers.py`

#### 4. Juris providers client-side date filtering — DONE

`JurisCaseProvider.get_cases()` now applies `_is_within_date_range()` after
parsing each case detail. All 10 subclasses inherit the fix. The Jetspeed
search URL still fetches `query=*`, but out-of-range cases are skipped
client-side.

**Changed files**: `providers/de/juris.py`, `tests/test_providers.py`

#### Shared: `_is_within_date_range()` on CaseProvider base

The date filtering method was added to `CaseProvider` in `providers/base.py`
along with `date_from`/`date_to` class attributes (default `""`). Any
provider can call `self._is_within_date_range(date_str)` — returns `True`
if no filters set or if the date is missing/unparseable (to avoid silently
dropping cases).

### OLDP Django app — PR open

API fixes are in [openlegaldata/oldp#154](https://github.com/openlegaldata/oldp/pull/154).
The existing API already provides the core features needed for backfill:

| Feature | Status | API parameter |
|---------|--------|---------------|
| Filter cases by state | Works | `court__state=<state_id>` |
| Filter cases by court | Works | `court=<court_id>` |
| Filter cases by date range | Works | `date_after`, `date_before` |
| Order by date | Works | `ordering=-date` |
| List states | Works | `GET /api/states/` |
| Case creation with dedup | Works | `POST /api/cases/` (409 on duplicate) |
| Authenticated rate limit | Not enforced | Only `AnonRateThrottle` is configured; `UserRateThrottle` is defined but not in `DEFAULT_THROTTLE_CLASSES` |

> **Note on the OLDP API filter parameter names**: The cases endpoint uses
> `court=<id>` (not `court_id=`). The `court__state=` parameter accepts a
> state integer PK. The `court` field in list responses is a nested object
> with `{id, name, slug, city, state, ...}` where `state` is an integer PK.

---

## Phase 0: Reconnaissance — Understand Current State

Before ingesting anything, query the production OLDP API to understand what data
already exists and where the gaps are.

### 0.1 Get total case count and date range

```bash
# Total cases in OLDP
curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
  "https://de.openlegaldata.io/api/cases/?limit=1" | jq '.count'

# Most recent case by date
curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
  "https://de.openlegaldata.io/api/cases/?ordering=-date&limit=1" \
  | jq '.results[0] | {date, file_number, court_name: .court.name}'

# Oldest case
curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
  "https://de.openlegaldata.io/api/cases/?ordering=date&limit=1" \
  | jq '.results[0] | {date, file_number, court_name: .court.name}'
```

### 0.2 Count cases per year (gap detection)

Check case counts year by year. A sudden drop or zero indicates a gap.

```bash
for year in 2018 2019 2020 2021 2022 2023 2024 2025 2026; do
  count=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?date_after=${year}-01-01&date_before=${year}-12-31&limit=1" \
    | jq '.count')
  echo "$year: $count cases"
done
```

### 0.3 Count cases per state (Bundesland)

Most providers map to a specific state. The OLDP API supports
`court__state=<state_id>` to filter cases by Bundesland (via
`Case → Court → State`). This directly reveals which states have gaps.

```bash
# First, get the list of states with their IDs
curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
  "https://de.openlegaldata.io/api/states/?format=json" \
  | jq '.results[] | {id, name, slug}'
```

Then count cases per state, split by before/after Oct 2022:

```bash
# Fetch state IDs and names
states=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
  "https://de.openlegaldata.io/api/states/?format=json&limit=50" \
  | jq -r '.results[] | "\(.id):\(.name)"')

echo "State | Total | Before 2022-10 | After 2022-10"
echo "------|-------|----------------|---------------"
for entry in $states; do
  sid="${entry%%:*}"
  sname="${entry#*:}"
  total=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?court__state=$sid&limit=1" | jq '.count')
  before=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?court__state=$sid&date_before=2022-10-01&limit=1" | jq '.count')
  after=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?court__state=$sid&date_after=2022-10-01&limit=1" | jq '.count')
  echo "$sname | $total | $before | $after"
done
```

Map the results to providers:

| State | Provider(s) | Notes |
|-------|-------------|-------|
| (Federal) | `ris`, `rii` | BGH, BVerfG, BVerwG, BFH, BAG, BSG, BPatG |
| Bayern | `by` | gesetze-bayern.de |
| Nordrhein-Westfalen | `nrw` | NRWE database |
| Niedersachsen | `ns` | NI-VORIS |
| Bremen | `hb` | 5 SixCMS court portals |
| Sachsen | `sn`, `sn-ovg`, `sn-verfgh` | 3 separate providers |
| Berlin / Brandenburg | `juris-bb` | gesetze.berlin.de |
| Hamburg | `juris-hh` | landesrecht-hamburg.de |
| Mecklenburg-Vorpommern | `juris-mv` | landesrecht-mv.de |
| Rheinland-Pfalz | `juris-rlp` | landesrecht.rlp.de |
| Sachsen-Anhalt | `juris-sa` | landesrecht.sachsen-anhalt.de |
| Schleswig-Holstein | `juris-sh` | gesetze-rechtsprechung.sh.juris.de |
| Baden-Württemberg | `juris-bw` | landesrecht-bw.de |
| Saarland | `juris-sl` | recht.saarland.de |
| Hessen | `juris-he` | lareda.hessenrecht.hessen.de |
| Thüringen | `juris-th` | landesrecht.thueringen.de |
| (EU) | `eu` | EUR-Lex (no state) |

A state with **zero cases after 2022-10** confirms the provider stopped.
A state with **suspiciously few cases before 2022-10** (compared to similar
states) suggests the provider was already failing earlier — backfill the
full range for that provider.

### 0.4 Count cases per court type (federal courts)

```bash
# Spot-check federal court counts (covered by ris + rii providers)
for court_type in BGH BVerfG BVerwG BFH BAG BSG BPatG; do
  court_id=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/courts/?code=$court_type&limit=1" \
    | jq '.results[0].id')
  total=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?court=$court_id&limit=1" \
    | jq '.count')
  after=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?court=$court_id&date_after=2022-10-01&limit=1" \
    | jq '.count')
  echo "$court_type (id=$court_id): $total total, $after after 2022-10"
done
```

### 0.6 Cross-reference with source volumes

Compare OLDP counts against known external source sizes:

| Provider | External source | How to estimate volume |
|----------|----------------|----------------------|
| `ris` | RIS API `/v1/case-law?size=1` | Check `view.totalItems` in response |
| `rii` | rechtsprechung-im-internet.de | Count listing pages per court (26/page) |
| `nrw` | nrwesuche.justiz.nrw.de | POST search with empty query, read total |
| `by` | gesetze-bayern.de | Browse listing pages |
| `eu` | EUR-Lex SPARQL | Run count query |

```bash
# Example: RIS total case count
curl -s "https://testphase.rechtsinformationen.bund.de/v1/case-law?size=1" \
  | jq '.view.totalItems'
```

### 0.7 Record findings

Create a spreadsheet or table with:

| Provider | Source total | OLDP total | OLDP after 2022-10 | OLDP before 2022-10 | Gap estimate |
|----------|------------|-----------|--------------------|--------------------|-------------|
| ris      |            |           |                    |                    |             |
| rii      |            |           |                    |                    |             |
| ...      |            |           |                    |                    |             |

This tells you:
- **Which providers have pre-2022 gaps** (source total >> OLDP total)
- **How many new cases to expect** per provider for the 2022-2026 window

---

## Phase 1: Dry Run with JSON Sink

Before writing to production, do a dry run using the `json-file` sink. This
validates that providers work, estimates volumes, and produces reviewable output.

```bash
mkdir -p /tmp/ingest-dryrun

# Example: RIS cases from Oct 2022 to today
oldp-ingestor -v --sink json-file --output-dir /tmp/ingest-dryrun/ris \
  cases --provider ris --date-from 2022-10-01

# Count output files
find /tmp/ingest-dryrun/ris/cases -name '*.json' | wc -l
```

Run this for a few providers to:
1. Confirm they still work (APIs may have changed in 3+ years)
2. Get realistic volume estimates
3. Spot-check data quality (court names, content)

---

## Phase 2: Backfill — HTTP/API Providers (Date-Filtered)

These providers support `--date-from` and fetch only cases within the window.
This is the most efficient approach.

### Run order and commands

```bash
cd /srv/openlegaldata/dev/dev-deployment
export OLDP_RESULTS_DIR=$(pwd)/results
mkdir -p "$OLDP_RESULTS_DIR"
```

**Providers with server-side date filtering** (truly incremental):

| # | Provider | Command | Est. time | Notes |
|---|----------|---------|-----------|-------|
| 1 | `ris` | `oldp-ingestor -v --results-dir results cases --provider ris --date-from 2022-10-01` | 1-4h | All federal courts. Can also filter by `--court BGH` etc. to split the load. |
| 2 | `nrw` | `oldp-ingestor -v --results-dir results cases --provider nrw --date-from 2022-10-01` | 1-3h | NRW courts. |
| 3 | `eu` | `oldp-ingestor -v --results-dir results cases --provider eu --date-from 2022-10-01` | 30min-2h | EUR-Lex SPARQL. Requires `EURLEX_USER`/`EURLEX_PASSWORD`. |
| 4 | `hb` | `oldp-ingestor -v --results-dir results cases --provider hb --date-from 2022-10-01` | 30min-1h | Bremen (5 court portals). |
| 5 | `sn-ovg` | `oldp-ingestor -v --results-dir results cases --provider sn-ovg --date-from 2022-10-01` | 15-30min | Sachsen OVG only. |
| 6 | `sn-verfgh` | `oldp-ingestor -v --results-dir results cases --provider sn-verfgh --date-from 2022-10-01` | 10-20min | Sachsen VerfGH only (small volume). |
| 7 | `sn` | `oldp-ingestor -v --results-dir results cases --provider sn --date-from 2022-10-01` | 30min-2h | Sachsen ordinary courts (Playwright). |
| 8 | `ns` | `oldp-ingestor -v --results-dir results cases --provider ns --date-from 2022-10-01` | 1-3h | Niedersachsen. Client-side date filtering (still fetches all pages from VORIS but skips out-of-range cases before POSTing). |

**If pre-2022 gaps were found in Phase 0**, extend `--date-from` further back
or omit it entirely to do a full fetch for that provider. The OLDP API's 409
dedup will skip existing cases.

### Splitting large providers by court or time window

If `ris` is too large for a single run, split by court type:

```bash
# Federal courts one by one
for court in BGH BVerfG BVerwG BFH BAG BSG BPatG; do
  oldp-ingestor -v --results-dir results \
    cases --provider ris --court $court --date-from 2022-10-01
done
```

Or split by time window (6-month chunks):

```bash
oldp-ingestor -v --results-dir results \
  cases --provider ris --date-from 2022-10-01 --date-to 2023-03-31
oldp-ingestor -v --results-dir results \
  cases --provider ris --date-from 2023-04-01 --date-to 2023-09-30
# ... continue in 6-month increments ...
oldp-ingestor -v --results-dir results \
  cases --provider ris --date-from 2025-07-01
```

---

## Phase 3: Backfill — Full-Fetch Providers (409 Dedup)

These providers do NOT support date filtering. They always fetch everything and
rely on the OLDP API returning 409 Conflict for duplicates.

| # | Provider | Command | Notes |
|---|----------|---------|-------|
| 1 | `rii` | `oldp-ingestor -v --results-dir results cases --provider rii` | 7 federal courts (BVerfG, BGH, etc.). Full XML download. |
| 2 | `by` | `oldp-ingestor -v --results-dir results cases --provider by` | Bavaria. Full XML download. |

These will produce many 409 "skipped" entries (existing cases). That is
expected and harmless — just slower because every existing case still triggers
an API POST.

**Tip**: Run `rii` even though `ris` covers the same federal courts. RII
provides full XML content while RIS may have incomplete HTML. The OLDP API
deduplicates by court + file number, so only genuinely new cases are created.

---

## Phase 4: Backfill — Playwright/Juris Providers

These 10 juris-hosted state portals require a Playwright browser. They are
slower and more resource-intensive.

The juris providers now have client-side date filtering: the search URL still
fetches `query=*` (all results), but `--date-from` / `--date-to` cause
out-of-range cases to be skipped before POSTing to OLDP. This avoids
unnecessary 409 API calls for pre-2022 cases during backfill.

```bash
# Run one at a time (each launches a browser)
for provider in juris-bb juris-hh juris-mv juris-rlp juris-sa \
                juris-sh juris-bw juris-sl juris-he juris-th; do
  echo "=== Running $provider ==="
  oldp-ingestor -v --results-dir results cases --provider $provider \
    --date-from 2022-10-01
  echo "=== Done $provider ==="
  sleep 10  # brief pause between providers
done
```

### Notes on juris providers

- **Client-side filtering**: The search still downloads all results from the
  portal (`query=*`), but cases with dates outside `--date-from`/`--date-to`
  are skipped before being sent to the OLDP API.
- **Runtime**: 30min-2h per portal depending on volume.
- **Rate**: Default 0.5s delay between pages. Do not lower this.
- **Browser deps**: Ensure `playwright install chromium` has been run.

---

## Phase 5: Verify Results and Approve Cases

### 5.1 Check ingestor results

```bash
# Status dashboard
oldp-ingestor status --results-dir results

# Or check individual result files
cat results/cases_ris.json | jq '{status, created, skipped, errors}'
```

### 5.2 Analyze court resolution errors

If the ingestor logs show "Could not resolve court from name" errors, use the
built-in analysis tool:

```bash
# Assuming you piped ingestor output to a log file
oldp-ingestor -v --results-dir results cases --provider ris \
  --date-from 2022-10-01 2>&1 | tee logs/ris-backfill.log

# Analyze missing courts
oldp-ingestor analyze-courts --input logs/ris-backfill.log
```

This cross-references missing court names against the OLDP courts/cities/states
database and suggests matches. Courts that are genuinely missing from OLDP need
to be created by an admin before those cases can be ingested.

### 5.3 Approve pending cases in OLDP admin

All API-created cases land with `review_status="pending"`. To make them public:

1. Go to **Django Admin > Cases > Cases**
2. Filter by **Review status: pending**
3. Optionally filter by **created_by_token** to see only ingestor submissions
4. Spot-check a sample for data quality
5. Bulk-approve by selecting cases and using the admin action

Alternatively, if you trust the ingestor output after spot-checking, ask a
Django admin to run a bulk approval:

```python
# Django shell on the production server
from oldp.apps.cases.models import Case
pending = Case.objects.filter(review_status="pending")
print(f"Approving {pending.count()} cases...")
pending.update(review_status="accepted")
```

### 5.4 Verify gap closure

Re-run the Phase 0 reconnaissance queries to confirm the gaps are filled:

```bash
for year in 2022 2023 2024 2025 2026; do
  count=$(curl -s -H "Authorization: Token $OLDP_API_TOKEN" \
    "https://de.openlegaldata.io/api/cases/?date_after=${year}-01-01&date_before=${year}-12-31&limit=1" \
    | jq '.count')
  echo "$year: $count cases"
done
```

---

## Phase 6: Set Up Continuous Ingestion

After the backfill, set up cron jobs so gaps don't accumulate again.

### 6.1 Initialize state files

After a successful backfill, create state files so `ingest.sh` knows the
last-run date and future runs are incremental:

```bash
cd /srv/openlegaldata/dev/dev-deployment

# Set today's date as the baseline for all date-supporting providers
for provider in ris nrw ns eu hb sn-ovg sn-verfgh sn \
               juris-bb juris-hh juris-mv juris-rlp juris-sa \
               juris-sh juris-bw juris-sl juris-he juris-th; do
  echo "2026-02-25" > ".ingest-state-cases-$provider"
done
```

### 6.2 Install crontab

Use the provided schedule as a starting point:

```cron
# Daily 02:00-04:30: HTTP/API providers
0  2 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases ris     >> logs/ris.log 2>&1
15 2 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases rii     >> logs/rii.log 2>&1
45 2 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases by      >> logs/by.log 2>&1
15 3 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases nrw     >> logs/nrw.log 2>&1
30 3 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases ns      >> logs/ns.log 2>&1
45 3 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases eu      >> logs/eu.log 2>&1
0  4 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases hb      >> logs/hb.log 2>&1
15 4 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases sn-ovg  >> logs/sn-ovg.log 2>&1
20 4 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases sn-verfgh >> logs/sn-verfgh.log 2>&1
30 4 * * *   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases sn      >> logs/sn.log 2>&1

# Monday 05:00-07:30: Playwright batch 1
0  5 * * 1   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-bb  >> logs/juris-bb.log 2>&1
30 5 * * 1   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-hh  >> logs/juris-hh.log 2>&1
0  6 * * 1   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-mv  >> logs/juris-mv.log 2>&1
30 6 * * 1   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-rlp >> logs/juris-rlp.log 2>&1
0  7 * * 1   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-sa  >> logs/juris-sa.log 2>&1

# Thursday 05:00-07:30: Playwright batch 2
0  5 * * 4   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-sh  >> logs/juris-sh.log 2>&1
30 5 * * 4   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-bw  >> logs/juris-bw.log 2>&1
0  6 * * 4   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-sl  >> logs/juris-sl.log 2>&1
30 6 * * 4   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-he  >> logs/juris-he.log 2>&1
0  7 * * 4   cd /srv/openlegaldata/dev/dev-deployment && bash ingest.sh cases juris-th  >> logs/juris-th.log 2>&1

# Weekly status check (Sunday 08:00)
0  8 * * 0   cd /srv/openlegaldata/dev/dev-deployment && oldp-ingestor status --results-dir results --stale-hours 168 >> logs/status.log 2>&1
```

### 6.3 Monitor

```bash
# Weekly health check
oldp-ingestor status --results-dir results --stale-hours 168

# Exit code 0 = all healthy, 1 = stale/failed/never-run providers
```

---

## Troubleshooting

### Provider API has changed

If a provider fails immediately, the external API may have changed its format
or URL since 2022. Check the provider documentation under `docs/providers/de/`
and update the provider code if needed. Use `--limit 5` for quick testing:

```bash
oldp-ingestor -v cases --provider ris --date-from 2025-01-01 --limit 5
```

### OLDP server overload

Although authenticated requests are not rate-limited by DRF (only
`AnonRateThrottle` is configured), a tight POST loop can still overwhelm the
server — each case POST triggers court resolution, reference extraction, and
database writes. If you see 500 errors or timeouts, increase `--write-delay`
(e.g. `--write-delay 0.2`) or reduce the batch size with `--limit`. The
`OLDPClient` also automatically retries on 429/503 with exponential backoff.

### Court resolution failures

Cases where the court name cannot be resolved to an existing OLDP court are
rejected with 400. These courts need to be created first:

```bash
# Option 1: Create courts via API (requires courts:write permission)
curl -X POST "https://de.openlegaldata.io/api/courts/" \
  -H "Authorization: Token $OLDP_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Amtsgericht Neustadt", "code": "AGNEUSTADT", "court_type": "AG", "state_name": "Rheinland-Pfalz"}'

# Option 2: Use analyze-courts to identify needed courts
oldp-ingestor analyze-courts --input logs/backfill.log
```

### Playwright browser issues

```bash
# Install browser
cd /srv/openlegaldata/dev/oldp-ingestor-dev
.venv/bin/playwright install chromium

# Test one provider with limit
oldp-ingestor -v cases --provider juris-bb --limit 3
```

---

## Summary: Execution Order

| Step | What | Duration estimate |
|------|------|-------------------|
| **Phase 0** | Reconnaissance — query OLDP, count gaps | 30 min manual |
| **Phase 1** | Dry run 2-3 providers with `--sink json-file` | 1-2h |
| **Phase 2** | Backfill date-filtered providers (ris, nrw, eu, hb, sn-*, ns) | 4-14h |
| **Phase 3** | Backfill full-fetch providers (rii, by) | 1-4h |
| **Phase 4** | Backfill Playwright/juris providers (10 portals, with date filter) | 5-20h |
| **Phase 5** | Verify results, fix court errors, approve cases | 1-2h manual |
| **Phase 6** | Set up cron for continuous ingestion | 30 min |

**Total estimated wall-clock time**: 1-3 days (phases 2-4 can run sequentially
overnight).

> **Note on pre-2022 gaps**: If Phase 0 reveals gaps before October 2022
> (e.g., a provider that was never successfully run, or a year with
> suspiciously low case counts), run the affected providers without
> `--date-from` or with an earlier date. The 409 dedup ensures no duplicates
> are created, so it is always safe to re-fetch a wider window than strictly
> necessary.
