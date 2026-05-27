# EUR-Lex Law Provider

Fetches EU legislation (regulations + directives) from EUR-Lex /
Cellar in German and emits one `LawBook` + N `Law` rows per
instrument. Sibling to the existing `EuCaseProvider`
([eu.md](eu.md)) for case law.

## Motivation

OLDP's law corpus came from `gesetze-im-internet.de` only, leaving EU
regulations entirely missing — but case search returns hundreds of
decisions citing DSGVO, DSA, KI-VO, etc. The MCP audit
(`dev-deployment/mcp-issues.md` #18) flagged this as a "half-broken"
user experience: `search_cases("Auskunftsanspruch DSGVO")` returns
results but `validate_citation("Art. 15 DSGVO")` says
*"Law book 'DSGVO' not found"*. This provider closes that gap.

## Authentication

None. The Cellar REST endpoint and EUR-Lex legal-content endpoint are
both public. The `EURLEX_USER` / `EURLEX_PASSWORD` env vars (used by
nothing now — kept for CLI compat on the case provider) are not
referenced by this provider.

## Data Pipeline

Per CELEX number:

1. **Cellar (primary)** — `GET https://publications.europa.eu/resource/celex/{CELEX}?language=DEU`
   with `Accept: application/xhtml+xml`. Returns a single ~860 KB
   document for DSGVO. Other Accept values (text/html, text/html;type=branch)
   tested and rejected by Cellar (400/406).
2. **EUR-Lex (fallback)** — `GET https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=CELEX:{CELEX}`
   when Cellar 404s. EUR-Lex's WAF currently returns **202** ("still
   being prepared") under load; this is treated as a transient failure
   and the operator's next cron run picks up the book.
3. **Parse** — lxml.html walks the document looking for
   `<div class="eli-subdivision" id="art_N">` blocks. Each becomes
   one `Law` with `section="Art. N"` (matching the German convention
   already used by GG `Art 1`).

## Discovery Modes

| Mode | Trigger | Behaviour |
|---|---|---|
| Seed list (default) | no `--celex`, no `--discover` | Uses the curated `EU_SEED_BOOKS` dict (10 entries) |
| Manual override | `--celex 32016R0679,32022R2065` | Ingests exactly the supplied CELEX numbers; book_code defaults to the CELEX when not in the seed list |
| SPARQL discovery | `--discover` | **Stub**: logs a warning and falls back to the seed list. Wired in the CLI so the flag is final; full implementation is on the roadmap once the seed flow stabilises. |

The curated seed list (CELEX → German short code):

| CELEX | book_code | What |
|---|---|---|
| `32016R0679` | `DSGVO` | Datenschutz-Grundverordnung (GDPR) |
| `32022R2065` | `DSA` | Digital Services Act |
| `32022R1925` | `DMA` | Digital Markets Act |
| `32024R1689` | `KI-VO` | KI-Verordnung (AI Act) |
| `32014R0910` | `eIDAS-VO` | eIDAS-Verordnung |
| `32000L0031` | `e-Commerce-RL` | E-Commerce-Richtlinie |
| `32012R1215` | `Brüssel-Ia-VO` | EuGVVO (Brussels Ia) |
| `32008R0593` | `Rom-I-VO` | Rom-I-Verordnung |
| `32007R0864` | `Rom-II-VO` | Rom-II-Verordnung |
| `32002L0058` | `ePrivacy-RL` | ePrivacy-Richtlinie |

The order matters: `--limit N` slices from the top, so a `--limit 1`
run lands DSGVO (the audit driver).

## Field Mappings (Cellar XHTML → OLDP)

### LawBook

| Source | OLDP | Notes |
|---|---|---|
| `EU_SEED_BOOKS[celex][0]` (curated) | `code` | "DSGVO", "DSA", … |
| `<p class="oj-doc-ti">` (joined with " — ") | `title` | Full title; CLI truncates to 250 chars |
| `vom DD. Month YYYY` regex on title block | `revision_date` | Empty when missing — OLDP uses the latest revision |

### Law

| Source | OLDP | Notes |
|---|---|---|
| `<div class="eli-subdivision" id="art_N">` | one Law per div | Non-article subdivisions (Anhang, headers) filtered by regex on the id |
| `art_N` id | `section` = `Art. N` | German convention; matches Art 1 GG |
| `<p class="oj-sti-art">` | `title` | Article subtitle; falls back to article number |
| Inner HTML minus the article-number `<p>` and the subtitle wrapper | `content` | Paragraph numbering preserved so refex can resolve `Art. 15 Abs. 3` |
| Numeric part of `art_N`, with bis/ter sort tiebreak | `order` | `Art. 5a` sorts between `Art. 5` and `Art. 6` |

## Known Quirks

- **Cellar Accept header is finicky.** Only `application/xhtml+xml`
  produces a 200; `text/html;type=branch` (in the EUR-Lex docs)
  returns 400 against the same URL. Confirmed 2026-05-27.
- **EUR-Lex 202 is the common case under load.** The case provider
  uses EUR-Lex as primary; this provider flips the priority because
  the Cellar path is far more reliable for legislation. EUR-Lex stays
  as a fallback for the rare case where Cellar 404s.
- **AWS WAF detection.** A 200 body containing `aws-waf-token` is the
  challenge page, not the document. Treated as transient
  (same convention as `eu.py`).
- **`_RES` suffix.** SPARQL discovery returns occasional CELEX
  variants with `_RES` suffix which always 404. The provider strips
  the suffix before constructing URLs — same fix as `eu.py:337`.
- **Non-article subdivisions.** EUR-Lex wraps annexes and headers in
  `<div class="eli-subdivision">` too. A strict regex on the id
  (`art_\d+[a-z]?$`) skips them so they don't pollute the Law table.

## Usage

```bash
# Ingest the curated set of 10 EU books into the configured OLDP:
oldp-ingestor -v laws --provider eurlex --limit 10 \
  --user-agent-name oldp-dev --user-agent-contact you@example.com

# Just DSGVO:
oldp-ingestor -v laws --provider eurlex --celex 32016R0679 \
  --user-agent-name oldp-dev --user-agent-contact you@example.com

# Via deployment script:
bash dev-deployment/ingest.sh laws eurlex
```

## Local-dev approval workflow

API-created LawBooks land as `review_status="pending"`, which the MCP
layer filters out. For local-dev e2e verification, run the helper:

```bash
# Dry-run:
python dev-deployment/scripts/accept_eu_law_books.py
# Apply:
python dev-deployment/scripts/accept_eu_law_books.py --apply
# Then rebuild the local ES index so search_laws picks them up:
podman exec oldp-app-1 python manage.py rebuild_index --noinput
```

Production approval stays human-driven through the admin UI; the
helper is **local-dev only**.

## Future work

- Implement the SPARQL discovery path (`--discover`) so freshly-published
  EU instruments get picked up without seed-list edits.
- Extract a shared `eurlex_common.py` once both `eu.py` (cases) and
  `eurlex_laws.py` (this provider) stabilise. Shared surface today:
  Cellar/EUR-Lex URL constants, AWS-WAF detection, `_RES` suffix
  handling, transient-vs-permanent classification.
