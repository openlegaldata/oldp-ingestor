"""Provider for EU legislation from EUR-Lex (regulations + directives).

Fetches German-language EU laws from the CELLAR REST API (with EUR-Lex
HTML as fallback) and emits them as ``LawBook`` + ``Law`` dicts for
the OLDP REST API. Designed to plug the DSGVO-shaped gap reported in
the MCP audit (`dev-deployment/mcp-issues.md` #18) where case search
returns hundreds of cases citing DSGVO but ``validate_citation``
cannot resolve any EU-regulation reference.

Two discovery modes:
  * **seed** (default) — a curated list of high-value EU instruments
    with their canonical German short codes (``DSGVO``, ``DSA``,
    ``KI-VO`` …) so the ``book_code`` matches the form German lawyers
    actually cite.
  * **sparql** (``--discover``) — placeholder hook for later: query
    Cellar for in-force regulations + directives. Logs a warning and
    falls back to the seed list for now so the CLI surface is final
    from day one.

The actual HTTP/SPARQL/AWS-WAF resilience pattern is borrowed from the
sibling :mod:`oldp_ingestor.providers.de.eu` case provider (constants
imported directly; helpers re-implemented locally to keep the case
provider's freshly-stabilised code untouched). A future
``eurlex_common.py`` extraction is on the TODO list once both
providers stabilise.
"""

from __future__ import annotations

import logging
import re
from typing import Iterator

import lxml.html
from lxml import etree

from oldp_ingestor.providers.base import LawProvider
from oldp_ingestor.providers.de.eu import (
    CELLAR_BASE_URL,
    EURLEX_BASE_URL,
    EURLEX_MIN_CONTENT_LEN,
)
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

# Curated seed list of high-value EU instruments. The keys are CELEX
# numbers (sector 3 = legislation; sector-letter encodes the
# document type — R = Regulation, L = Directive). The values are the
# tuples ``(book_code, fallback_title)`` where ``book_code`` is the
# colloquial German short name lawyers cite ("Art. 15 DSGVO", "Art. 5
# DSA", …) and ``fallback_title`` is used only if the document's own
# title block can't be parsed for some reason.
#
# Order matters: ``--limit N`` takes the first N entries, and the
# audit (mcp-issues.md #18) calls out DSGVO as the must-have entry, so
# it sits at the top.
EU_SEED_BOOKS: dict[str, tuple[str, str]] = {
    "32016R0679": ("DSGVO", "Datenschutz-Grundverordnung"),
    "32022R2065": ("DSA", "Digital Services Act"),
    "32022R1925": ("DMA", "Digital Markets Act"),
    "32024R1689": ("KI-VO", "KI-Verordnung"),
    "32014R0910": ("eIDAS-VO", "eIDAS-Verordnung"),
    # Note: 32000L0031 (e-Commerce-RL) and 32002L0058 (ePrivacy-RL) were
    # in the original seed list but their DE Cellar manifestations
    # 404 and EUR-Lex's HTML render returns a perpetual 202. Swapped
    # in two newer instruments with stable DE Cellar XHTML:
    "32016L0680": ("JI-RL", "Datenschutz-Richtlinie für Polizei und Justiz (JI-RL)"),
    "32012R1215": ("Brüssel-Ia-VO", "Brüssel-Ia-Verordnung (EuGVVO)"),
    "32008R0593": ("Rom-I-VO", "Rom-I-Verordnung"),
    "32007R0864": ("Rom-II-VO", "Rom-II-Verordnung"),
    "32019L0790": ("DSM-RL", "DSM-Richtlinie (Urheberrecht im digitalen Binnenmarkt)"),
}

# Cellar prefers ``Accept: application/xhtml+xml`` for German full-text
# XHTML; ``text/html;type=branch`` returns 400 against the same URL
# (verified 2026-05-27). The EUR-Lex direct page returns 202 (async
# preparation) under load and is therefore the *fallback* here, not
# the primary as it is in the case provider.
_CELLAR_LANG_PARAM = "?language=DEU"
_CELLAR_HEADERS = {
    "Accept": "application/xhtml+xml",
    "Accept-Language": "deu",
}

# German month names → ISO month number for revision_date parsing.
_DE_MONTHS = {
    "Januar": "01",
    "Februar": "02",
    "März": "03",
    "April": "04",
    "Mai": "05",
    "Juni": "06",
    "Juli": "07",
    "August": "08",
    "September": "09",
    "Oktober": "10",
    "November": "11",
    "Dezember": "12",
}
_DE_DATE_RE = re.compile(
    r"vom\s+(\d{1,2})\.\s*(" + "|".join(_DE_MONTHS) + r")\s+(\d{4})"
)


class EurLexLawProvider(ScraperBaseClient, LawProvider):
    """Fetches EU legislation (regulations + directives) from EUR-Lex/Cellar.

    Args:
        celex_numbers: Override the seed list with an explicit
            collection of CELEX numbers. Empty/``None`` means use the
            seed list.
        discover: When ``True``, attempt SPARQL-based discovery of
            in-force EU instruments. Currently a no-op stub that logs
            a warning and falls back to the seed list — wired so the
            CLI flag is final without committing the implementer to
            ship discovery in the same change.
        limit: Cap on the number of books processed (testing aid,
            also used by the e2e ingest target of 10).
        request_delay: Inter-request delay in seconds; default
            matches the rest of the codebase.
        proxy: Optional HTTP/SOCKS5 proxy URL.
    """

    SOURCE = {
        "name": "EUR-Lex (Cellar)",
        "homepage": "https://eur-lex.europa.eu/",
    }

    def __init__(
        self,
        celex_numbers: list[str] | None = None,
        discover: bool = False,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(
            base_url=CELLAR_BASE_URL, request_delay=request_delay, proxy=proxy
        )
        self.discover = discover
        self.limit = limit

        if celex_numbers:
            self._celex_numbers: list[str] = [c.strip() for c in celex_numbers if c]
        elif discover:
            # Discovery stub — keeps the CLI surface final without
            # committing to ship SPARQL discovery in this change. When
            # implemented, _discover_celex_numbers() will run a SPARQL
            # query against CELLAR's regulation+directive predicates
            # and rank by document date.
            discovered = self._discover_celex_numbers()
            self._celex_numbers = discovered or list(EU_SEED_BOOKS.keys())
        else:
            self._celex_numbers = list(EU_SEED_BOOKS.keys())

        if self.limit:
            self._celex_numbers = self._celex_numbers[: self.limit]

        # get_law_books streams; get_laws looks up the parsed article
        # set by book_code so the CLI's two-pass (book then laws) hits
        # the cache rather than re-fetching the same Cellar URL twice.
        self._articles_by_book: dict[str, list[dict]] = {}

    # ------------------------------------------------------------------ #
    # Discovery placeholder
    # ------------------------------------------------------------------ #

    def _discover_celex_numbers(self) -> list[str]:
        """Placeholder for SPARQL-based discovery.

        Returns an empty list to signal "no discoveries"; the caller
        falls back to the curated seed list. The intended
        implementation queries CELLAR for ``?work cdm:resource_legal_in-force
        true`` filtered to ``cdm:regulation`` and ``cdm:directive`` work
        types, ordered by ``cdm:work_date_document`` DESC. Following
        the resilience pattern from
        :meth:`oldp_ingestor.providers.de.eu.EuCaseProvider._search_eclis`,
        connection drops mid-paging should be treated as end-of-run
        rather than crashing the whole ingest.
        """
        logger.warning(
            "EurLexLawProvider: --discover is currently a stub; "
            "falling back to the curated seed list"
        )
        return []

    # ------------------------------------------------------------------ #
    # Fetch + parse
    # ------------------------------------------------------------------ #

    def _fetch_xhtml(self, celex: str) -> str | None:
        """Fetch a CELEX document as German XHTML via Cellar.

        Tries the Cellar resource URL first (returns the full document
        body as ``application/xhtml+xml``), falls back to the EUR-Lex
        direct page if Cellar 404s. AWS-WAF and 202 "still preparing"
        responses are treated as transient and short-circuit to
        ``None`` so the operator can re-run cron later.
        """
        # Strip _RES suffix — CELEX variants with this suffix always
        # 404 on both Cellar and EUR-Lex (see eu.py:337).
        base_celex = re.sub(r"_RES$", "", celex)

        cellar_url = (
            f"{CELLAR_BASE_URL}/resource/celex/{base_celex}{_CELLAR_LANG_PARAM}"
        )
        try:
            resp = self._get(cellar_url, headers=_CELLAR_HEADERS)
        except Exception as exc:
            logger.warning(
                "Cellar fetch failed for %s: %s — trying EUR-Lex fallback",
                base_celex,
                exc,
            )
            resp = None

        if resp is not None and resp.status_code == 200:
            return resp.text

        # Fall back to EUR-Lex's legal-content endpoint. This often
        # returns 202 ("still being prepared") under load — treat as
        # transient and re-fetch on the next cron run.
        eurlex_url = (
            f"{EURLEX_BASE_URL}/legal-content/DE/TXT/HTML/?uri=CELEX:{base_celex}"
        )
        try:
            resp = self._get(
                eurlex_url, headers={"Accept": "text/html", "Accept-Language": "de"}
            )
        except Exception as exc:
            logger.warning("EUR-Lex fallback failed for %s: %s", base_celex, exc)
            return None

        if resp.status_code == 202:
            logger.warning(
                "EUR-Lex returned 202 (still preparing) for %s — retry next run",
                base_celex,
            )
            return None

        if resp.status_code != 200:
            logger.warning(
                "EUR-Lex fallback returned %d for %s", resp.status_code, base_celex
            )
            return None

        if "aws-waf-token" in resp.text:
            logger.warning("WAF challenge detected for %s — retry next run", base_celex)
            return None

        if len(resp.text) < EURLEX_MIN_CONTENT_LEN:
            logger.warning(
                "EUR-Lex returned suspiciously short body for %s (%d chars)",
                base_celex,
                len(resp.text),
            )
            return None

        return resp.text

    def _parse_document(
        self, html_text: str, book_code: str, fallback_title: str
    ) -> tuple[dict, list[dict]] | None:
        """Parse a Cellar XHTML document into (book, articles).

        Returns ``None`` if no articles could be extracted (a clear
        signal that the document layout differs from the expected
        ``oj-ti-art`` convention and a human should look).
        """
        # Strip <?xml ...?> preamble if present (lxml.html dislikes it on str).
        cleaned = re.sub(r"<\?xml[^?]*\?>", "", html_text)
        try:
            root = lxml.html.fromstring(cleaned)
        except etree.ParserError as exc:
            logger.warning("Failed to parse %s XHTML: %s", book_code, exc)
            return None

        title = self._extract_document_title(root, fallback_title)
        revision_date = self._extract_revision_date(root)

        articles = self._extract_articles(root)
        if not articles:
            logger.warning(
                "No articles found in %s — page layout may have changed", book_code
            )
            return None

        book = {
            "code": book_code,
            "title": title,
            "revision_date": revision_date,
        }
        return book, articles

    @staticmethod
    def _extract_document_title(root, fallback: str) -> str:
        """Concatenate the ``<p class="oj-doc-ti">`` elements under
        ``<div class="eli-main-title">``.

        EUR-Lex splits the full title across 3–4 paragraphs (act type +
        date + subject + optional EWR notice). We join them with " — "
        so the result is identifiable in a list_law_books search but
        not so verbose it overflows the 250-char API limit. The CLI
        truncates to 250 if we still overshoot.
        """
        parts = []
        for p in root.xpath(
            '//div[contains(@class, "eli-main-title")]//p[contains(@class, "oj-doc-ti")]'
        ):
            text = " ".join(p.text_content().split())
            if text:
                parts.append(text)
        if not parts:
            return fallback
        return " — ".join(parts)

    @staticmethod
    def _extract_revision_date(root) -> str:
        """Parse 'vom 27. April 2016' from the title block into YYYY-MM-DD.

        Returns an empty string when no date can be parsed — the CLI
        will then drop the field; OLDP's LawBook revision_date is
        optional at create time when the latest revision suffices.
        """
        for p in root.xpath(
            '//div[contains(@class, "eli-main-title")]//p[contains(@class, "oj-doc-ti")]'
        ):
            text = " ".join(p.text_content().split())
            m = _DE_DATE_RE.search(text)
            if m:
                day, month_de, year = m.group(1), m.group(2), m.group(3)
                return f"{year}-{_DE_MONTHS[month_de]}-{int(day):02d}"
        return ""

    @staticmethod
    def _extract_articles(root) -> list[dict]:
        """Walk the document and emit one dict per ``<div class="eli-subdivision" id="art_*">``.

        Each EU regulation/directive wraps every article in a
        ``<div class="eli-subdivision" id="art_N">``; inside that:

          ``<p class="oj-ti-art">Artikel N</p>``
          ``<div class="eli-title"><p class="oj-sti-art">Subtitle</p></div>``
          (then per-paragraph ``<div id="N.NNN"><p class="oj-normal">...</p></div>``
          blocks, optional tables, etc.)

        We emit ``section="Art. N"`` to match the German citation
        convention (matches Art 1 GG in OLDP's existing corpus) and
        serialise the article's inner HTML — minus the redundant
        title/subtitle paragraphs — as ``content``.
        """
        articles: list[dict] = []

        for art_div in root.xpath(
            '//div[starts-with(@id, "art_") and contains(@class, "eli-subdivision")]'
        ):
            art_id = art_div.get("id", "")
            # id like "art_15" -> "15"; skip the few non-article art_*
            # ids EUR-Lex occasionally uses (e.g. "art_annex").
            num_match = re.match(r"art_(\d+[a-z]?)$", art_id)
            if not num_match:
                continue
            number = num_match.group(1)
            section = f"Art. {number}"

            # Title paragraph
            ti_arts = art_div.xpath('./p[contains(@class, "oj-ti-art")]')
            sti_arts = art_div.xpath('.//p[contains(@class, "oj-sti-art")]')
            title_text = (
                " ".join(sti_arts[0].text_content().split())
                if sti_arts
                else (
                    " ".join(ti_arts[0].text_content().split()) if ti_arts else section
                )
            )

            # Build content as a copy of the article div with the
            # ti-art header and the sti-art wrapper dropped — the API
            # already stores section + title separately.
            content_root = etree.Element("div")
            for child in art_div.iterchildren():
                # Skip the article-number header
                if child.tag == "p" and "oj-ti-art" in (child.get("class") or ""):
                    continue
                # Skip the subtitle wrapper
                if child.tag == "div" and "eli-title" in (child.get("class") or ""):
                    continue
                content_root.append(etree.fromstring(etree.tostring(child)))

            content = etree.tostring(content_root, encoding="unicode")

            articles.append(
                {
                    "section": section,
                    "title": title_text,
                    "content": content,
                    "order": _article_order_key(number),
                }
            )

        return articles

    # ------------------------------------------------------------------ #
    # LawProvider API
    # ------------------------------------------------------------------ #

    def iter_law_books(self) -> Iterator[dict]:
        """Yield each EU book in turn, caching the parsed articles.

        ``get_laws`` looks up the parsed article list by ``book_code``
        — we cache it during this pass so the second call doesn't
        re-fetch the same Cellar URL.
        """
        for celex in self._celex_numbers:
            seed = EU_SEED_BOOKS.get(celex)
            if seed is None:
                # Allow CLI-supplied --celex values that aren't in the
                # seed list — fall back to the CELEX as code so the
                # book still ingests; operator can rename later.
                book_code = celex
                fallback_title = celex
            else:
                book_code, fallback_title = seed

            html = self._fetch_xhtml(celex)
            if html is None:
                logger.warning("Skipping %s: no document content available", celex)
                continue

            parsed = self._parse_document(html, book_code, fallback_title)
            if parsed is None:
                continue

            book, articles = parsed
            self._articles_by_book[book_code] = articles
            yield book

    def get_law_books(self) -> list[dict]:
        return list(self.iter_law_books())

    def get_laws(self, book_code: str, revision_date: str) -> list[dict]:
        articles = self._articles_by_book.get(book_code, [])
        # Stamp book_code (required by the API serializer) and
        # revision_date (so re-runs land on the same LawBook revision;
        # the CLI uses (code, revision_date) as the idempotency key).
        for art in articles:
            art.setdefault("book_code", book_code)
            if revision_date:
                art.setdefault("revision_date", revision_date)
        return articles


def _article_order_key(number: str) -> int:
    """Map an article identifier to a sort order.

    Articles in EU regulations are numbered 1, 2, …, with occasional
    bis/ter additions written as "5a", "5b". The trailing letter
    sorts immediately after the integer, so we map "5" → 50, "5a"
    → 51, "5b" → 52. That keeps "Art. 5a" between "Art. 5" and "Art.
    6" in the list_law_books order.
    """
    m = re.match(r"(\d+)([a-z]?)", number)
    if not m:
        return 0
    base = int(m.group(1)) * 10
    suffix = m.group(2)
    return base + (ord(suffix) - ord("a") + 1 if suffix else 0)
