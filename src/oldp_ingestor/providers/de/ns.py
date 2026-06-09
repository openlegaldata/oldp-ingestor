"""Provider for voris.wolterskluwer-online.de (Lower Saxony / Niedersachsen).

Fetches case law from the Niedersachsen VORIS portal (Wolters Kluwer Drupal site).
Data format: server-rendered HTML pages, GET-based search with query params.
"""

import logging
import re

import lxml.html
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.lookup import LookupCapability, LookupMixin
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

NS_BASE_URL = "https://voris.wolterskluwer-online.de"
NS_SEARCH_PATH = "/search"
NS_PER_PAGE = 12  # fixed by the portal
NS_MAX_PAGE = 5000

# OLDP state id for Niedersachsen (matches the portal's jurisdiction).
_NS_STATE_ID = 11

# Pattern matching ``<a href="/browse/document/<uuid>" hreflang="...">
# <court>, <date> - <AZ> [- <title>]</a>`` in the search-result HTML.
# The full label gives the agent enough metadata to judge a candidate
# without an extra fetch.
_NS_RESULT_LINK_RE = re.compile(
    r'<h3><a href="(/browse/document/[a-f0-9-]{36})"[^>]*>([^<]+)</a></h3>'
)
_NS_LABEL_RE = re.compile(
    r"^(?P<court>[^,]+),\s*(?P<date>\d{1,2}\.\d{1,2}\.\d{4})\s*-\s*"
    r"(?P<az>[^-\n]+?)(?:\s*-\s*(?P<title>.*))?$"
)


class NsCaseProvider(LookupMixin, ScraperBaseClient, CaseProvider):
    """Fetches case law from voris.wolterskluwer-online.de (Niedersachsen).

    Uses GET-based search with query parameters. The site is a Drupal 11
    SSR application (no JavaScript rendering needed).

    Supports server-side date filtering via `date` and `end_date_range`
    GET parameters (YYYY-MM-DD format).

    Args:
        date_from: Optional start date filter (YYYY-MM-DD).
        date_to: Optional end date filter (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "NI-VORIS Niedersachsen",
        "homepage": "https://voris.wolterskluwer-online.de",
    }

    LOOKUP_CAPABILITY = LookupCapability(
        keys=("file_number",),
        court_filter={"state_ids": [_NS_STATE_ID]},
        cost="medium",
    )

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(base_url=NS_BASE_URL, request_delay=request_delay, proxy=proxy)
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _search_page(self, page: int) -> list[str]:
        """GET search page and extract document UUIDs from results.

        Returns list of document paths like '/browse/document/<uuid>'.
        Results are sorted date_desc; date filtering happens client-side
        since VORIS ignores the date= param and returns 404 when
        combined with end_date_range.
        """
        params = {
            "query": "*",
            "publicationtype": "publicationform-ats-filter!ATS_Rechtsprechung",
            "sort_order": "date_desc",
            "page": str(page),
        }

        resp = self._get(NS_SEARCH_PATH, params=params)

        links = []
        seen = set()
        for match in re.finditer(r"/browse/document/([a-f0-9-]{36})", resp.text):
            path = match.group(0)
            if path not in seen:
                seen.add(path)
                links.append(path)

        return links

    def _get_field_value(self, dl_element, field_name: str) -> str:
        """Extract field value from a <dl> element by matching <dt> text.

        Handles the WK pattern where [keine Angabe] is wrapped in a
        <span class="wkde-empty"> — returns empty string for those.
        """
        dts = dl_element.xpath("dt")
        dds = dl_element.xpath("dd")

        for dt, dd in zip(dts, dds):
            if dt.text_content().strip() == field_name:
                # Check for [keine Angabe] placeholder
                empty_spans = dd.xpath('.//span[@class="wkde-empty"]')
                if empty_spans:
                    return ""
                return dd.text_content().strip()
        return ""

    def _parse_case_from_html(self, html_str: str, source_url: str) -> dict | None:
        """Parse WK/VORIS case detail page and return OLDP case dict."""
        tree = lxml.html.fromstring(html_str)

        # Extract content from wkde-document-body section
        body_sections = tree.xpath('//*[contains(@class, "wkde-document-body")]')
        if not body_sections:
            logger.warning("Could not find document body from: %s", source_url)
            return None

        content = self.get_inner_html(body_sections[0])
        if not content or not content.strip():
            logger.warning("Empty document body from: %s", source_url)
            return None

        # Extract metadata from bibliography <dl>
        bib_sections = tree.xpath('//*[contains(@class, "wkde-bibliography")]//dl')
        if not bib_sections:
            logger.warning("Could not find bibliography from: %s", source_url)
            return None

        dl = bib_sections[0]

        # Replace non-breaking spaces with regular spaces (WK uses \xa0)
        court_name = self._get_field_value(dl, "Gericht").replace("\xa0", " ")
        date_raw = self._get_field_value(dl, "Datum")
        date = self.parse_german_date(date_raw) if date_raw else ""
        file_number = self._get_field_value(dl, "Aktenzeichen")
        ecli = self._get_field_value(dl, "ECLI")
        case_type = self._get_field_value(dl, "Entscheidungsform")

        if not court_name or not file_number:
            logger.warning("Missing court or file_number from: %s", source_url)
            return None

        case: dict = {
            "court_name": court_name,
            "file_number": file_number,
            "date": date,
            "content": content,
            "source_url": source_url,
        }

        if case_type:
            case["type"] = case_type
        if ecli:
            case["ecli"] = ecli

        return case

    def iter_cases(self):
        """Search VORIS and yield cases one at a time (streaming)."""
        page = 0  # 0-indexed pagination
        empty_pages = 0
        yielded = 0

        while page <= NS_MAX_PAGE:
            try:
                links = self._search_page(page)
            except requests.HTTPError as exc:
                # voris caps pagination at 200 pages and returns 404 instead
                # of an empty result set (confirmed prod 2026-05-31..06-04,
                # always page=200). Treat 4xx on the search endpoint as
                # end-of-pagination — not a failure.
                status = exc.response.status_code if exc.response is not None else None
                if status is not None and 400 <= status < 500:
                    logger.info("End of pagination at page %d (HTTP %d)", page, status)
                    break
                logger.warning("Failed to search page %d: %s", page, exc)
                break
            except requests.RequestException as exc:
                logger.warning("Failed to search page %d: %s", page, exc)
                break

            if not links:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue

            empty_pages = 0
            logger.info("Page %d: found %d case links", page, len(links))

            for doc_path in links:
                case_url = f"{NS_BASE_URL}{doc_path}"
                if self.failure_tracker.should_skip(doc_path):
                    continue

                try:
                    html_str = self._get(doc_path).text
                except requests.HTTPError as exc:
                    # voris fronts every /browse/document/* with Cloudflare;
                    # specific UUIDs are persistently 403-blocked from our
                    # IP regardless of UA/headers (verified 2026-06-04 with
                    # a full browser-fingerprint probe — still 403 while
                    # ``/search`` returns 200). Treating these as transient
                    # produced 17× repeat-warnings in the 7-day prod window.
                    # Feed 4xx into the failure tracker so a stuck UUID is
                    # dropped from the retry budget; leave 5xx transient.
                    status = (
                        exc.response.status_code if exc.response is not None else None
                    )
                    if status is not None and 400 <= status < 500:
                        logger.warning("Failed to fetch case %s: %s", case_url, exc)
                        self.failure_tracker.record_failure(doc_path, f"HTTP {status}")
                        continue
                    logger.warning("Failed to fetch case %s: %s", case_url, exc)
                    continue  # 5xx / no response → transient
                except requests.RequestException as exc:
                    logger.warning("Failed to fetch case %s: %s", case_url, exc)
                    continue  # network → transient

                try:
                    case = self._parse_case_from_html(html_str, case_url)
                except Exception as exc:
                    logger.warning("Failed to parse case %s: %s", case_url, exc)
                    self.failure_tracker.record_failure(doc_path, exc)
                    continue

                if case is None:
                    self.failure_tracker.record_failure(
                        doc_path, "unparseable case page"
                    )
                    continue
                if not self._is_within_date_range(case.get("date", "")):
                    continue

                self.failure_tracker.record_success(doc_path)
                yield case
                yielded += 1

                if self.limit and yielded >= self.limit:
                    return

            page += 1

    def get_cases(self) -> list[dict]:
        """Materialise :meth:`iter_cases` as a list. Prefer streaming."""
        return list(self.iter_cases())

    # ------------------------------------------------------------------
    # Targeted citation-based lookup (LookupMixin)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_az(az: str) -> str:
        return " ".join(az.split()).lower()

    def lookup_search(
        self,
        file_number: str | None = None,
        ecli: str | None = None,
        court_hint: str | None = None,
        date: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Look up a specific NDS decision by Aktenzeichen.

        voris has no dedicated AZ field but its ``query=`` parameter
        accepts a quoted exact-match phrase that resolves to a small
        candidate set. Candidates are still filtered client-side for the
        exact AZ to drop near-matches from the same court.

        ``ecli`` is not exposed in voris's search index — calls relying
        on it raise ``ValueError`` so the agent doesn't misread an empty
        result.
        """
        if not file_number:
            raise ValueError("voris lookup requires file_number (ecli not supported)")

        params = {
            "query": f'"{file_number}"',
            "publicationtype": "publicationform-ats-filter!ATS_Rechtsprechung",
            "sort_order": "date_desc",
            "page": "0",
        }
        resp = self._get(NS_SEARCH_PATH, params=params)
        norm_target = self._normalise_az(file_number)

        candidates: list[dict] = []
        for href, label in _NS_RESULT_LINK_RE.findall(resp.text):
            m = _NS_LABEL_RE.match(label)
            if not m:
                continue
            az = m.group("az").strip()
            if self._normalise_az(az) != norm_target:
                continue
            german_date = m.group("date").strip()
            iso_date = self.parse_german_date(german_date) if german_date else ""
            candidates.append(
                {
                    "doc_id": href,
                    "court_name": m.group("court").strip(),
                    "file_number": az,
                    "date": iso_date,
                    "ecli": "",
                    "type": "",
                    "snippet": (m.group("title") or "").strip()[:200] or label[:200],
                }
            )
            if len(candidates) >= limit:
                break
        return candidates

    def lookup_fetch(self, doc_id: str) -> dict | None:
        """Fetch the full voris case dict for ``doc_id``.

        ``doc_id`` here is the document path returned by
        :meth:`lookup_search` (``/browse/document/<uuid>``). Reuses the
        SSR HTML parser; the same Cloudflare 4xx behaviour as the
        streaming flow applies — callers see the raw exception.
        """
        try:
            html_str = self._get(doc_id).text
        except requests.RequestException as exc:
            logger.warning("Failed to fetch voris case %s: %s", doc_id, exc)
            return None
        case_url = f"{NS_BASE_URL}{doc_id}"
        try:
            return self._parse_case_from_html(html_str, case_url)
        except Exception as exc:
            logger.warning("Failed to parse voris case %s: %s", case_url, exc)
            return None
