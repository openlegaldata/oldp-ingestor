"""Providers for eJustiz Bürgerservice portals (Juris-based).

All German state legal portals running juris eJustiz share the same SPA
architecture. The base class handles search, pagination, and case detail
extraction. Subclasses only override BASE_URL.

Covered states: BB (Berlin-Brandenburg), HH (Hamburg), MV (Mecklenburg-
Vorpommern), RLP (Rhineland-Palatinate), SA (Saxony-Anhalt),
SH (Schleswig-Holstein), BW (Baden-Württemberg), SL (Saarland),
HE (Hessen), TH (Thüringen).

These sites are JavaScript SPAs that require Playwright for rendering.

Supports an optional ``cache_dir`` so that parsed case dicts are persisted
to disk.  On subsequent runs the cached file is read instead of
re-fetching via Playwright, allowing interrupted runs to resume.
"""

import json
import logging
import os
import re
from datetime import datetime

import lxml.html
from lxml.cssselect import CSSSelector

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

logger = logging.getLogger(__name__)

# Info table selectors
_INFO_LABELS = {
    "Gericht": "court_name",
    "Entscheidungsdatum": "date",
    "Aktenzeichen": "file_number",
    "Dokumenttyp": "type",
    "ECLI": "ecli",
}


class JurisCaseProvider(PlaywrightBaseClient, CaseProvider):
    """Base provider for eJustiz Bürgerservice portals.

    All German state legal portals running juris eJustiz share the
    same SPA architecture. Subclasses only override BASE_URL and
    potentially CSS selectors.

    Supports server-side date filtering via Playwright extended search
    form. When date_from/date_to are set, Playwright clicks "Erweiterte
    Suche" to reveal date fields, fills ``#DatumInputFrom`` /
    ``#DatumInputTo`` (DD.MM.YYYY format), and submits the search. The
    portal returns only matching results. All 10 state subclasses
    inherit this behavior.

    Args:
        court: Optional court filter.
        date_from: Optional start date (YYYY-MM-DD). Triggers Playwright-based
            extended search; filled into ``#DatumInputFrom`` (DD.MM.YYYY) for
            server-side filtering.
        date_to: Optional end date (YYYY-MM-DD). Triggers Playwright-based
            extended search; filled into ``#DatumInputTo`` (DD.MM.YYYY) for
            server-side filtering.
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between page loads.
    """

    BASE_URL: str = ""
    SEARCH_PATH: str = "/search"
    # The SPA needs time to render — wait for the result list or document content
    WAIT_SELECTOR: str = (
        ".result-list-entry, .result-list__title, .docLayoutText, .jportal-content"
    )
    # Timeout for SPA rendering (ms)
    SPA_TIMEOUT: int = 15000

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.5,
        proxy: str | None = None,
        cache_dir: str | None = None,
    ):
        super().__init__(request_delay=request_delay, proxy=proxy)
        self.court = court
        self.date_from = date_from
        self.date_to = date_to
        self.limit = limit
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    # Whether the initial search with date filters has been submitted
    _search_submitted: bool = False

    def _search_url(self, page: int = 1) -> str:
        """Build search URL for a given page.

        For page 1, if date filters are set, we use Playwright form interaction
        (see _submit_search_with_dates). Otherwise use the old Jetspeed URL.
        For page > 1, always use the pagination URL.
        """
        if page == 1 and not self._search_submitted:
            return (
                f"{self.BASE_URL}/js_peid/Suchportlet1/media-type/html"
                f"?formhaschangedvalue=yes&eventSubmit_doSearch=suchen"
                f"&action=portlets.jw.MainAction&deletemask=no&wt_form=1"
                f"&form=bsstdFastSearch&desc=all&query=*&standardsuche=suchen"
            )
        elif page == 1:
            # Already submitted via form — return None to signal skip
            return ""
        else:
            pos = 1 + (page - 2) * 25
            return (
                f"{self.BASE_URL}/js_peid/Suchportlet1/media-type/html"
                f"?formhaschangedvalue=yes&eventSubmit_doSearch=suchen"
                f"&action=portlets.jw.MainAction&deletemask=no&wt_form=1"
                f"&form=bsstdFastSearch&desc=all&query=*"
                f"&currentNavigationPosition={pos}&numberofresults=15000"
                f"&sortmethod=standard&standardsuche=suchen"
            )

    @staticmethod
    def _iso_to_german_date(iso_date: str) -> str:
        """Convert YYYY-MM-DD to DD.MM.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}.{parts[1]}.{parts[0]}"
        return iso_date

    def _submit_search_with_dates(self):
        """Submit extended search form and paginate within the SPA.

        Opens the search page, fills DatumInputFrom/DatumInputTo, submits,
        and yields ``(ids, page_dates)`` tuples for each results page by
        clicking the SPA's "next" button.  Staying within the same page
        preserves the date filter context (URL-based pagination loses it).

        Raises on failure — no silent fallback to unfiltered search.
        """
        import time

        self._ensure_browser()

        search_url = f"{self.BASE_URL}{self.SEARCH_PATH}"
        logger.info(
            "Submitting extended search with dates: %s to %s",
            self.date_from,
            self.date_to,
        )

        if self.request_delay > 0:
            time.sleep(self.request_delay)

        page = self._context.new_page()
        try:
            page.goto(search_url, timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)

            # Click "Erweiterte Suche" to reveal date fields
            ext_search = page.locator("span.extended-search__label")
            ext_search.wait_for(timeout=30000)
            ext_search.click()
            page.wait_for_selector(
                "#DatumInputFrom, .extended-search-datefield__input",
                timeout=30000,
            )

            # Fill date fields
            if self.date_from:
                page.locator("#DatumInputFrom").fill(
                    self._iso_to_german_date(self.date_from)
                )
            if self.date_to:
                page.locator("#DatumInputTo").fill(
                    self._iso_to_german_date(self.date_to)
                )

            # Submit the search
            submit_btn = page.locator(
                'button[type="submit"], input[type="submit"], '
                'button:has-text("Suchen"), .search-form__button'
            ).first
            submit_btn.click()
            page.wait_for_selector(self.WAIT_SELECTOR, timeout=30000)

            self._search_submitted = True

            # Yield page 1
            html = page.content()
            ids = list(set(re.findall(r"/document/([A-Z0-9]+)/", html)))
            page_dates = self._extract_dates_from_listing(html)
            yield ids, page_dates

            # Paginate by clicking "next" within the SPA
            page_num = 1
            while ids:
                next_btns = page.locator('a[aria-label="Next"], [class*=next]')
                if next_btns.count() == 0:
                    break

                if self.request_delay > 0:
                    time.sleep(self.request_delay)

                next_btns.first.click()
                time.sleep(2)  # SPA needs time to re-render
                page.wait_for_load_state("networkidle", timeout=30000)

                page_num += 1
                html = page.content()
                ids = list(set(re.findall(r"/document/([A-Z0-9]+)/", html)))
                page_dates = self._extract_dates_from_listing(html)
                yield ids, page_dates
        finally:
            page.close()

    @staticmethod
    def _extract_dates_from_listing(html: str) -> list[str]:
        """Extract dates from search result listing.

        Dates appear in ``div.result-list__title-entry--leading`` elements
        as DD.MM.YYYY, sorted newest-first.  Returns ISO dates (YYYY-MM-DD).
        """
        raw = re.findall(
            r"result-list__title-entry--leading[^>]*>\s*(\d{2}\.\d{2}\.\d{4})", html
        )
        iso: list[str] = []
        for d in raw:
            parts = d.split(".")
            if len(parts) == 3:
                iso.append(f"{parts[2]}-{parts[1]}-{parts[0]}")
        return iso

    def _get_case_ids_from_page(self, url: str) -> list[str]:
        """Render search page and extract doc IDs."""
        html = self._get_page_html(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )
        ids = list(set(re.findall(r"/document/([A-Z0-9]+)/", html)))
        return ids

    def _get_ids_and_dates_from_page(self, url: str) -> tuple[list[str], list[str]]:
        """Render search page and extract doc IDs and dates."""
        html = self._get_page_html(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )
        ids = list(set(re.findall(r"/document/([A-Z0-9]+)/", html)))
        dates = self._extract_dates_from_listing(html)
        return ids, dates

    def _case_detail_url(self, doc_id: str) -> str:
        """Build URL for a specific case document."""
        return f"{self.BASE_URL}/document/{doc_id}"

    @staticmethod
    def _parse_german_date(date_str: str) -> str:
        """Parse DD.MM.YYYY -> YYYY-MM-DD."""
        try:
            dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return date_str

    def _parse_info_table(self, tree) -> dict:
        """Extract metadata from info table.

        Handles both old (<td class="TD30">) and new (<th class="TD30">)
        markup patterns used by eJustiz portals.
        """
        info: dict = {}

        # Try TD30/TD70 pattern (matches both <td> and <th>)
        for label, field in _INFO_LABELS.items():
            for elem in tree.xpath(
                f'//*[contains(@class, "TD30")]/strong[text()="{label}:"]'
            ):
                td_parent = elem.getparent()
                if td_parent is not None:
                    tr = td_parent.getparent()
                    if tr is not None:
                        for text in tr.xpath('.//*[contains(@class, "TD70")]//text()'):
                            text = text.strip()
                            if text:
                                info[field] = text
                                break

        # Fallback: try div-based layout
        if "court_name" not in info:
            for label, field in _INFO_LABELS.items():
                for elem in tree.xpath(
                    f'//div[contains(@class, "fieldLabel") and '
                    f'contains(text(), "{label}")]'
                ):
                    sibling = elem.getnext()
                    if sibling is not None:
                        text = sibling.text_content().strip()
                        if text:
                            info[field] = text

        # Convert date
        if "date" in info:
            info["date"] = self._parse_german_date(info["date"])

        return info

    @staticmethod
    def _sanitize_content(root) -> None:
        """Remove Juris-specific UI artifacts from *root* element in-place."""
        # 1. Remove permalink section: <h3 class="unsichtbar">Permalink</h3>
        #    and <div id="permalink" ...> (entire elements)
        for el in root.xpath('//h3[contains(@class, "unsichtbar")]'):
            if "Permalink" in (el.text_content() or ""):
                el.getparent().remove(el)
        for el in root.xpath('//*[@id="permalink"]'):
            el.getparent().remove(el)

        # 2. Remove <img> tags with /jportal/ src (broken portal icons)
        for img in root.xpath('//img[contains(@src, "/jportal/")]'):
            parent = img.getparent()
            if parent is not None:
                # Preserve tail text
                if img.tail:
                    prev = img.getprevious()
                    if prev is not None:
                        prev.tail = (prev.tail or "") + img.tail
                    else:
                        parent.text = (parent.text or "") + img.tail
                parent.remove(img)

        # 3. Remove all data-juris-* attributes
        for el in root.iter():
            to_remove = [a for a in el.attrib if a.startswith("data-juris-")]
            for a in to_remove:
                del el.attrib[a]

        # 4. Remove HTML comments (hlIgnoreOn/Off, emptyTag, etc.)
        for comment in root.xpath("//comment()"):
            parent = comment.getparent()
            if parent is not None:
                if comment.tail:
                    prev = comment.getprevious()
                    if prev is not None:
                        prev.tail = (prev.tail or "") + comment.tail
                    else:
                        parent.text = (parent.text or "") + comment.tail
                parent.remove(comment)

        # 5. Remove <span class="unsichtbar"> elements (hidden labels like "Randnummer")
        for span in root.xpath('//span[contains(@class, "unsichtbar")]'):
            parent = span.getparent()
            if parent is not None:
                if span.tail:
                    prev = span.getprevious()
                    if prev is not None:
                        prev.tail = (prev.tail or "") + span.tail
                    else:
                        parent.text = (parent.text or "") + span.tail
                parent.remove(span)

        # 6. Convert <a class="doclink"> to <span> (dead links without JS)
        for link in root.xpath('//a[contains(@class, "doclink")]'):
            span = lxml.html.HtmlElement()
            span.tag = "span"
            span.text = link.text
            span.tail = link.tail
            for child in link:
                span.append(child)
            parent = link.getparent()
            if parent is not None:
                parent.replace(link, span)

        # 7. Remove href from remaining <a> tags
        for link in CSSSelector("a")(root):
            if "href" in link.attrib:
                del link.attrib["href"]

    def _extract_content(self, tree) -> str:
        """Extract decision text HTML from case page."""
        for selector in [".docLayoutText", ".documentText", ".content"]:
            matches = CSSSelector(selector)(tree)
            if matches:
                content_tree = matches[0]
                self._sanitize_content(content_tree)
                return lxml.html.tostring(content_tree, pretty_print=True).decode(
                    "utf-8"
                )

        # Fallback: return body
        body = tree.xpath("//body")
        if body:
            self._sanitize_content(body[0])
            return lxml.html.tostring(body[0]).decode("utf-8")

        return ""

    def _parse_case_detail(self, url: str) -> dict | None:
        """Fetch and parse a single case detail page."""
        tree = self._get_page_tree(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )

        info = self._parse_info_table(tree)
        content = self._extract_content(tree)

        if not info.get("court_name") or not content or len(content) < 10:
            return None

        case: dict = {
            "court_name": info.get("court_name", ""),
            "file_number": info.get("file_number", ""),
            "date": info.get("date", ""),
            "content": content,
        }

        if info.get("type"):
            case["type"] = info["type"]
        if info.get("ecli"):
            case["ecli"] = info["ecli"]

        return case

    def _get_case(self, doc_id: str) -> dict | None:
        """Get a parsed case dict, using cache if available."""
        if self.cache_dir:
            cache_path = os.path.join(self.cache_dir, f"{doc_id}.json")
            if os.path.exists(cache_path):
                with open(cache_path, encoding="utf-8") as f:
                    return json.load(f)

        detail_url = self._case_detail_url(doc_id)
        case = self._parse_case_detail(detail_url)

        if case is not None and self.cache_dir:
            cache_path = os.path.join(self.cache_dir, f"{doc_id}.json")
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(case, f, ensure_ascii=False)

        return case

    def get_cases(self) -> list[dict]:
        """Search portal, fetch and parse case details.

        When ``date_from`` or ``date_to`` are set the extended search form
        is submitted via Playwright on page 1.  If the form submission
        fails, a ``RuntimeError`` is raised instead of silently falling
        back to an unfiltered search.

        Subsequent pages use direct URL pagination.  Because pagination
        loses the server-side date filter, dates are extracted from the
        listing and used for **early-stop**: once all dates on a page are
        before ``date_from``, pagination stops.  Individual cases are also
        filtered client-side before fetching details.

        When ``cache_dir`` is set, parsed case dicts are persisted to disk
        so interrupted runs can resume.
        """
        cases: list[dict] = []
        self._search_submitted = False

        try:
            if self.date_from or self.date_to:
                # SPA-internal pagination with date filter
                pages = self._submit_search_with_dates()
            else:
                # URL-based per-page pagination (no date filter)
                pages = self._paginate_url_based()

            for page_num, (ids, page_dates) in enumerate(pages, 1):
                if not ids:
                    continue

                logger.info("Page %d: found %d doc IDs", page_num, len(ids))

                # Early-stop: all dates on page before date_from
                if self.date_from and page_dates:
                    newest_on_page = max(page_dates)
                    if newest_on_page < self.date_from:
                        logger.info(
                            "Early stop at page %d (newest %s < date_from %s)",
                            page_num,
                            newest_on_page,
                            self.date_from,
                        )
                        break

                for doc_id in ids:
                    try:
                        case = self._get_case(doc_id)
                    except Exception as exc:
                        logger.warning("Failed to parse case %s: %s", doc_id, exc)
                        continue

                    if case is not None:
                        if not self._is_within_date_range(case.get("date", "")):
                            continue
                        cases.append(case)

                    if self.limit and len(cases) >= self.limit:
                        return cases
        finally:
            self.close()

        return cases

    def _paginate_url_based(self):
        """Yield ``(ids, page_dates)`` via URL-based pagination (no date filter)."""
        page = 1
        empty_pages = 0
        while True:
            url = self._search_url(page)
            try:
                ids, page_dates = self._get_ids_and_dates_from_page(url)
            except Exception as exc:
                logger.warning("Failed to search page %d: %s", page, exc)
                break
            if not ids:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue
            empty_pages = 0
            yield ids, page_dates
            page += 1


# --- Per-state subclasses (thin — just BASE_URL override) ---


class BbBeCaseProvider(JurisCaseProvider):
    """Berlin-Brandenburg (gesetze.berlin.de)."""

    BASE_URL = "https://gesetze.berlin.de/bsbe"
    SOURCE = {
        "name": "Landesrecht Berlin-Brandenburg",
        "homepage": "https://gesetze.berlin.de",
    }


class HhCaseProvider(JurisCaseProvider):
    """Hamburg (landesrecht-hamburg.de)."""

    BASE_URL = "https://www.landesrecht-hamburg.de/bsha"
    SOURCE = {
        "name": "Landesrecht Hamburg",
        "homepage": "https://www.landesrecht-hamburg.de",
    }


class MvCaseProvider(JurisCaseProvider):
    """Mecklenburg-Vorpommern (landesrecht-mv.de)."""

    BASE_URL = "https://www.landesrecht-mv.de/bsmv"
    SOURCE = {
        "name": "Landesrecht Mecklenburg-Vorpommern",
        "homepage": "https://www.landesrecht-mv.de",
    }


class RlpCaseProvider(JurisCaseProvider):
    """Rhineland-Palatinate (landesrecht.rlp.de)."""

    BASE_URL = "https://www.landesrecht.rlp.de/bsrp"
    SOURCE = {
        "name": "Landesrecht Rheinland-Pfalz",
        "homepage": "https://www.landesrecht.rlp.de",
    }


class SaCaseProvider(JurisCaseProvider):
    """Saxony-Anhalt (landesrecht.sachsen-anhalt.de)."""

    BASE_URL = "https://www.landesrecht.sachsen-anhalt.de/bsst"
    SOURCE = {
        "name": "Landesrecht Sachsen-Anhalt",
        "homepage": "https://www.landesrecht.sachsen-anhalt.de",
    }


class ShCaseProvider(JurisCaseProvider):
    """Schleswig-Holstein (gesetze-rechtsprechung.sh.juris.de)."""

    BASE_URL = "https://www.gesetze-rechtsprechung.sh.juris.de/bssh"
    SOURCE = {
        "name": "Landesrecht Schleswig-Holstein",
        "homepage": "https://www.gesetze-rechtsprechung.sh.juris.de",
    }


class BwCaseProvider(JurisCaseProvider):
    """Baden-Württemberg (landesrecht-bw.de)."""

    BASE_URL = "https://www.landesrecht-bw.de/bsbw"
    SOURCE = {
        "name": "Landesrecht Baden-Württemberg",
        "homepage": "https://www.landesrecht-bw.de",
    }


class SlCaseProvider(JurisCaseProvider):
    """Saarland (recht.saarland.de)."""

    BASE_URL = "https://recht.saarland.de/bssl"
    SOURCE = {"name": "Landesrecht Saarland", "homepage": "https://recht.saarland.de"}


class HeCaseProvider(JurisCaseProvider):
    """Hessen (lareda.hessenrecht.hessen.de)."""

    BASE_URL = "https://www.lareda.hessenrecht.hessen.de/bshe"
    SOURCE = {
        "name": "Landesrecht Hessen",
        "homepage": "https://www.lareda.hessenrecht.hessen.de",
    }


class ThCaseProvider(JurisCaseProvider):
    """Thüringen (landesrecht.thueringen.de)."""

    BASE_URL = "https://landesrecht.thueringen.de/bsth"
    SOURCE = {
        "name": "Landesrecht Thüringen",
        "homepage": "https://landesrecht.thueringen.de",
    }
