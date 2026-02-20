"""Providers for eJustiz Bürgerservice portals (Juris-based).

All German state legal portals running juris eJustiz share the same SPA
architecture. The base class handles search, pagination, and case detail
extraction. Subclasses only override BASE_URL.

Covered states: BB (Berlin-Brandenburg), HH (Hamburg), MV (Mecklenburg-
Vorpommern), RLP (Rhineland-Palatinate), SA (Saxony-Anhalt),
SH (Schleswig-Holstein), BW (Baden-Württemberg), SL (Saarland),
HE (Hessen), TH (Thüringen).

These sites are JavaScript SPAs that require Playwright for rendering.
"""

import logging
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

    Args:
        court: Optional court filter.
        date_from: Optional start date (YYYY-MM-DD).
        date_to: Optional end date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between page loads.
    """

    BASE_URL: str = ""
    SEARCH_PATH: str = "/search"
    # The SPA needs time to render — wait for the result list or document content
    WAIT_SELECTOR: str = ".result-list-entry, .docLayoutText, .jportal-content"
    # Timeout for SPA rendering (ms)
    SPA_TIMEOUT: int = 15000

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.5,
    ):
        super().__init__(request_delay=request_delay)
        self.court = court
        self.date_from = date_from
        self.date_to = date_to
        self.limit = limit

    def _search_url(self, page: int = 1) -> str:
        """Build search URL for a given page.

        The old Jetspeed portlet URLs are redirected by the SPA but still
        trigger the search with the embedded query parameters.
        """
        if page == 1:
            return (
                f"{self.BASE_URL}/js_peid/Suchportlet1/media-type/html"
                f"?formhaschangedvalue=yes&eventSubmit_doSearch=suchen"
                f"&action=portlets.jw.MainAction&deletemask=no&wt_form=1"
                f"&form=bsstdFastSearch&desc=all&query=*&standardsuche=suchen"
            )
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

    def _get_case_ids_from_page(self, url: str) -> list[str]:
        """Render search page and extract doc IDs.

        The new React SPA uses path-based document URLs like
        /document/NJRE001631520/ instead of the old query-param format.
        """
        html = self._get_page_html(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )
        ids = list(set(re.findall(r"/document/([A-Z0-9]+)/", html)))
        return ids

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

    def get_cases(self) -> list[dict]:
        """Search portal, fetch and parse case details."""
        cases: list[dict] = []

        try:
            page = 1
            empty_pages = 0

            while True:
                url = self._search_url(page)
                try:
                    ids = self._get_case_ids_from_page(url)
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
                logger.info("Page %d: found %d doc IDs", page, len(ids))

                for doc_id in ids:
                    detail_url = self._case_detail_url(doc_id)
                    try:
                        case = self._parse_case_detail(detail_url)
                    except Exception as exc:
                        logger.warning("Failed to parse case %s: %s", doc_id, exc)
                        continue

                    if case is not None:
                        cases.append(case)

                    if self.limit and len(cases) >= self.limit:
                        return cases

                page += 1
        finally:
            self.close()

        return cases


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
