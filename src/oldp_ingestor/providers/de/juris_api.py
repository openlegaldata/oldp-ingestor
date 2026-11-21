"""Providers for the juris state portals over their JSON API (no browser).

The juris "Bürgerservice" portals (see ``juris.py``) are single-page apps.
The app itself loads everything from ``/jportal/wsrest/recherche3/``; these
providers call the same endpoints with ``requests`` instead of driving
Chromium (observed November 2026, portal version V08_37_00):

1. ``GET <BASE_URL>/`` — the page names its version in
   ``<meta name='juris-version'>``; every API call sends it.
2. ``POST /jportal/wsrest/recherche3/init`` with the cookie
   ``r3autologin="<portal>"`` — opens an anonymous session and returns
   ``csrfToken``, sent as ``X-CSRF-TOKEN`` with every further call.
3. ``POST …/search`` with ``RESULT_LIST`` (up to 500 hits per call,
   ``filters: {CATEGORY: [Rechtsprechung]}``) — the listing with date,
   court, file number and type, and the total in ``hits``.
4. ``POST …/document`` — the decision as JSON: ``head`` (the table with
   court, date, file number, ECLI, type) and ``text`` (HTML with margin
   numbers ``<a name="rd_n">``).

One request per decision instead of a browser page load (about ten API
calls plus scripts), and the listing pages to its end (Hessen: 48,336
decisions in 97 requests). When the session expires
(``security_notAuthenticated``) the provider signs in once more.
"""

import json
import logging
import re

import lxml.html
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.de import juris
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

API = "/jportal/wsrest/recherche3"


class JurisApiCaseProvider(ScraperBaseClient, CaseProvider):
    """A juris state portal over its JSON API.

    Subclasses set ``BASE_URL`` (with the portal path, e.g. ``…/bsbw``) and
    ``SOURCE``; see the generated per-state classes below.

    Args:
        date_from: Optional start date (YYYY-MM-DD), compared with the
            listing date; decisions outside are not fetched.
        date_to: Optional end date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    BASE_URL: str = ""
    SOURCE: dict = {"name": "", "homepage": ""}
    PAGE_SIZE = 500
    CATEGORY = "Rechtsprechung"

    # Header parsing and clean-up shared with the browser provider
    _parse_info_table = juris.JurisCaseProvider._parse_info_table
    _parse_german_date = staticmethod(juris.JurisCaseProvider._parse_german_date)
    _sanitize_content = staticmethod(juris.JurisCaseProvider._sanitize_content)

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.5,
        proxy: str | None = None,
    ):
        match = re.fullmatch(r"(https?://[^/]+)(/.+)", self.BASE_URL)
        if match is None:
            raise ValueError(f"BASE_URL without portal path: {self.BASE_URL!r}")
        origin, portal_path = match.groups()
        super().__init__(base_url=origin, request_delay=request_delay, proxy=proxy)
        self.portal_id = portal_path.strip("/")
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit
        self._version: str | None = None
        self._token: str | None = None

    # --- session ---------------------------------------------------------

    def _start_session(self) -> None:
        html = self._get(f"{self.BASE_URL}/").text
        match = re.search(r"name=['\"]juris-version['\"] content=['\"]([^'\"]+)", html)
        if match is None:
            raise RuntimeError(f"{self.BASE_URL}: no juris-version in the page")
        self._version = match.group(1)
        self.session.cookies.set("r3autologin", f'"{self.portal_id}"', path="/")
        init = self._post(
            f"{API}/init",
            json=self._body({}),
            headers={"JURIS-PORTALID": self.portal_id},
        ).json()
        self._token = init["csrfToken"]

    def _body(self, payload: dict) -> dict:
        return {
            **payload,
            "clientID": self.portal_id,
            "clientVersion": self._version,
            "r3ID": "oldp-ingestor",
        }

    def _api(self, name: str, payload: dict) -> requests.Response:
        """POST to an API endpoint; signs in again once if the session expired."""
        for attempt in range(2):
            if self._token is None:
                self._start_session()
            try:
                return self._post(
                    f"{API}/{name}",
                    json=self._body(payload),
                    headers={
                        "JURIS-PORTALID": self.portal_id,
                        "X-CSRF-TOKEN": self._token or "",
                    },
                )
            except requests.HTTPError as exc:
                resp = exc.response
                expired = (
                    resp is not None
                    and resp.status_code == 500
                    and b"security_notAuthenticated" in resp.content
                )
                if attempt == 0 and expired:
                    self._token = None
                    continue
                raise
        raise AssertionError("unreachable")

    # --- listing ---------------------------------------------------------

    def search_page(self, page: int) -> tuple[int, list[dict]]:
        """(total, entries) of listing page ``page`` (0-based)."""
        data = self._api(
            "search",
            {
                "searchTasks": {
                    "RESULT_LIST": {
                        "size": self.PAGE_SIZE,
                        "start": page * self.PAGE_SIZE + 1,
                        "addToHistory": False,
                        "addLinks": False,
                    }
                },
                "filters": {"CATEGORY": [self.CATEGORY]},
                "searches": [],
            },
        ).json()
        entries = []
        for hit in data.get("resultList", []):
            title = hit.get("titleList") or []
            subtitle = hit.get("subtitleList") or []
            entries.append(
                {
                    "doc_id": hit["docId"],
                    "date": self._parse_german_date(hit.get("date", "")),
                    "court_name": title[0] if title else "",
                    "file_number": title[1] if len(title) > 1 else "",
                    "type": subtitle[0] if subtitle else "",
                }
            )
        return int(data.get("hits", -1)), entries

    # --- documents -------------------------------------------------------

    def document_url(self, doc_id: str) -> str:
        return f"{self.BASE_URL}/document/{doc_id}"

    def fetch_document(self, doc_id: str) -> bytes:
        """The raw JSON answer of the ``document`` endpoint."""
        return self._api(
            "document",
            {
                "docId": doc_id,
                "format": "xsl",
                "keyword": None,
                "sourceParams": {"source": "Unknown", "category": "Alles"},
                "searches": [],
            },
        ).content

    @classmethod
    def content_html(cls, document: dict) -> str:
        """Head and text of a document as HTML, cleaned like the browser
        provider's content (no ``data-juris-*`` attributes, comments, permalink
        or hidden labels); margin number anchors stay."""
        parts = []
        for field in ("head", "text"):
            if document.get(field):
                tree = lxml.html.fromstring(f"<div>{document[field]}</div>")
                cls._sanitize_content(tree)
                parts.append(lxml.html.tostring(tree, encoding="unicode"))
        return "\n".join(parts)

    def parse_document(self, raw: bytes, url: str) -> dict | None:
        """A case dict from the answer of ``document``, or None."""
        try:
            document = json.loads(raw)
        except ValueError:
            logger.warning("No JSON from %s", url)
            return None
        head = lxml.html.fromstring(f"<div>{document.get('head', '')}</div>")
        info = self._parse_info_table(head)
        if not info.get("court_name") or not document.get("text"):
            return None
        case = {
            "court_name": info["court_name"],
            "file_number": info.get("file_number", ""),
            "date": info.get("date", ""),
            "content": self.content_html(document),
            "source_url": url,
        }
        for key in ("type", "ecli"):
            if info.get(key):
                case[key] = info[key]
        return case

    def iter_cases(self):
        """Yield the decisions of the portal, listing page by listing page."""
        page = 0
        yielded = 0
        while True:
            _, entries = self.search_page(page)
            if not entries:
                return
            for entry in entries:
                if not self._is_within_date_range(entry["date"]):
                    continue
                url = self.document_url(entry["doc_id"])
                try:
                    case = self.parse_document(
                        self.fetch_document(entry["doc_id"]), url
                    )
                except requests.RequestException as exc:
                    logger.warning("Failed to fetch %s: %s", url, exc)
                    continue
                if case is None:
                    continue
                yield case
                yielded += 1
                if self.limit and yielded >= self.limit:
                    return
            page += 1

    def get_cases(self) -> list[dict]:
        """Materialise :meth:`iter_cases` as a list. Prefer streaming."""
        return list(self.iter_cases())


def _api_variant(browser_cls: type) -> type:
    """The API provider for the portal of a browser provider of ``juris.py``."""
    return type(
        browser_cls.__name__.replace("CaseProvider", "ApiCaseProvider"),
        (JurisApiCaseProvider,),
        {
            "BASE_URL": browser_cls.BASE_URL,
            "SOURCE": browser_cls.SOURCE,
            "__module__": __name__,
            "__doc__": f"{browser_cls.__doc__} Over the JSON API.",
        },
    )


BbBeApiCaseProvider = _api_variant(juris.BbBeCaseProvider)
HhApiCaseProvider = _api_variant(juris.HhCaseProvider)
MvApiCaseProvider = _api_variant(juris.MvCaseProvider)
RlpApiCaseProvider = _api_variant(juris.RlpCaseProvider)
SaApiCaseProvider = _api_variant(juris.SaCaseProvider)
ShApiCaseProvider = _api_variant(juris.ShCaseProvider)
BwApiCaseProvider = _api_variant(juris.BwCaseProvider)
SlApiCaseProvider = _api_variant(juris.SlCaseProvider)
HeApiCaseProvider = _api_variant(juris.HeCaseProvider)
ThApiCaseProvider = _api_variant(juris.ThCaseProvider)
