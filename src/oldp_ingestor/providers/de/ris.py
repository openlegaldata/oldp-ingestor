"""Provider for the Rechtsinformationsportal des Bundes (RIS) API.

API docs: https://docs.rechtsinformationen.bund.de/

Data model (FRBR-based)
-----------------------
The RIS API models legislation using three abstraction levels:

- **Work** – the abstract legislative act (e.g. BGB as a whole).
  Identified by an ELI (European Legislation Identifier) such as
  ``eli/bund/bgbl-1/1896/s195``.
- **Expression** – a specific version of a work at a point in time
  (i.e. a particular consolidated revision).  Represented by the
  ``workExample`` field on the list response and returned in full via
  the expression detail endpoint.
- **Manifestation** – a concrete serialisation of an expression
  (HTML, XML, ZIP).  Accessible via encoding URLs on the expression.

Endpoints used
--------------
List legislation (paginated, filterable)::

    GET /v1/legislation?size=300&pageIndex=0[&searchTerm=...]

Response is a ``hydra:Collection`` with ``member[]`` items.  Each
member wraps a ``Legislation`` item (work-level metadata) plus a
``workExample`` reference to the current expression.

Expression detail::

    GET /v1/legislation/eli/{jurisdiction}/{agent}/{year}/{naturalId}/{pointInTime}/{version}/{language}

Returns the full expression including ``hasPart`` (article list) and
``encoding`` (manifestation URLs for HTML/XML/ZIP).

Article HTML::

    GET /v1/legislation/eli/.../{pointInTimeManifestation}/{subtype}/{articleEid}.html

Returns a full HTML page for a single article.  We extract the
``<body>`` content.

Pagination
----------
- ``size`` (int, 1–300, default 100) – results per page.
- ``pageIndex`` (int, 0-indexed, default 0) – page number.
- Response ``view.next`` / ``view.previous`` contain links for
  navigation; ``totalItems`` gives the total count.

Filtering (query parameters on /v1/legislation)
------------------------------------------------
- ``searchTerm`` – full-text search; multiple words must all appear
  (AND logic). Enclose in double-quotes for exact phrase matching.
- ``eli`` – filter by work ELI (e.g. ``eli/bund/bgbl-1/1979/s1325``).
- ``temporalCoverageFrom`` / ``temporalCoverageTo`` – expressions in
  force within a date range (ISO 8601).
- ``mostRelevantOn`` – return a single expression per work for a given
  date.
- ``dateFrom`` / ``dateTo`` – adoption/signature date range.
- ``sort`` – sort field, prefix ``-`` for descending.  Values:
  ``date``, ``-date``, ``temporalCoverageFrom``,
  ``legislationIdentifier``.

Rate limiting
-------------
- 600 requests per minute per client IP.
- Exceeding the limit returns ``503 Service Unavailable``.
- Recommended: implement exponential backoff and cache responses.

Note: This is a trial service and may be subject to changes.
See https://docs.rechtsinformationen.bund.de/guides/ for details.
"""

import logging
import re

import requests

from oldp_ingestor.providers.base import LawProvider
from oldp_ingestor.providers.de.ris_common import (
    BASE_URL,
    MAX_PAGE_SIZE,
    RISBaseClient,
    extract_body,
)

logger = logging.getLogger(__name__)


def _parse_article_name(name: str) -> tuple[str, str]:
    """Parse an article name like '§ 1 Organisation des ...' into (section, title)."""
    match = re.match(r"^((?:§|Artikel|Art\.)\s*\S+)\s*(.*)", name)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return name, ""


def _slugify(text: str) -> str:
    """Convert text into a URL-friendly slug."""
    slug = re.sub(r"[^a-z0-9_-]", "-", text.lower())
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-")


def _html_content_url(encoding: list[dict]) -> str | None:
    """Find the HTML encoding contentUrl from the encoding list."""
    for enc in encoding:
        if enc.get("encodingFormat") == "text/html":
            return enc["contentUrl"]
    return None


class RISProvider(RISBaseClient, LawProvider):
    """Fetches legislation from the Rechtsinformationsportal des Bundes API.

    This provider lists law books via ``GET /v1/legislation`` (paginated),
    fetches each expression's detail to obtain the article list (``hasPart``)
    and HTML manifestation URLs (``encoding``), then retrieves individual
    article HTML for each law.

    The expression detail is cached after the ``get_law_books()`` call so
    that the subsequent ``get_laws()`` calls can look up articles without
    additional network requests.

    Args:
        search_term: Optional full-text ``searchTerm`` filter passed to the
            list endpoint.  Multiple words use AND logic; wrap in quotes
            for exact phrase matching.
        limit: Maximum number of law books to return.  Applied client-side
            after fetching from the API.
    """

    SOURCE = {
        "name": "Rechtsinformationssystem des Bundes (RIS)",
        "homepage": BASE_URL,
    }

    def __init__(
        self,
        search_term: str | None = None,
        limit: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(request_delay=request_delay)
        self.search_term = search_term
        self.limit = limit
        self.date_from = date_from
        self.date_to = date_to
        #: Cached expression details keyed by ``(abbreviation, legislationDate)``.
        #: Populated by :meth:`get_law_books`, consumed by :meth:`get_laws`.
        self._expression_cache: dict[tuple[str, str], dict] = {}

    def _fetch_expression_detail(self, expression_id: str) -> dict:
        """Fetch full expression detail including hasPart and encoding."""
        return self._get_json(expression_id)

    def get_law_books(self) -> list[dict]:
        """Fetch law books from ``GET /v1/legislation``.

        Paginates through the list endpoint using ``size`` and ``pageIndex``.
        For each legislation item, fetches the expression detail to obtain
        the article list and caches it in ``_expression_cache`` for later
        use by :meth:`get_laws`.

        Returns:
            List of dicts with keys ``code`` (abbreviation), ``title``
            (name), and ``revision_date`` (legislationDate).
        """
        books = []
        page_index = 0
        total_items = None

        while True:
            params = {"size": MAX_PAGE_SIZE, "pageIndex": page_index}
            if self.search_term:
                params["searchTerm"] = self.search_term
            if self.date_from:
                params["dateFrom"] = self.date_from
            if self.date_to:
                params["dateTo"] = self.date_to
            if self.date_from or self.date_to:
                params["sort"] = "-date"

            data = self._get_json("/v1/legislation", **params)
            members = data.get("member", [])

            if not members:
                break

            # Log total on first page
            if total_items is None:
                total_items = data.get("totalItems", "?")
                logger.info("Fetching law books (total available: %s)...", total_items)

            logger.info(
                "Page %d: processing %d items (%d/%s)...",
                page_index,
                len(members),
                len(books) + len(members),
                total_items,
            )

            for member in members:
                item = member["item"]
                abbreviation = item.get("abbreviation", "")
                legislation_date = item.get("legislationDate", "")

                if not abbreviation or not legislation_date:
                    logger.debug("Skipping member with missing abbreviation or date")
                    continue

                # Fetch expression detail
                work_example = item.get("workExample", {})
                expression_id = work_example.get("@id", "")
                if not expression_id:
                    logger.debug("Skipping %s: no expression ID", abbreviation)
                    continue

                detail = self._fetch_expression_detail(expression_id)
                work_example_detail = detail.get("workExample", {})

                # Cache for use in get_laws()
                cache_key = (abbreviation, legislation_date)
                self._expression_cache[cache_key] = work_example_detail

                books.append(
                    {
                        "code": abbreviation,
                        "title": item.get("name", ""),
                        "revision_date": legislation_date,
                    }
                )

                if self.limit and len(books) >= self.limit:
                    return books

            # Check if there's a next page
            view = data.get("view", {})
            if not view.get("next"):
                break
            page_index += 1

        return books

    def get_laws(self, book_code: str, revision_date: str) -> list[dict]:
        """Fetch individual laws (articles) for a given law book.

        Uses the cached expression detail from :meth:`get_law_books` to
        iterate over ``hasPart`` entries (articles).  For each article,
        the HTML manifestation URL is derived from the expression's
        ``encoding`` list by replacing the ``.html`` suffix with
        ``/{eId}.html``.

        Args:
            book_code: Abbreviation of the law book (e.g. ``"BGB"``).
            revision_date: The ``legislationDate`` identifying the expression.

        Returns:
            List of law dicts ready for the OLDP ``/api/laws/`` endpoint.
        """
        cache_key = (book_code, revision_date)
        expression = self._expression_cache.get(cache_key, {})

        parts = expression.get("hasPart", [])
        encoding = expression.get("encoding", [])
        html_base_url = _html_content_url(encoding) if encoding else None

        laws = []
        for order, part in enumerate(parts, start=1):
            e_id = part.get("eId", "")
            name = part.get("name", "")
            section, title = _parse_article_name(name)

            content = ""
            if html_base_url and e_id:
                # Article HTML URL: base URL without .html extension + /{eId}.html
                article_url = re.sub(r"\.html$", f"/{e_id}.html", html_base_url)
                try:
                    content = extract_body(self._get_text(article_url))
                except requests.RequestException as exc:
                    logger.warning("Failed to fetch article HTML for %s: %s", e_id, exc)

            laws.append(
                {
                    "book_code": book_code,
                    "revision_date": revision_date,
                    "section": section,
                    "title": title,
                    "content": content,
                    "slug": _slugify(section) or _slugify(e_id),
                    "order": order,
                }
            )

        return laws
