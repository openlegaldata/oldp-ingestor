"""Provider for gesetze-im-internet.de (German federal laws).

Pulls the German federal law corpus from the gesetze-im-internet.de TOC
feed plus per-law xml.zip archives (DTD: gii-norm).

Change detection
----------------
The TOC (``gii-toc.xml``) carries no per-entry timestamps; only ``<title>``
and ``<link>``. So we use HTTP conditional GET on each zip:

* On first run for a given URL slug, we download unconditionally, parse,
  and remember the response's ``Last-Modified`` header in the cache.
* On subsequent runs we send ``If-Modified-Since: <stored Last-Modified>``.
  ``304 Not Modified`` → skip without parsing. ``200`` → re-download,
  re-parse, and decide whether the newly parsed ``revision_date`` is
  ahead of what OLDP currently has.

The site's HTTP ``Last-Modified`` corresponds within a few seconds to the
inner XML's ``<dokumente builddate>`` so it's a faithful proxy.

State sources
-------------
* OLDP API (`GET /api/law_books/?latest=true`) — fetched once at run start
  via :class:`OLDPClient` and used to decide whether a parsed revision
  date is newer than what's already stored. OLDP's ``LawBookCreator``
  flips ``latest=False`` on the previous revision automatically when a
  newer one is inserted, so no PATCH is needed from the client.
* Local ``cache_dir`` — required to make runs resumable. Layout::

      <cache_dir>/
        url_slug_to_jurabk.json   # {"bgb": "BGB", "sgb_5": "SGB 5", ...}
        done.json                 # {"<url_slug>": {"jurabk": ..., "http_last_modified": ..., "oldp_revision_date": ...}}
        zips/<url_slug>.zip       # last fetched zip body, used to re-parse for laws

Politeness
----------
Inherits the shared User-Agent (advertises non-commercial research +
contact URL), per-host RPM ceiling, jitter, exponential backoff on
429/503, and circuit breaker from :class:`HttpBaseClient`. A wide-open
``robots.txt`` and no declared ``Crawl-delay`` mean any reasonable RPM
is acceptable; the existing ingestor defaults are used.

Out of scope (v1)
-----------------
* Image and binary attachments inside the zip are ignored — we ingest
  text content only.
* No diff-level updates. Each detected change yields a fresh
  ``LawBook(code, revision_date)`` row plus its ``Law`` rows; OLDP
  handles latest-flag flipping in a transaction.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests

from oldp_ingestor.client import OLDPClient
from oldp_ingestor.providers.base import LawProvider
from oldp_ingestor.providers.de.gii_parser import (
    GiiParseError,
    parse_gii_zip,
)
from oldp_ingestor.providers.http_client import HttpBaseClient

logger = logging.getLogger(__name__)


GII_BASE_URL = "https://www.gesetze-im-internet.de"
GII_TOC_URL = f"{GII_BASE_URL}/gii-toc.xml"
OLDP_LAW_BOOKS_PATH = "/api/law_books/?latest=true&limit=1000"


def _http_date(date_str: str) -> str:
    """Convert a YYYY-MM-DD date into an RFC 7231 IMF-fixdate string.

    Used as ``If-Modified-Since`` value when no exact ``Last-Modified``
    is cached for a slug. Maps ``2026-04-13`` to
    ``Mon, 13 Apr 2026 23:59:59 GMT`` (end-of-day) so that any zip
    rebuilt during that day still triggers a 200 response and any zip
    untouched since then yields a 304.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    return format_datetime(dt, usegmt=True)


def _http_date_to_iso(http_date_str: str | None) -> str | None:
    """Parse RFC 7231 HTTP-date into YYYY-MM-DD or return None."""
    if not http_date_str:
        return None
    try:
        return parsedate_to_datetime(http_date_str).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def _slug_from_link(link: str) -> str:
    """Extract the URL slug (first path component) from a TOC link."""
    path = urlparse(link).path
    parts = [p for p in path.split("/") if p]
    return parts[0] if parts else ""


class GiiLawProvider(HttpBaseClient, LawProvider):
    """Fetches German federal laws from gesetze-im-internet.de.

    Args:
        oldp_client: Authenticated client used once at run start to load
            the (jurabk → revision_date) map of currently-latest law
            books in OLDP. Set to ``None`` to skip the bootstrap (every
            zip will then be downloaded and parsed unconditionally).
        cache_dir: Required directory for resumable runs. Stores the
            per-slug state, the url_slug → jurabk mapping, and the last
            fetched zip body so :meth:`get_laws` can re-parse without a
            second download.
        toc_url: Override the default TOC URL.
        force_full: Skip ``If-Modified-Since`` and download every zip
            unconditionally. Useful for forced re-syncs.
        limit: Cap the number of TOC entries processed (testing aid).
        request_delay: Baseline inter-request delay in seconds.
        proxy: Optional HTTP/SOCKS5 proxy URL.
    """

    SOURCE = {
        "name": "Gesetze im Internet (BMJ / juris)",
        "homepage": GII_BASE_URL,
    }

    def __init__(
        self,
        oldp_client: OLDPClient | None,
        cache_dir: str,
        toc_url: str = GII_TOC_URL,
        force_full: bool = False,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(
            base_url=GII_BASE_URL, request_delay=request_delay, proxy=proxy
        )
        if not cache_dir:
            raise ValueError(
                "cache_dir is required for the gii provider — "
                "needed for resumable runs and for re-parsing zips in get_laws()."
            )
        self.oldp_client = oldp_client
        self.cache_dir = cache_dir
        self.toc_url = toc_url
        self.force_full = force_full
        self.limit = limit
        self._zips_dir = os.path.join(cache_dir, "zips")
        os.makedirs(self._zips_dir, exist_ok=True)
        self._mapping_path = os.path.join(cache_dir, "url_slug_to_jurabk.json")
        self._state_path = os.path.join(cache_dir, "done.json")
        self._mapping: dict[str, str] = self._load_json(self._mapping_path)
        self._state: dict[str, dict[str, Any]] = self._load_json(self._state_path)
        # Populated in get_law_books, consumed in get_laws.
        self._book_to_zip: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------ #
    # Persistence helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_json(path: str) -> dict:
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Failed to load %s: %s — starting empty", path, exc)
            return {}

    @staticmethod
    def _save_json(path: str, data: dict) -> None:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp, path)

    def _persist_state(self) -> None:
        self._save_json(self._mapping_path, self._mapping)
        self._save_json(self._state_path, self._state)

    # ------------------------------------------------------------------ #
    # OLDP bootstrap
    # ------------------------------------------------------------------ #

    def _fetch_oldp_latest_books(self) -> dict[str, str]:
        """Return ``{code: revision_date}`` for all currently-latest LawBooks.

        Uses LimitOffsetPagination (``?limit=1000``); OLDP prod ignores
        ``page_size``. Empty dict if no client or on bootstrap failure.
        """
        if self.oldp_client is None:
            logger.info("No OLDP client — skipping bootstrap of latest books")
            return {}
        books: dict[str, str] = {}
        path = OLDP_LAW_BOOKS_PATH
        page = 0
        while path:
            page += 1
            data = self.oldp_client.get(path)
            for item in data.get("results", []):
                code = item.get("code")
                rev = item.get("revision_date")
                if code and rev:
                    books[code] = rev
            next_url = data.get("next")
            if not next_url:
                break
            parsed = urlparse(next_url)
            path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        logger.info(
            "Loaded %d latest law book(s) from OLDP (%d page(s))", len(books), page
        )
        return books

    # ------------------------------------------------------------------ #
    # TOC
    # ------------------------------------------------------------------ #

    def _fetch_toc(self) -> list[tuple[str, str, str]]:
        """Return a list of ``(url_slug, title, zip_url)`` from gii-toc.xml."""
        logger.info("Fetching GII TOC: %s", self.toc_url)
        text = self._get_text(self.toc_url)
        root = ET.fromstring(text)
        entries: list[tuple[str, str, str]] = []
        for item in root.iter("item"):
            link_el = item.find("link")
            title_el = item.find("title")
            if link_el is None or not link_el.text:
                continue
            zip_url = link_el.text.strip()
            # Upgrade http://...gesetze-im-internet... to https — both serve
            # the same content but https avoids the 301 round trip.
            if zip_url.startswith("http://www.gesetze-im-internet.de"):
                zip_url = "https://" + zip_url[len("http://") :]
            url_slug = _slug_from_link(zip_url)
            if not url_slug:
                continue
            title = (title_el.text or "").strip() if title_el is not None else ""
            entries.append((url_slug, title, zip_url))
        logger.info("TOC contains %d entries", len(entries))
        return entries

    # ------------------------------------------------------------------ #
    # Per-entry processing
    # ------------------------------------------------------------------ #

    def _conditional_get_zip(
        self, zip_url: str, since: str | None
    ) -> requests.Response:
        """GET the zip with optional ``If-Modified-Since``.

        Returns the underlying ``requests.Response`` so callers can branch
        on ``status_code`` (200 vs 304). Other status codes have already
        been raised by ``_request_with_retry``.
        """
        headers: dict[str, str] = {}
        if since and not self.force_full:
            headers["If-Modified-Since"] = since
        return self._get(zip_url, headers=headers)

    def _zip_path(self, url_slug: str) -> str:
        return os.path.join(self._zips_dir, f"{url_slug}.zip")

    def _save_zip(self, url_slug: str, content: bytes) -> str:
        path = self._zip_path(url_slug)
        with open(path, "wb") as f:
            f.write(content)
        return path

    def _process_entry(
        self,
        url_slug: str,
        toc_title: str,
        zip_url: str,
        oldp_books: dict[str, str],
    ) -> dict[str, Any] | None:
        """Process one TOC entry; return a book dict or None to skip.

        On a 304 (or otherwise unchanged) response we return None and the
        caller skips the upload. On 200 we parse the zip header, decide
        whether the newly-parsed revision date overtakes what OLDP has,
        and return a book dict if an upload is warranted.
        """
        prev = self._state.get(url_slug, {})
        since = prev.get("http_last_modified")

        try:
            resp = self._conditional_get_zip(zip_url, since)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch %s: %s", zip_url, exc)
            return None

        if resp.status_code == 304:
            # Body unchanged on the gii side. Two sub-cases:
            #
            # (a) OLDP already has this book at the recorded revision →
            #     truly nothing to do, skip.
            # (b) OLDP doesn't have it, or has an older revision (e.g. a
            #     previous run died mid-upload) → re-parse from the cached
            #     zip and queue an upload. This is what makes runs
            #     resumable: state alone isn't enough; we cross-check
            #     against OLDP's actual ``?latest=true`` snapshot.
            jurabk = self._mapping.get(url_slug, prev.get("jurabk"))
            cached_zip = self._zip_path(url_slug)
            oldp_revision = oldp_books.get(jurabk) if jurabk else None
            recorded = prev.get("oldp_revision_date")

            if jurabk and oldp_revision and recorded and oldp_revision >= recorded:
                logger.debug("Unchanged (304), oldp up-to-date: %s", url_slug)
                prev["oldp_revision_date"] = oldp_revision
                self._state[url_slug] = prev
                return None

            if not (jurabk and recorded and os.path.isfile(cached_zip)):
                # Missing pieces — treat as if we never saw this slug.
                logger.debug(
                    "Unchanged (304) but cache incomplete for %s — skipping",
                    url_slug,
                )
                return None

            logger.info(
                "Resume: 304 from gii but oldp lacks %s @ %s — re-uploading "
                "from cached zip",
                jurabk,
                recorded,
            )
            try:
                with open(cached_zip, "rb") as f:
                    book, _laws = parse_gii_zip(f.read())
            except (GiiParseError, OSError) as exc:
                logger.warning("Failed to re-parse cached %s: %s", url_slug, exc)
                return None
            revision_date = book.get("revision_date") or recorded
            self._book_to_zip[(jurabk, revision_date)] = cached_zip
            return {
                "code": jurabk,
                "title": book.get("title") or toc_title or jurabk,
                "revision_date": revision_date,
                "changelog": book.get("changelog"),
                "footnotes": book.get("footnotes"),
            }

        # 200: parse, decide, persist.
        new_last_modified = resp.headers.get("Last-Modified") or since or ""

        try:
            zip_path = self._save_zip(url_slug, resp.content)
            book, _laws = parse_gii_zip(resp.content)
        except (GiiParseError, OSError) as exc:
            logger.warning("Failed to parse %s: %s", url_slug, exc)
            return None

        jurabk = book["code"]
        revision_date = book.get("revision_date") or _http_date_to_iso(
            new_last_modified
        )
        if not revision_date:
            logger.warning(
                "Could not determine revision_date for %s — skipping", url_slug
            )
            return None

        self._mapping[url_slug] = jurabk

        oldp_revision = oldp_books.get(jurabk)
        new_state = {
            "jurabk": jurabk,
            "http_last_modified": new_last_modified,
            "oldp_revision_date": oldp_revision or revision_date,
        }

        if oldp_revision and oldp_revision >= revision_date and not self.force_full:
            logger.debug(
                "OLDP already has %s @ %s (parsed %s) — no upload needed",
                jurabk,
                oldp_revision,
                revision_date,
            )
            self._state[url_slug] = new_state
            return None

        # An upload is warranted. Hand the zip path off to get_laws().
        self._book_to_zip[(jurabk, revision_date)] = zip_path
        new_state["oldp_revision_date"] = revision_date
        self._state[url_slug] = new_state

        return {
            "code": jurabk,
            "title": book.get("title") or toc_title or jurabk,
            "revision_date": revision_date,
            "changelog": book.get("changelog"),
            "footnotes": book.get("footnotes"),
        }

    # ------------------------------------------------------------------ #
    # LawProvider API
    # ------------------------------------------------------------------ #

    def get_law_books(self) -> list[dict[str, Any]]:
        oldp_books = self._fetch_oldp_latest_books()
        entries = self._fetch_toc()
        if self.limit:
            entries = entries[: self.limit]
            logger.info("Limit applied — processing %d TOC entries", len(entries))

        books: list[dict[str, Any]] = []
        try:
            for idx, (url_slug, title, zip_url) in enumerate(entries, start=1):
                if idx % 100 == 0:
                    logger.info(
                        "Progress: %d/%d entries scanned, %d uploads queued",
                        idx,
                        len(entries),
                        len(books),
                    )
                book = self._process_entry(url_slug, title, zip_url, oldp_books)
                if book is not None:
                    books.append(book)
        finally:
            # Persist after every run, even on failure, so the next run
            # picks up where we left off.
            self._persist_state()

        logger.info("GII sweep done: %d new/updated book(s) to upload", len(books))
        return books

    def get_laws(self, book_code: str, revision_date: str) -> list[dict[str, Any]]:
        zip_path = self._book_to_zip.get((book_code, revision_date))
        if not zip_path or not os.path.isfile(zip_path):
            logger.warning(
                "No cached zip for (%s, %s) — returning empty law list",
                book_code,
                revision_date,
            )
            return []
        with open(zip_path, "rb") as f:
            zip_bytes = f.read()
        try:
            book, laws = parse_gii_zip(zip_bytes)
        except GiiParseError as exc:
            logger.warning(
                "Failed to re-parse zip for (%s, %s): %s",
                book_code,
                revision_date,
                exc,
            )
            return []
        # Stamp the revision_date on each law so the API attaches them to
        # the right book revision (the API would otherwise default to
        # whatever is currently latest, which is racy).
        for law in laws:
            law["revision_date"] = revision_date
        return laws
