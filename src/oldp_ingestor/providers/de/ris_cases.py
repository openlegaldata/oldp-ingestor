"""RIS case law provider.

Fetches court decisions from the Rechtsinformationsportal des Bundes
(RIS) API endpoint ``GET /v1/case-law``.

API docs: https://docs.rechtsinformationen.bund.de/
"""

import logging

import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.de.ris_common import (
    BASE_URL,
    MAX_PAGE_SIZE,
    MIN_CONTENT_LENGTH,
    RISBaseClient,
    extract_body,
)
from oldp_ingestor.providers.lookup import LookupCapability, LookupMixin

logger = logging.getLogger(__name__)


# All federal courts indexed by the RIS API. The codes match both the
# RIS ``courtType`` filter and the OLDP ``court_type`` column, so a
# single list serves the upstream query and the OLDP-side coverage
# declaration used by the lookup mixin.
_RIS_COURT_TYPES = ("BGH", "BFH", "BVerwG", "BAG", "BSG", "BPatG", "BVerfG")


# Override court labels that the RIS API returns with a redundant
# seat-city suffix. The API's ``/v1/case-law/courts`` endpoint emits
# entries like ``id="BGH Karlsruhe" label="Bundesgerichtshof Karlsruhe"``
# for every German federal court — the trailing city is the court's
# seat, not part of the institutional name. Posting the label verbatim
# to the OLDP API yielded 400 ``court_not_found`` (e.g. prod
# 2026-05-28 02:15:13 for ``Bundesgerichtshof Karlsruhe``: OLDP only
# knows the canonical name without the seat suffix).
#
# Keys are RIS court codes (the ``courtName`` field on each case item
# and the ``id`` field on the courts endpoint — both carry the same
# seat suffix). Values are the canonical OLDP court names.
_RIS_COURT_LABEL_OVERRIDES: dict[str, str] = {
    "BGH Karlsruhe": "Bundesgerichtshof",
    "BFH München": "Bundesfinanzhof",
    "BVerwG Leipzig": "Bundesverwaltungsgericht",
    "BPatG München": "Bundespatentgericht",
    "BAG Erfurt": "Bundesarbeitsgericht",
    "BSG Kassel": "Bundessozialgericht",
    "BVerfG Karlsruhe": "Bundesverfassungsgericht",
}


class RISCaseProvider(LookupMixin, RISBaseClient, CaseProvider):
    """Fetches case law from the RIS API.

    The RIS API does **not** honour ``decisionDateFrom`` / ``decisionDateTo``
    query parameters — it always returns the most recent 10,000 decisions
    regardless of any date filter.  Date filtering is therefore applied
    client-side via :meth:`~oldp_ingestor.providers.base.CaseProvider._is_within_date_range`.

    Because the API returns results in **descending** date order (newest
    first), iteration stops as soon as a decision date falls before
    ``date_from``, avoiding unnecessary page fetches.

    Args:
        court: Optional court code filter (e.g. ``"BGH"``).
        date_from: Optional start date filter (ISO 8601). Applied client-side.
        date_to: Optional end date filter (ISO 8601). Applied client-side.
        limit: Maximum number of cases to return (client-side).
        request_delay: Delay in seconds between API requests.
        proxy: Optional HTTP proxy URL (e.g. ``"http://user:pass@host:port"``).
    """

    SOURCE = {
        "name": "Rechtsinformationssystem des Bundes (RIS)",
        "homepage": BASE_URL,
    }

    # Citation-based lookup capability. ``court_types`` doubles as both
    # the OLDP coverage declaration (these court_type codes are the
    # exact federal-court codes OLDP carries) and the upstream filter
    # used in :meth:`lookup_search`.
    LOOKUP_CAPABILITY = LookupCapability(
        keys=("file_number", "ecli", "court"),
        court_filter={"court_types": list(_RIS_COURT_TYPES)},
        cost="low",
    )

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(request_delay=request_delay, proxy=proxy)
        self.court = court
        self.date_from = date_from
        self.date_to = date_to
        self.limit = limit
        self._court_labels: dict[str, str] | None = None

    def _fetch_court_labels(self) -> dict[str, str]:
        """Fetch court code-to-label mapping from ``GET /v1/case-law/courts``.

        The endpoint returns a JSON list of objects with ``id`` (court code)
        and ``label`` (full name).

        Returns:
            Dict mapping court codes (e.g. ``"BGH"``) to full names
            (e.g. ``"Bundesgerichtshof"``).
        """
        data = self._get_json("/v1/case-law/courts")
        # Response is a plain list, not a hydra collection
        courts = data if isinstance(data, list) else data.get("member", [])
        return {c["id"]: c["label"] for c in courts if "id" in c and "label" in c}

    def _resolve_court_name(self, court_code: str) -> str:
        """Resolve a court code to its full name, with lazy loading and fallback.

        Overrides in :data:`_RIS_COURT_LABEL_OVERRIDES` take precedence —
        the RIS API ships federal-court labels with a seat-city suffix
        that OLDP cannot resolve (see the override table for context).

        If the courts endpoint fails, an empty dict is cached for the lifetime
        of this provider instance (one CLI run).  Subsequent calls fall back to
        the raw court code — this is intentional fail-open behaviour that
        preserves data with degraded court names rather than aborting the run.
        """
        if court_code in _RIS_COURT_LABEL_OVERRIDES:
            return _RIS_COURT_LABEL_OVERRIDES[court_code]
        if self._court_labels is None:
            try:
                self._court_labels = self._fetch_court_labels()
            except requests.RequestException as exc:
                logger.warning("Failed to fetch court labels: %s", exc)
                self._court_labels = {}
        return self._court_labels.get(court_code, court_code)

    def _fetch_case_html(self, document_number: str) -> str | None:
        """Fetch HTML content for a case decision.

        Args:
            document_number: The RIS document number.

        Returns:
            Extracted body HTML, or ``None`` on failure.
        """
        try:
            html = self._get_text(f"/v1/case-law/{document_number}.html")
            body = extract_body(html)
            if len(body) < MIN_CONTENT_LENGTH:
                logger.debug(
                    "Skipping %s: content too short (%d chars)",
                    document_number,
                    len(body),
                )
                return None
            return body
        except requests.RequestException as exc:
            logger.warning("Failed to fetch HTML for case %s: %s", document_number, exc)
            return None

    def _fetch_case_detail(self, document_number: str) -> dict | None:
        """Fetch case detail JSON for abstract fields.

        Returns:
            Detail dict, or ``None`` on failure.
        """
        try:
            return self._get_json(f"/v1/case-law/{document_number}")
        except requests.RequestException as exc:
            logger.debug("Failed to fetch detail for case %s: %s", document_number, exc)
            return None

    @staticmethod
    def _build_abstract(detail: dict) -> str | None:
        """Extract the best abstract text from a case detail response.

        Prefers ``guidingPrinciple``, then ``headnote``, ``otherHeadnote``,
        and finally ``tenor``.  Returns ``None`` if all are empty.
        Truncates to 50 000 characters.
        """
        for field in ("guidingPrinciple", "headnote", "otherHeadnote", "tenor"):
            value = detail.get(field)
            if value and isinstance(value, str) and value.strip():
                return value.strip()[:50000]
        return None

    @staticmethod
    def _map_case(
        item: dict, content: str, abstract: str | None, source_url: str = ""
    ) -> dict:
        """Map RIS case-law fields to OLDP case dict format."""
        file_numbers = item.get("fileNumbers", [])
        file_number = file_numbers[0] if file_numbers else ""

        case: dict = {
            "court_name": item.get("courtName", ""),
            "file_number": file_number,
            "date": item.get("decisionDate", ""),
            "content": content,
            "source_url": source_url,
        }

        # Optional fields
        if item.get("documentType"):
            case["type"] = item["documentType"]
        if item.get("ecli"):
            case["ecli"] = item["ecli"]
        if item.get("headline"):
            case["title"] = item["headline"]
        if abstract:
            case["abstract"] = abstract

        return case

    def get_cases(self) -> list[dict]:
        """Fetch case law from ``GET /v1/case-law``.

        Paginates through the list endpoint, fetches HTML content and
        detail for each decision, and returns mapped case dicts.
        """
        cases = []
        page_index = 0
        total_items = None

        while True:
            params: dict = {"size": MAX_PAGE_SIZE, "pageIndex": page_index}
            if self.court:
                params["courtType"] = self.court
            # Note: decisionDateFrom / decisionDateTo are ignored by the RIS API.
            # Date filtering is applied client-side (see loop body below).

            try:
                data = self._get_json("/v1/case-law", **params)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 422:
                    # The API returns 422 when pageIndex exceeds the 10 000-item cap.
                    logger.debug(
                        "422 on page %d — reached API boundary, stopping", page_index
                    )
                    break
                raise
            members = data.get("member", [])

            if not members:
                break

            if total_items is None:
                total_items = data.get("totalItems", "?")
                logger.info("Fetching cases (total available: %s)...", total_items)

            logger.info(
                "Page %d: processing %d items (%d/%s)...",
                page_index,
                len(members),
                len(cases) + len(members),
                total_items,
            )

            done = False
            for member in members:
                item = member.get("item", member)
                document_number = item.get("documentNumber", "")

                if not document_number:
                    logger.debug("Skipping case with missing documentNumber")
                    continue

                # Client-side date filter (API ignores decisionDateFrom/To params).
                # Results are in descending date order, so once we pass date_from
                # all remaining pages are also out of range.
                decision_date = item.get("decisionDate", "")
                if not self._is_within_date_range(decision_date):
                    if (
                        self.date_from
                        and decision_date
                        and decision_date < self.date_from
                    ):
                        logger.info(
                            "Reached cases before date_from (%s) at %s, stopping",
                            self.date_from,
                            decision_date,
                        )
                        done = True
                        break
                    continue

                # Fetch HTML content
                content = self._fetch_case_html(document_number)
                if content is None:
                    continue

                # Fetch detail for abstract (optional)
                detail = self._fetch_case_detail(document_number)
                abstract = self._build_abstract(detail) if detail else None

                # Resolve court name
                court_code = item.get("courtName", "")
                if court_code:
                    item = dict(item)  # avoid mutating original
                    item["courtName"] = self._resolve_court_name(court_code)

                source_url = f"{self.base_url}/v1/case-law/{document_number}.html"
                case = self._map_case(item, content, abstract, source_url=source_url)
                cases.append(case)

                if self.limit and len(cases) >= self.limit:
                    return cases

            if done:
                break

            # Check if there's a next page
            view = data.get("view", {})
            if not view.get("next"):
                break
            page_index += 1

        return cases

    # ------------------------------------------------------------------
    # Targeted citation-based lookup (LookupMixin)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_file_number(fn: str) -> str:
        """Normalise an Aktenzeichen for exact-match comparison.

        Strips whitespace and collapses internal runs so ``"VI ZR 123/22"``
        and ``"VI  ZR 123/22"`` compare equal. Case-insensitive on the
        senate letters (lowercase is unusual but documents do appear with
        both forms).
        """
        return " ".join(fn.split()).lower()

    def lookup_search(
        self,
        file_number: str | None = None,
        ecli: str | None = None,
        court_hint: str | None = None,
        date: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Search RIS for candidates matching a citation.

        The RIS API has no dedicated ``fileNumber`` / ``ecli`` filter,
        but its ``searchTerm`` full-text query reliably matches both.
        Candidates are filtered client-side for *exact* file-number /
        ECLI hits to keep the result list small and unambiguous —
        without that filter ``searchTerm="VI ZR 127/22"`` returns 651
        fuzzy matches (verified prod 2026-06-06).

        ``court_hint`` is only applied when it matches a known RIS court
        code (``courtType`` filter); arbitrary court names are ignored
        rather than producing a confusing empty result.
        """
        if not (file_number or ecli):
            raise ValueError("lookup_search requires file_number or ecli")

        # Prefer the more specific identifier.
        params: dict = {"size": min(max(int(limit) * 3, 30), MAX_PAGE_SIZE)}
        params["searchTerm"] = ecli if ecli else file_number
        if court_hint and court_hint in _RIS_COURT_TYPES:
            params["courtType"] = court_hint

        data = self._get_json("/v1/case-law", **params)
        members = data.get("member", [])

        norm_fn = self._normalise_file_number(file_number) if file_number else ""

        candidates: list[dict] = []
        for member in members:
            item = member.get("item", member)
            item_ecli = item.get("ecli", "") or ""
            item_fns = [
                self._normalise_file_number(f) for f in item.get("fileNumbers", [])
            ]

            if ecli and item_ecli != ecli:
                continue
            if file_number and norm_fn not in item_fns:
                continue

            doc_number = item.get("documentNumber", "")
            if not doc_number:
                continue

            file_numbers = item.get("fileNumbers", [])
            candidates.append(
                {
                    "doc_id": doc_number,
                    "court_name": self._resolve_court_name(item.get("courtName", "")),
                    "file_number": file_numbers[0] if file_numbers else "",
                    "date": item.get("decisionDate", "") or "",
                    "ecli": item_ecli,
                    "type": item.get("documentType", "") or "",
                    "snippet": (item.get("headline", "") or "")[:200],
                }
            )
            if len(candidates) >= limit:
                break
        return candidates

    def lookup_fetch(self, doc_id: str) -> dict | None:
        """Fetch a full RIS case by ``documentNumber``.

        Reuses the same HTML + detail + abstract pipeline as
        :meth:`get_cases`, returning a case dict with the same shape.
        ``None`` is returned when the HTML body is missing or shorter
        than :data:`MIN_CONTENT_LENGTH` — both signal a structurally
        broken document that the caller should treat as ``not_found``
        rather than retrying.
        """
        content = self._fetch_case_html(doc_id)
        if content is None:
            return None
        detail = self._fetch_case_detail(doc_id) or {}
        # Compose a search-item-shaped dict for _map_case from the detail
        # payload (same field names; detail is a superset of search).
        item = {
            "courtName": self._resolve_court_name(detail.get("courtName", "") or ""),
            "fileNumbers": detail.get("fileNumbers", []),
            "decisionDate": detail.get("decisionDate", ""),
            "documentType": detail.get("documentType", ""),
            "ecli": detail.get("ecli", ""),
            "headline": detail.get("headline", ""),
        }
        abstract = self._build_abstract(detail) if detail else None
        source_url = f"{self.base_url}/v1/case-law/{doc_id}.html"
        return self._map_case(item, content, abstract, source_url=source_url)
