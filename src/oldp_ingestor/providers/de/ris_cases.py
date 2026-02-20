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

logger = logging.getLogger(__name__)


class RISCaseProvider(RISBaseClient, CaseProvider):
    """Fetches case law from the RIS API.

    Args:
        court: Optional court code filter (e.g. ``"BGH"``).
        date_from: Only fetch decisions on or after this date (ISO 8601).
        date_to: Only fetch decisions on or before this date (ISO 8601).
        limit: Maximum number of cases to return (client-side).
        request_delay: Delay in seconds between API requests.
    """

    SOURCE = {
        "name": "Rechtsinformationssystem des Bundes (RIS)",
        "homepage": BASE_URL,
    }

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(request_delay=request_delay)
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

        If the courts endpoint fails, an empty dict is cached for the lifetime
        of this provider instance (one CLI run).  Subsequent calls fall back to
        the raw court code — this is intentional fail-open behaviour that
        preserves data with degraded court names rather than aborting the run.
        """
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
    def _map_case(item: dict, content: str, abstract: str | None) -> dict:
        """Map RIS case-law fields to OLDP case dict format."""
        file_numbers = item.get("fileNumbers", [])
        file_number = file_numbers[0] if file_numbers else ""

        case: dict = {
            "court_name": item.get("courtName", ""),
            "file_number": file_number,
            "date": item.get("decisionDate", ""),
            "content": content,
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
            if self.date_from:
                params["decisionDateFrom"] = self.date_from
            if self.date_to:
                params["decisionDateTo"] = self.date_to

            data = self._get_json("/v1/case-law", **params)
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

            for member in members:
                item = member.get("item", member)
                document_number = item.get("documentNumber", "")

                if not document_number:
                    logger.debug("Skipping case with missing documentNumber")
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

                case = self._map_case(item, content, abstract)
                cases.append(case)

                if self.limit and len(cases) >= self.limit:
                    return cases

            # Check if there's a next page
            view = data.get("view", {})
            if not view.get("next"):
                break
            page_index += 1

        return cases
