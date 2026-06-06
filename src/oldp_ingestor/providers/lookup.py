"""Targeted citation-based lookup mixin.

The streaming ``iter_cases`` flow paginates an upstream catalogue and yields
everything it finds. This module adds a complementary entry point used by
agents (e.g. Claude Code) to fetch a *specific* decision when OLDP could
not resolve an extracted citation. The contract:

* One upstream request per call. No pagination loops, no full-portal
  crawls — the agent decides routing, the provider just executes the
  query.
* Three-status result everywhere (``ok`` / ``not_found`` / ``error``) so
  the agent can distinguish "upstream said no" from "upstream broke".
* Lightweight candidates from :meth:`LookupMixin.lookup_search`, full
  case dict (same shape as ``iter_cases``) from
  :meth:`LookupMixin.lookup_fetch`. The agent picks one candidate, then
  fetches.

Court coverage is declared per provider as a *filter* against the OLDP
prod courts API — the actual court list is resolved at runtime so it
tracks whatever OLDP currently knows about. The provider declaration
only encodes routing intent ("RIS handles all federal courts"), not the
court records themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(frozen=True)
class LookupCapability:
    """Declarative capability for citation-based lookup on one provider.

    Attributes:
        keys: Subset of ``{"file_number", "ecli", "court"}`` — the search
            arguments this provider accepts. Calling
            :meth:`LookupMixin.lookup_search` with any other key set
            yields an ``error`` response.
        court_filter: Filter spec used to resolve provider coverage
            against the OLDP courts API. Supported keys:

            * ``court_types``: list of OLDP ``court_type`` codes
              (e.g. ``["BGH","BFH"]``).
            * ``state_ids``: list of OLDP ``state`` IDs
              (e.g. ``[12]`` = Nordrhein-Westfalen).
            * ``slugs``: explicit list of OLDP court slugs
              (e.g. ``["bgh"]``) for the rare case neither of the above
              matches cleanly.

            All filters are OR-combined: a court matches the provider if
            *any* of its declarations match.
        cost: Coarse cost hint for the agent's planner.
            ``"low"`` = single HTTP request, no JS.
            ``"medium"`` = a few requests / SSR scrape.
            ``"high"`` = headless browser (Playwright).
    """

    keys: tuple[str, ...]
    court_filter: dict = field(default_factory=dict)
    cost: str = "low"


SUPPORTED_KEYS = frozenset({"file_number", "ecli", "court"})
SUPPORTED_COSTS = frozenset({"low", "medium", "high"})


def validate_capability(cap: LookupCapability) -> None:
    """Sanity-check a capability declaration. Raises ``ValueError`` on issues."""
    unknown_keys = set(cap.keys) - SUPPORTED_KEYS
    if unknown_keys:
        raise ValueError(f"Unknown lookup keys: {sorted(unknown_keys)}")
    if cap.cost not in SUPPORTED_COSTS:
        raise ValueError(f"Unknown cost tier: {cap.cost!r}")
    unknown_filters = set(cap.court_filter) - {"court_types", "state_ids", "slugs"}
    if unknown_filters:
        raise ValueError(f"Unknown court_filter keys: {sorted(unknown_filters)}")


class LookupMixin:
    """Mixin for case providers that support targeted citation-based lookup.

    Subclasses declare ``LOOKUP_CAPABILITY`` and implement
    :meth:`lookup_search` / :meth:`lookup_fetch`. The base class methods
    raise :class:`NotImplementedError` so providers that genuinely cannot
    do targeted lookup (e.g. a portal with no search-by-AZ) just don't
    inherit from this mixin and are excluded from ``lookup providers``
    output.
    """

    LOOKUP_CAPABILITY: ClassVar[LookupCapability]

    def lookup_search(
        self,
        file_number: str | None = None,
        ecli: str | None = None,
        court_hint: str | None = None,
        date: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Single upstream search; return lightweight candidates.

        Each candidate dict contains:

        * ``doc_id`` (str): opaque identifier; pass to
          :meth:`lookup_fetch` to retrieve the full case.
        * ``court_name`` (str)
        * ``file_number`` (str)
        * ``date`` (str): ``YYYY-MM-DD`` or empty.
        * ``ecli`` (str): may be empty.
        * ``type`` (str): may be empty.
        * ``snippet`` (str): a short truncated content excerpt the agent
          uses for sanity-checking the match. May be empty when the
          search response doesn't include text.

        Returning ``[]`` means the search ran but found nothing. Network
        / upstream failures should propagate as exceptions — the CLI
        layer converts them to ``status: error``.
        """
        raise NotImplementedError

    def lookup_fetch(self, doc_id: str) -> dict | None:
        """Fetch the full case dict for ``doc_id``.

        Returns the same dict shape as
        :meth:`~oldp_ingestor.providers.base.CaseProvider.iter_cases`
        yields, so the CLI's ``lookup ingest`` can hand it straight to
        the OLDP sink. Returns ``None`` when the doc exists in the
        search index but the detail page is unavailable / unparseable
        (rare — should usually raise instead).
        """
        raise NotImplementedError


def filter_courts(all_courts: list[dict], cap: LookupCapability) -> list[dict]:
    """Apply a capability's ``court_filter`` to a fetched OLDP courts list.

    Returns the subset of *all_courts* matching at least one of the
    declared selectors. Preserves input order; deduplicates by ``id``.
    """
    if not cap.court_filter:
        return []

    wanted_types = set(cap.court_filter.get("court_types", []))
    wanted_states = set(cap.court_filter.get("state_ids", []))
    wanted_slugs = set(cap.court_filter.get("slugs", []))

    matched: list[dict] = []
    seen_ids: set[int] = set()
    for court in all_courts:
        cid = court.get("id")
        if cid in seen_ids:
            continue
        if (
            (wanted_types and court.get("court_type") in wanted_types)
            or (wanted_states and court.get("state") in wanted_states)
            or (wanted_slugs and court.get("slug") in wanted_slugs)
        ):
            seen_ids.add(cid)
            matched.append(court)
    return matched


def summarise_court(court: dict) -> dict:
    """Project an OLDP court record down to the fields the agent needs."""
    return {
        "id": court.get("id"),
        "name": court.get("name", ""),
        "code": court.get("code", ""),
        "slug": court.get("slug", ""),
        "court_type": court.get("court_type", ""),
        "jurisdiction": court.get("jurisdiction", ""),
        "level_of_appeal": court.get("level_of_appeal", ""),
        "state": court.get("state"),
    }
