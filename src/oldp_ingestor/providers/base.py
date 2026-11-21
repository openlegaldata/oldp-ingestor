import logging

from oldp_ingestor.providers.failure_tracker import (
    FailureTracker,
    NullFailureTracker,
)


class Provider:
    """Base class for all data providers."""

    # Default no-op tracker; CLI replaces with a real ``FailureTracker``
    # when ``--state-dir`` is configured. Providers can call
    # ``self.failure_tracker.should_skip(doc_id)`` / ``record_failure(...)``
    # / ``record_success(...)`` unconditionally — see
    # ``providers/failure_tracker.py``.
    failure_tracker: FailureTracker | NullFailureTracker = NullFailureTracker()

    #: Number of upstream failures that kept the provider from listing or
    #: fetching documents (search endpoint down, SPA never rendered, ...).
    #: Providers swallow these so one flaky source never crashes a run, but
    #: the CLI adds the count to the run's ``errors`` so the result file says
    #: ``partial`` instead of a clean ``ok`` with zero cases — otherwise a
    #: source can stay broken for weeks without the status dashboard or the
    #: health check noticing. Use :meth:`record_source_error` to increment.
    source_errors: int = 0

    def record_source_error(self, msg: str, *args, logger=None) -> None:
        """Log *msg* at ERROR level and count it as a source error.

        Args:
            msg: ``%``-style log message.
            *args: Arguments for *msg*.
            logger: Logger to emit on; defaults to the provider module's
                logger so the log line keeps the provider's name.
        """
        log = logger or logging.getLogger(type(self).__module__)
        log.error(msg, *args)
        self.source_errors += 1


class LawProvider(Provider):
    """Base class for law data providers."""

    SOURCE: dict = {"name": "", "homepage": ""}

    def get_law_books(self) -> list[dict]:
        raise NotImplementedError

    def get_laws(self, book_code: str, revision_date: str) -> list[dict]:
        raise NotImplementedError

    def iter_law_books(self):
        """Yield law book dicts one at a time (streaming).

        Preferred over :meth:`get_law_books` for large catalogues (e.g. the
        gii provider's ~6,800 books) — letting the CLI start uploading
        the first book before the entire sweep completes keeps memory
        bounded and shifts a long backfill from "sweep 2h, then upload
        4h" to "interleave both, finish in roughly the slower of the two".

        Default implementation materialises :meth:`get_law_books` and
        yields from it, preserving compatibility for providers that
        don't benefit from streaming.
        """
        yield from self.get_law_books()


class CaseProvider(Provider):
    """Base class for case data providers."""

    SOURCE: dict = {"name": "", "homepage": ""}
    date_from: str = ""
    date_to: str = ""

    #: Days the cron wrapper (``scripts/ingest.sh``) looks back when it
    #: computes ``--date-from`` for incremental runs. ``None`` means "use the
    #: wrapper default" (7 days). Sources that publish decisions weeks after
    #: the decision date need a longer window, otherwise every decision is
    #: already older than ``--date-from`` by the time it appears upstream and
    #: the run silently finds nothing. Exposed via ``oldp-ingestor
    #: providers`` as ``lookback_days``; re-fetching is cheap because
    #: existing cases come back as 409 and are counted as ``skipped``.
    LOOKBACK_DAYS: int | None = None

    def _is_within_date_range(self, date_str: str) -> bool:
        """Check if *date_str* (YYYY-MM-DD) falls within the configured range.

        Returns ``True`` when no filters are set, or when *date_str* is
        missing/unparseable (to avoid silently dropping cases).
        """
        if not self.date_from and not self.date_to:
            return True
        if not date_str or len(date_str) < 10:
            return True
        if self.date_from and date_str < self.date_from:
            return False
        if self.date_to and date_str > self.date_to:
            return False
        return True

    def get_cases(self) -> list[dict]:
        """Return list of case dicts for the cases API.

        Each dict must contain:
          - court_name (str): court name for API resolution
          - file_number (str): court file number
          - date (str): YYYY-MM-DD
          - content (str): HTML content
          - source_url (str): URL the content was extracted from (the PDF
            URL, HTML detail page, or API endpoint as appropriate). This
            is a required field on the OLDP ``Case`` model.

        Optional keys: type, ecli, abstract, title, source

        Subclasses must implement at least one of ``get_cases()`` or
        ``iter_cases()``. The default ``iter_cases()`` yields from this.
        """
        raise NotImplementedError

    def iter_cases(self):
        """Yield case dicts one at a time (streaming).

        Preferred over ``get_cases()`` for large datasets — avoids building
        an unbounded in-memory list. The CLI consumes this as a stream and
        writes each case to the sink immediately.

        Default implementation materialises ``get_cases()`` and yields
        from it, preserving backwards compatibility. Override directly to
        avoid materialisation for providers that otherwise accumulate many
        cases (e.g. the juris portals, rii).
        """
        yield from self.get_cases()
