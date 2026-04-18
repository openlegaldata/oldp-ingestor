class Provider:
    """Base class for all data providers."""

    pass


class LawProvider(Provider):
    """Base class for law data providers."""

    SOURCE: dict = {"name": "", "homepage": ""}

    def get_law_books(self) -> list[dict]:
        raise NotImplementedError

    def get_laws(self, book_code: str, revision_date: str) -> list[dict]:
        raise NotImplementedError


class CaseProvider(Provider):
    """Base class for case data providers."""

    SOURCE: dict = {"name": "", "homepage": ""}
    date_from: str = ""
    date_to: str = ""

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
