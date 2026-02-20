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

    def get_cases(self) -> list[dict]:
        """Return list of case dicts for the cases API.

        Each dict must contain:
          - court_name (str): court name for API resolution
          - file_number (str): court file number
          - date (str): YYYY-MM-DD
          - content (str): HTML content

        Optional keys: type, ecli, abstract, title, source
        """
        raise NotImplementedError
