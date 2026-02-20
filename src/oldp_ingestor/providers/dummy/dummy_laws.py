import json

from oldp_ingestor.providers.base import LawProvider

LAWBOOK_FIELDS = (
    "code",
    "title",
    "revision_date",
    "order",
    "changelog",
    "footnotes",
    "sections",
)
LAW_FIELDS = (
    "section",
    "title",
    "content",
    "slug",
    "order",
    "amtabk",
    "kurzue",
    "doknr",
    "footnotes",
)


class DummyLawProvider(LawProvider):
    """Loads law data from a Django fixture JSON file."""

    SOURCE = {"name": "Dummy", "homepage": ""}

    def __init__(self, path: str):
        with open(path) as f:
            self.fixtures = json.load(f)

        # Build a lookup from lawbook pk -> (code, revision_date)
        self.book_lookup: dict[int, tuple[str, str]] = {}
        for entry in self.fixtures:
            if entry["model"] == "laws.lawbook":
                pk = entry["pk"]
                fields = entry["fields"]
                self.book_lookup[pk] = (fields["code"], fields["revision_date"])

    def get_law_books(self) -> list[dict]:
        books = []
        for entry in self.fixtures:
            if entry["model"] != "laws.lawbook":
                continue
            fields = entry["fields"]
            book = {k: fields[k] for k in LAWBOOK_FIELDS if k in fields}
            books.append(book)
        return books

    def get_laws(self, book_code: str, revision_date: str) -> list[dict]:
        # Find lawbook pk(s) matching this code + revision_date
        matching_pks = {
            pk
            for pk, (code, rev) in self.book_lookup.items()
            if code == book_code and rev == revision_date
        }

        laws = []
        for entry in self.fixtures:
            if entry["model"] != "laws.law":
                continue
            fields = entry["fields"]
            if fields["book"] not in matching_pks:
                continue
            law = {k: fields.get(k) for k in LAW_FIELDS}
            law["book_code"] = book_code
            law["revision_date"] = revision_date
            laws.append(law)
        return laws
