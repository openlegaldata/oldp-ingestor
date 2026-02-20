import json

from oldp_ingestor.providers.base import CaseProvider

CASE_FIELDS = (
    "file_number",
    "date",
    "content",
    "type",
    "ecli",
    "abstract",
    "title",
)


class DummyCaseProvider(CaseProvider):
    """Loads case data from a Django fixture JSON file."""

    SOURCE = {"name": "Dummy", "homepage": ""}

    def __init__(self, path: str):
        with open(path) as f:
            self.fixtures = json.load(f)

        # Build a lookup from court pk -> court name
        self.court_lookup: dict[int, str] = {}
        for entry in self.fixtures:
            if entry["model"] == "courts.court":
                self.court_lookup[entry["pk"]] = entry["fields"]["name"]

    def get_cases(self) -> list[dict]:
        cases = []
        for entry in self.fixtures:
            if entry["model"] != "cases.case":
                continue
            fields = entry["fields"]
            case = {k: fields[k] for k in CASE_FIELDS if k in fields}
            court_pk = fields.get("court")
            case["court_name"] = self.court_lookup.get(
                court_pk, f"Unknown court (pk={court_pk})"
            )
            cases.append(case)
        return cases
