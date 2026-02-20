import json
import os
import re

from oldp_ingestor.sinks.base import Sink


def _sanitize_filename(name: str) -> str:
    """Replace unsafe characters with underscores, collapse duplicates."""
    if not name:
        return "unnamed"
    result = re.sub(r'[/\\<>:"|?*\s]', "_", name)
    result = re.sub(r"_+", "_", result)
    result = result.strip("_.")
    return result or "unnamed"


class JSONFileSink(Sink):
    """Sink that writes each entity as a JSON file in a directory tree."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def _write_json(self, path: str, data: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def write_law_book(self, book: dict) -> None:
        code = _sanitize_filename(book.get("code", "unknown"))
        path = os.path.join(self.output_dir, "law_books", f"{code}.json")
        self._write_json(path, book)

    def write_law(self, law: dict) -> None:
        book_code = _sanitize_filename(
            law.get("book_code") or law.get("book", "unknown")
        )
        slug = _sanitize_filename(law.get("slug") or law.get("section", "unknown"))
        path = os.path.join(self.output_dir, "laws", book_code, f"{slug}.json")
        self._write_json(path, law)

    def write_case(self, case: dict) -> None:
        file_number = _sanitize_filename(case.get("file_number", "unknown"))
        path = os.path.join(self.output_dir, "cases", f"{file_number}.json")
        self._write_json(path, case)
