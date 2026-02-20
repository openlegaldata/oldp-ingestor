import json

import pytest
import requests

from oldp_ingestor.sinks.base import Sink
from oldp_ingestor.sinks.api import ApiSink
from oldp_ingestor.sinks.json_file import JSONFileSink, _sanitize_filename


# --- Sink ABC ---


class TestSinkABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Sink()

    def test_must_implement_all_methods(self):
        class PartialSink(Sink):
            def write_law_book(self, book):
                pass

        with pytest.raises(TypeError):
            PartialSink()

    def test_concrete_subclass(self):
        class ConcreteSink(Sink):
            def write_law_book(self, book):
                pass

            def write_law(self, law):
                pass

            def write_case(self, case):
                pass

        sink = ConcreteSink()
        assert isinstance(sink, Sink)


# --- ApiSink ---


class TestApiSink:
    def test_write_law_book(self):
        calls = []

        class FakeClient:
            def post(self, path, data):
                calls.append((path, data))
                return {}

        sink = ApiSink(client=FakeClient())
        sink.write_law_book({"code": "BGB"})
        assert calls == [("/api/law_books/", {"code": "BGB"})]

    def test_write_law(self):
        calls = []

        class FakeClient:
            def post(self, path, data):
                calls.append((path, data))
                return {}

        sink = ApiSink(client=FakeClient())
        sink.write_law({"section": "§ 1"})
        assert calls == [("/api/laws/", {"section": "§ 1"})]

    def test_write_case(self):
        calls = []

        class FakeClient:
            def post(self, path, data):
                calls.append((path, data))
                return {}

        sink = ApiSink(client=FakeClient())
        sink.write_case({"file_number": "I ZR 1/21"})
        assert calls == [("/api/cases/", {"file_number": "I ZR 1/21"})]

    def test_raises_http_error(self):
        class FakeResponse:
            status_code = 500

        class FakeClient:
            def post(self, path, data):
                raise requests.HTTPError(response=FakeResponse())

        sink = ApiSink(client=FakeClient())
        with pytest.raises(requests.HTTPError):
            sink.write_law_book({"code": "X"})


# --- _sanitize_filename ---


class TestSanitizeFilename:
    def test_basic_name(self):
        assert _sanitize_filename("BGB") == "BGB"

    def test_slashes(self):
        assert _sanitize_filename("I ZR 1/21") == "I_ZR_1_21"

    def test_backslashes(self):
        assert _sanitize_filename("a\\b") == "a_b"

    def test_spaces(self):
        assert _sanitize_filename("hello world") == "hello_world"

    def test_empty_string(self):
        assert _sanitize_filename("") == "unnamed"

    def test_consecutive_underscores(self):
        assert _sanitize_filename("a///b") == "a_b"

    def test_colons(self):
        assert _sanitize_filename("a:b") == "a_b"

    def test_leading_dots(self):
        assert _sanitize_filename(".hidden") == "hidden"

    def test_question_marks(self):
        assert _sanitize_filename("a?b") == "a_b"

    def test_only_special_chars(self):
        assert _sanitize_filename("///") == "unnamed"

    def test_unicode_preserved(self):
        assert _sanitize_filename("Urteil-über") == "Urteil-über"


# --- JSONFileSink ---


class TestJSONFileSink:
    def test_write_law_book(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        book = {"code": "BGB", "title": "Bürgerliches Gesetzbuch"}
        sink.write_law_book(book)

        path = tmp_path / "law_books" / "BGB.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data == book

    def test_write_law(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        law = {
            "book_code": "BGB",
            "slug": "s-1",
            "section": "§ 1",
            "content": "<p>X</p>",
        }
        sink.write_law(law)

        path = tmp_path / "laws" / "BGB" / "s-1.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data == law

    def test_write_law_falls_back_to_section(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        law = {"book_code": "BGB", "section": "§ 1", "content": "<p>X</p>"}
        sink.write_law(law)

        path = tmp_path / "laws" / "BGB" / "§_1.json"
        assert path.exists()

    def test_write_case(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        case = {"file_number": "I ZR 1/21", "date": "2024-01-15"}
        sink.write_case(case)

        path = tmp_path / "cases" / "I_ZR_1_21.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data == case

    def test_overwrites_existing(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        sink.write_law_book({"code": "BGB", "title": "Old"})
        sink.write_law_book({"code": "BGB", "title": "New"})

        path = tmp_path / "law_books" / "BGB.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["title"] == "New"

    def test_creates_nested_dirs(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        law = {"book_code": "X/Y", "slug": "s-1", "content": "C"}
        sink.write_law(law)

        path = tmp_path / "laws" / "X_Y" / "s-1.json"
        assert path.exists()

    def test_missing_fields_use_unknown(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        sink.write_law_book({})
        path = tmp_path / "law_books" / "unknown.json"
        assert path.exists()

        sink.write_case({})
        path = tmp_path / "cases" / "unknown.json"
        assert path.exists()

    def test_unicode_content(self, tmp_path):
        sink = JSONFileSink(str(tmp_path))
        book = {"code": "BGB", "title": "Bürgerliches Gesetzbuch — Fassung"}
        sink.write_law_book(book)

        path = tmp_path / "law_books" / "BGB.json"
        content = path.read_text(encoding="utf-8")
        assert "Bürgerliches" in content
        assert "\\u" not in content  # ensure_ascii=False
