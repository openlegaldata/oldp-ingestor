"""Tests for the gii (gesetze-im-internet.de) law provider and parser."""

import io
import json
import os
import zipfile
from pathlib import Path

import pytest
import requests

from oldp_ingestor.providers.de.gii import (
    GII_TOC_URL,
    GiiLawProvider,
    _http_date,
    _http_date_to_iso,
    _slug_from_link,
)
from oldp_ingestor.providers.de.gii_parser import (
    GiiParseError,
    extract_xml_from_zip,
    parse_book_metadata,
    parse_gii_xml,
    parse_gii_zip,
)


RESOURCES = Path(__file__).parent / "resources" / "gii"


def _read_fixture(name: str) -> bytes:
    return (RESOURCES / name).read_bytes()


# ---------------------------------------------------------------------- #
# parser
# ---------------------------------------------------------------------- #


def test_extract_xml_from_zip_returns_bytes():
    xml = extract_xml_from_zip(_read_fixture("gg.zip"))
    assert xml.startswith(b"<?xml")
    assert b"<jurabk>GG</jurabk>" in xml


def test_extract_xml_from_zip_raises_on_empty_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w"):
        pass
    with pytest.raises(GiiParseError):
        extract_xml_from_zip(buf.getvalue())


def test_parse_book_metadata_gg():
    book = parse_book_metadata(extract_xml_from_zip(_read_fixture("gg.zip")))
    assert book["code"] == "GG"
    assert book["title"].startswith("Grundgesetz")
    # builddate=20250326... → 2025-03-26
    assert book["builddate"] == "2025-03-26"
    assert book["doknr"] == "BJNR000010949"
    # Either a parsed Stand date or fallback to builddate
    assert "revision_date" in book


def test_parse_book_metadata_baunvo_picks_stand_date():
    """BauNVO's 'Stand' line carries the most recent revision date."""
    book = parse_book_metadata(extract_xml_from_zip(_read_fixture("baunvo.zip")))
    assert book["code"] == "BauNVO"
    # 'zuletzt geändert durch Art. 2 G v. 3.7.2023 I Nr. 176' → 2023-07-03
    assert book["revision_date"] == "2023-07-03"


def test_parse_book_metadata_raises_on_wrong_root():
    with pytest.raises(GiiParseError):
        parse_book_metadata(b"<rootless/>")


def test_parse_book_metadata_raises_without_jurabk():
    bad = b"<dokumente><norm><metadaten/></norm></dokumente>"
    with pytest.raises(GiiParseError, match="jurabk"):
        parse_book_metadata(bad)


def test_parse_gii_xml_emits_book_and_laws():
    book, laws = parse_gii_xml(extract_xml_from_zip(_read_fixture("baunvo.zip")))
    assert book["code"] == "BauNVO"
    # changelog/footnotes are JSON-encoded strings
    assert isinstance(book["changelog"], str)
    parsed_changelog = json.loads(book["changelog"])
    assert any(c["type"] == "Stand" for c in parsed_changelog)
    # Each law carries the book code
    assert all(law["book_code"] == "BauNVO" for law in laws)
    assert laws  # non-empty
    # Laws have section labels (e.g. § 1, § 2) and ascending order
    sections = [law["section"] for law in laws]
    assert any(s.startswith("§") for s in sections)
    orders = [law["order"] for law in laws]
    assert orders == sorted(orders)


def test_parse_gii_zip_round_trip():
    book, laws = parse_gii_zip(_read_fixture("gg.zip"))
    assert book["code"] == "GG"
    # GG has many norms; sanity-check the count
    assert len(laws) > 100


# ---------------------------------------------------------------------- #
# provider helpers
# ---------------------------------------------------------------------- #


def test_slug_from_link_first_path_component():
    assert _slug_from_link("http://www.gesetze-im-internet.de/bgb/xml.zip") == "bgb"
    assert (
        _slug_from_link("https://www.gesetze-im-internet.de/sgb_5/xml.zip") == "sgb_5"
    )
    assert _slug_from_link("/bimschv_1_2010/xml.zip") == "bimschv_1_2010"
    assert _slug_from_link("") == ""


def test_http_date_round_trip():
    s = _http_date("2026-04-13")
    # Parsed back should yield the same date (we set 23:59:59 internally)
    assert _http_date_to_iso(s) == "2026-04-13"


def test_http_date_to_iso_handles_garbage():
    assert _http_date_to_iso("") is None
    assert _http_date_to_iso(None) is None
    assert _http_date_to_iso("not a date") is None


def test_provider_requires_cache_dir():
    with pytest.raises(ValueError, match="cache_dir is required"):
        GiiLawProvider(oldp_client=None, cache_dir="")


# ---------------------------------------------------------------------- #
# provider integration (mocked HTTP)
# ---------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, *, status_code=200, content=b"", text="", headers=None):
        self.status_code = status_code
        self.content = content
        self.text = text
        self.headers = headers or {}

    def json(self):
        raise NotImplementedError


@pytest.fixture
def cache_dir(tmp_path):
    return str(tmp_path / "cache")


@pytest.fixture
def gg_bytes():
    return _read_fixture("gg.zip")


@pytest.fixture
def baunvo_bytes():
    return _read_fixture("baunvo.zip")


@pytest.fixture
def toc_xml():
    return (RESOURCES / "gii-toc.xml").read_text(encoding="utf-8")


def _install_http_mock(monkeypatch, responses):
    """Patch HttpBaseClient._get to return queued responses keyed by URL."""

    def fake_get(self, url, **kwargs):
        if url in responses:
            value = responses[url]
            if callable(value):
                return value(kwargs)
            return value
        raise AssertionError(f"Unexpected GET {url}")

    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.HttpBaseClient._get", fake_get
    )


def test_get_law_books_cold_run(
    monkeypatch, cache_dir, toc_xml, gg_bytes, baunvo_bytes
):
    """First run: no cache, no oldp client — both zips are downloaded."""
    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(
            content=gg_bytes,
            headers={"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"},
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=baunvo_bytes,
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    books = provider.get_law_books()

    codes = [b["code"] for b in books]
    assert codes == ["GG", "BauNVO"]

    # Mapping cache populated
    assert provider._mapping == {"gg": "GG", "baunvo": "BauNVO"}
    # State persisted on disk after the run
    state = json.loads(Path(cache_dir, "done.json").read_text())
    assert "gg" in state and state["gg"]["jurabk"] == "GG"
    assert state["gg"]["http_last_modified"].startswith("Wed, 26 Mar 2025")
    # Zips persisted to disk
    assert Path(cache_dir, "zips", "gg.zip").exists()
    assert Path(cache_dir, "zips", "baunvo.zip").exists()

    # get_laws re-parses from the cached zip
    gg_laws = provider.get_laws("GG", books[0]["revision_date"])
    assert gg_laws and all(law["book_code"] == "GG" for law in gg_laws)
    assert all(law["revision_date"] == books[0]["revision_date"] for law in gg_laws)


def test_skips_when_oldp_already_has_revision(
    monkeypatch, cache_dir, toc_xml, gg_bytes, baunvo_bytes
):
    """OLDP already has BauNVO @ the parsed revision date; should be skipped."""
    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(
            content=gg_bytes,
            headers={"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"},
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=baunvo_bytes,
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    class FakeOldp:
        def get(self, path):
            return {
                "results": [
                    # BauNVO already has the same revision date — skip.
                    {"code": "BauNVO", "revision_date": "2023-07-03"},
                ],
                "next": None,
            }

    provider = GiiLawProvider(oldp_client=FakeOldp(), cache_dir=cache_dir)
    books = provider.get_law_books()

    codes = [b["code"] for b in books]
    assert codes == ["GG"]  # BauNVO skipped


def test_conditional_get_304_skips_without_parsing(
    monkeypatch, cache_dir, toc_xml, gg_bytes
):
    """Pre-populated state → 304 keeps zip untouched and emits no upload."""
    # Seed the state file so the provider sends If-Modified-Since.
    os.makedirs(cache_dir, exist_ok=True)
    state = {
        "gg": {
            "jurabk": "GG",
            "http_last_modified": "Wed, 26 Mar 2025 21:40:03 GMT",
            "oldp_revision_date": "2025-03-26",
        },
        "baunvo": {
            "jurabk": "BauNVO",
            "http_last_modified": "Thu, 17 Aug 2023 19:35:04 GMT",
            "oldp_revision_date": "2013-06-11",
        },
    }
    Path(cache_dir, "done.json").write_text(json.dumps(state))
    Path(cache_dir, "url_slug_to_jurabk.json").write_text(
        json.dumps({"gg": "GG", "baunvo": "BauNVO"})
    )

    captured_headers: dict[str, dict] = {}

    def fake_zip_response(kwargs):
        captured_headers["last"] = kwargs.get("headers", {})
        return _FakeResponse(status_code=304)

    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": fake_zip_response,
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": fake_zip_response,
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    books = provider.get_law_books()

    assert books == []
    # Confirm If-Modified-Since was sent
    assert captured_headers["last"].get("If-Modified-Since")


def test_force_full_skips_if_modified_since(
    monkeypatch, cache_dir, toc_xml, gg_bytes, baunvo_bytes
):
    """--full bypasses conditional GET even when state is present."""
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    Path(cache_dir, "done.json").write_text(
        json.dumps(
            {
                "gg": {
                    "jurabk": "GG",
                    "http_last_modified": "Wed, 26 Mar 2025 21:40:03 GMT",
                    "oldp_revision_date": "2025-03-26",
                }
            }
        )
    )

    captured_headers: dict[str, dict] = {}

    def fake_zip_response(content, headers):
        def _h(kwargs):
            captured_headers.setdefault("calls", []).append(kwargs.get("headers", {}))
            return _FakeResponse(content=content, headers=headers)

        return _h

    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": fake_zip_response(
            gg_bytes, {"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"}
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": fake_zip_response(
            baunvo_bytes, {"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"}
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir, force_full=True)
    provider.get_law_books()

    # No If-Modified-Since header should be present in any call
    for headers in captured_headers["calls"]:
        assert "If-Modified-Since" not in headers


def test_get_laws_without_cached_zip_returns_empty(cache_dir):
    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    assert provider.get_laws("GG", "2025-03-26") == []


def test_request_exception_is_logged_and_skipped(
    monkeypatch, cache_dir, toc_xml, baunvo_bytes
):
    def boom(kwargs):
        raise requests.RequestException("connection blew up")

    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": boom,
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=baunvo_bytes,
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    books = provider.get_law_books()

    # Only baunvo was successfully fetched and parsed
    assert [b["code"] for b in books] == ["BauNVO"]


def test_corrupt_state_file_is_recovered(monkeypatch, cache_dir, toc_xml, gg_bytes):
    """A garbled done.json should not crash the run — we start empty."""
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    Path(cache_dir, "done.json").write_text("{not valid json")

    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(
            content=gg_bytes,
            headers={"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"},
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=_read_fixture("baunvo.zip"),
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    assert provider._state == {}  # corrupt file ignored
    books = provider.get_law_books()
    assert {b["code"] for b in books} == {"GG", "BauNVO"}


def test_zip_parse_failure_is_skipped(monkeypatch, cache_dir, toc_xml, gg_bytes):
    """A corrupt zip body for one slug skips that slug, not the whole run."""
    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(
            content=b"not a valid zip",
            headers={"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"},
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=_read_fixture("baunvo.zip"),
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    books = provider.get_law_books()
    assert [b["code"] for b in books] == ["BauNVO"]


def test_get_laws_handles_corrupt_cached_zip(monkeypatch, cache_dir, toc_xml, gg_bytes):
    """If the cached zip on disk is later truncated, get_laws returns []."""
    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(
            content=gg_bytes,
            headers={"Last-Modified": "Wed, 26 Mar 2025 21:40:03 GMT"},
        ),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            content=_read_fixture("baunvo.zip"),
            headers={"Last-Modified": "Thu, 17 Aug 2023 19:35:04 GMT"},
        ),
    }
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=None, cache_dir=cache_dir)
    books = provider.get_law_books()
    # Corrupt the cached zip after the sweep
    Path(cache_dir, "zips", "gg.zip").write_bytes(b"junk")
    laws = provider.get_laws("GG", books[0]["revision_date"])
    assert laws == []


def test_resume_re_uploads_when_oldp_lacks_book(
    monkeypatch, cache_dir, toc_xml, gg_bytes, baunvo_bytes
):
    """304 + oldp missing the book → re-parse from cached zip and emit upload.

    This is the resumability story: a previous run died mid-upload after
    saving the zip and recording state, but oldp never received the
    POST. On the next run gii says 304 (zip unchanged) but oldp's
    /latest=true snapshot doesn't list the code, so we replay from cache.
    """
    Path(cache_dir, "zips").mkdir(parents=True, exist_ok=True)
    Path(cache_dir, "zips", "gg.zip").write_bytes(gg_bytes)
    Path(cache_dir, "zips", "baunvo.zip").write_bytes(baunvo_bytes)
    Path(cache_dir, "url_slug_to_jurabk.json").write_text(
        json.dumps({"gg": "GG", "baunvo": "BauNVO"})
    )
    Path(cache_dir, "done.json").write_text(
        json.dumps(
            {
                "gg": {
                    "jurabk": "GG",
                    "http_last_modified": "Wed, 26 Mar 2025 21:40:03 GMT",
                    "oldp_revision_date": "2025-03-26",
                },
                "baunvo": {
                    "jurabk": "BauNVO",
                    "http_last_modified": "Thu, 17 Aug 2023 19:35:04 GMT",
                    "oldp_revision_date": "2023-07-03",
                },
            }
        )
    )
    responses = {
        GII_TOC_URL: _FakeResponse(text=toc_xml),
        "https://www.gesetze-im-internet.de/gg/xml.zip": _FakeResponse(status_code=304),
        "https://www.gesetze-im-internet.de/baunvo/xml.zip": _FakeResponse(
            status_code=304
        ),
    }
    _install_http_mock(monkeypatch, responses)

    class FakeOldp:
        def get(self, path):
            # OLDP only knows about BauNVO — GG was queued but never landed.
            return {
                "results": [{"code": "BauNVO", "revision_date": "2023-07-03"}],
                "next": None,
            }

    provider = GiiLawProvider(oldp_client=FakeOldp(), cache_dir=cache_dir)
    books = provider.get_law_books()

    codes = [b["code"] for b in books]
    assert codes == ["GG"]  # BauNVO truly unchanged; GG re-uploaded
    # And the cached zip is the source for get_laws
    laws = provider.get_laws("GG", books[0]["revision_date"])
    assert laws and all(law["book_code"] == "GG" for law in laws)


def test_oldp_bootstrap_paginates(monkeypatch, cache_dir, toc_xml):
    """The bootstrap follows the `next` link on the OLDP response."""
    pages = [
        {
            "results": [{"code": "BGB", "revision_date": "2017-07-20"}],
            "next": "https://oldp.test/api/law_books/?latest=true&limit=1000&offset=1000",
        },
        {
            "results": [{"code": "GG", "revision_date": "2024-12-01"}],
            "next": None,
        },
    ]
    calls: list[str] = []

    class FakeOldp:
        def get(self, path):
            calls.append(path)
            return pages.pop(0)

    responses = {GII_TOC_URL: _FakeResponse(text=toc_xml)}
    # Stub the zip URLs as 304 so we don't need to set state up
    responses["https://www.gesetze-im-internet.de/gg/xml.zip"] = _FakeResponse(
        status_code=304
    )
    responses["https://www.gesetze-im-internet.de/baunvo/xml.zip"] = _FakeResponse(
        status_code=304
    )
    _install_http_mock(monkeypatch, responses)

    provider = GiiLawProvider(oldp_client=FakeOldp(), cache_dir=cache_dir)
    provider.get_law_books()

    # First call uses default path with ?latest=true&limit=1000.
    # Second call follows the `next` link's path+query.
    assert calls[0].endswith("/api/law_books/?latest=true&limit=1000")
    assert calls[1].endswith("/api/law_books/?latest=true&limit=1000&offset=1000")
