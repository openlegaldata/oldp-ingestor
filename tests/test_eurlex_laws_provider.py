"""Unit tests for the EUR-Lex law provider.

Follows the test-isolation rule from oldp-ingestor/CLAUDE.md:
all HTTP calls are monkeypatched; no network access at test time.

Fixture: tests/resources/eurlex/32016R0679_de.html — a minimal EUR-Lex
Cellar XHTML for DSGVO covering Art. 1, Art. 15, Art. 82, and one
non-article subdivision the parser must skip.
"""

from __future__ import annotations

import logging
from pathlib import Path

from oldp_ingestor.providers.de.eurlex_laws import (
    EU_SEED_BOOKS,
    EurLexLawProvider,
    _article_order_key,
)

FIXTURE_DIR = Path(__file__).parent / "resources" / "eurlex"
DSGVO_HTML = (FIXTURE_DIR / "32016R0679_de.html").read_text(encoding="utf-8")


class _FakeResponse:
    """Stand-in for requests.Response used by the monkeypatched _get."""

    def __init__(self, *, status_code: int = 200, text: str = ""):
        self.status_code = status_code
        self.text = text


def _make_provider(monkeypatch, *, response_map=None, **kwargs):
    """Build an EurLexLawProvider with HTTP fully mocked.

    ``response_map`` is a dict keyed by substring-of-URL → _FakeResponse;
    the first match wins. ``None`` (or no match) raises ConnectionError.
    """
    provider = EurLexLawProvider(**kwargs)

    response_map = response_map or {}

    def fake_get(url, params=None, headers=None):
        for needle, response in response_map.items():
            if needle in url:
                if isinstance(response, Exception):
                    raise response
                return response
        raise ConnectionError(f"no fake response configured for {url}")

    monkeypatch.setattr(provider, "_get", fake_get)
    return provider


# --- seed list shape -------------------------------------------------- #


def test_seed_list_has_dsgvo_first():
    """DSGVO is the audit driver; it must be the first entry so
    ``--limit 1`` doesn't accidentally ingest a different book."""
    assert next(iter(EU_SEED_BOOKS)) == "32016R0679"
    assert EU_SEED_BOOKS["32016R0679"][0] == "DSGVO"


def test_seed_list_size_is_ten():
    """End-to-end target is 10 books — keep the seed list aligned so
    `--limit 10` (no --celex) hits the curated set exactly."""
    assert len(EU_SEED_BOOKS) == 10


# --- iter_law_books happy path --------------------------------------- #


def test_iter_law_books_returns_dsgvo(monkeypatch):
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/32016R0679": _FakeResponse(text=DSGVO_HTML),
        },
        limit=1,
    )

    books = list(provider.iter_law_books())

    assert len(books) == 1
    book = books[0]
    assert book["code"] == "DSGVO"
    assert "DATENSCHUTZ" in book["title"].upper()
    assert book["revision_date"] == "2016-04-27"


def test_get_laws_returns_parsed_articles(monkeypatch):
    """get_laws is called after iter_law_books drained — articles
    should come from the cache populated during the streaming pass."""
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/32016R0679": _FakeResponse(text=DSGVO_HTML),
        },
        limit=1,
    )

    list(provider.iter_law_books())  # populates the per-book cache
    laws = provider.get_laws("DSGVO", "2016-04-27")

    sections = [law["section"] for law in laws]
    assert sections == ["Art. 1", "Art. 15", "Art. 82"]

    # Title comes from the oj-sti-art subtitle, not the article number.
    by_section = {law["section"]: law for law in laws}
    assert by_section["Art. 15"]["title"] == "Auskunftsrecht der betroffenen Person"
    # Content keeps the paragraph numbering so refex can resolve
    # "Art. 15 Abs. 3" downstream.
    assert "(3)" in by_section["Art. 15"]["content"]
    # The article-number <p class="oj-ti-art"> should be stripped
    # from content (already exposed as section).
    assert "Artikel 15" not in by_section["Art. 15"]["content"]
    # And revision_date should be stamped through.
    assert by_section["Art. 15"]["revision_date"] == "2016-04-27"
    # book_code MUST be stamped on each law — the OLDP API serializer
    # requires it. The CLI doesn't add it for us. (Regression test
    # for the 99-laws "book_code: this field is required" e2e bug.)
    assert by_section["Art. 15"]["book_code"] == "DSGVO"


def test_parser_skips_non_article_subdivisions(monkeypatch):
    """The fixture contains ``<div id="art_annex_1">`` (an Anhang
    block) — the regex on the id must reject it so it doesn't land
    in the OLDP corpus as a stray Law row."""
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/32016R0679": _FakeResponse(text=DSGVO_HTML),
        },
        limit=1,
    )
    list(provider.iter_law_books())

    laws = provider.get_laws("DSGVO", "2016-04-27")
    assert all(law["section"].startswith("Art. ") for law in laws)
    # No "Art. annex" or similar leaks through.
    assert not any("annex" in law["section"].lower() for law in laws)


# --- transient failure handling -------------------------------------- #


def test_eurlex_202_falls_back_to_none(monkeypatch, caplog):
    """When Cellar 404s AND EUR-Lex returns 202 (still preparing),
    the provider must log + skip the book rather than crash."""
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/32016R0679": _FakeResponse(status_code=404),
            "/legal-content/DE/TXT/HTML/?uri=CELEX:32016R0679": _FakeResponse(
                status_code=202
            ),
        },
        limit=1,
    )

    with caplog.at_level(logging.WARNING):
        books = list(provider.iter_law_books())

    assert books == []
    assert any("202" in rec.message for rec in caplog.records)


def test_aws_waf_response_falls_back_to_none(monkeypatch, caplog):
    """A 200 body containing 'aws-waf-token' is the WAF challenge
    page — must be treated as transient, not parsed as a document."""
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/32016R0679": _FakeResponse(status_code=404),
            "/legal-content/DE/TXT/HTML/?uri=CELEX:32016R0679": _FakeResponse(
                text="<html><body>aws-waf-token challenge</body></html>"
            ),
        },
        limit=1,
    )
    with caplog.at_level(logging.WARNING):
        books = list(provider.iter_law_books())

    assert books == []
    assert any("WAF" in rec.message for rec in caplog.records)


def test_res_suffix_is_stripped(monkeypatch):
    """SPARQL sometimes returns ``32016R0679_RES``; the URL built by
    _fetch_xhtml must strip that suffix or both endpoints 404."""
    captured_urls: list[str] = []

    def fake_get(url, params=None, headers=None):
        captured_urls.append(url)
        return _FakeResponse(text=DSGVO_HTML)

    provider = EurLexLawProvider(celex_numbers=["32016R0679_RES"], limit=1)
    monkeypatch.setattr(provider, "_get", fake_get)

    list(provider.iter_law_books())

    assert captured_urls, "expected at least one HTTP call"
    # Whatever URL was used, _RES must NOT appear in it.
    assert all("_RES" not in u for u in captured_urls), captured_urls


# --- CLI surface ----------------------------------------------------- #


def test_celex_override_takes_precedence(monkeypatch):
    """``--celex CELEX1,CELEX2`` skips the seed list entirely."""
    provider = _make_provider(
        monkeypatch,
        response_map={
            "/resource/celex/CUSTOM123": _FakeResponse(text=DSGVO_HTML),
        },
        celex_numbers=["CUSTOM123"],
    )
    books = list(provider.iter_law_books())
    # Unknown CELEX → book_code falls back to the CELEX string.
    assert len(books) == 1
    assert books[0]["code"] == "CUSTOM123"


def test_limit_caps_the_seed_list():
    """``--limit 3`` against the default seed list must process
    exactly the first 3 entries, in seed order."""
    provider = EurLexLawProvider(limit=3)
    # Sanity-check the internal list; the streaming behaviour is
    # exercised separately above.
    assert len(provider._celex_numbers) == 3
    assert provider._celex_numbers == list(EU_SEED_BOOKS.keys())[:3]


def test_discover_stub_falls_back_to_seed_list(monkeypatch, caplog):
    """The discover stub must keep the CLI surface final without
    actually running SPARQL — it logs a warning and yields the
    seed list."""
    with caplog.at_level(logging.WARNING):
        provider = EurLexLawProvider(discover=True, limit=2)

    assert provider._celex_numbers == list(EU_SEED_BOOKS.keys())[:2]
    assert any(
        "discover" in rec.message.lower() and "stub" in rec.message.lower()
        for rec in caplog.records
    )


# --- order key ------------------------------------------------------- #


def test_article_order_key_orders_bis_articles():
    """Art. 5a must sort between Art. 5 and Art. 6 so OLDP's order
    field matches reader expectations."""
    keys = [
        _article_order_key("5"),
        _article_order_key("5a"),
        _article_order_key("5b"),
        _article_order_key("6"),
    ]
    assert keys == sorted(keys)
    assert keys[0] < keys[1] < keys[2] < keys[3]
