"""Tests for the 2026-11 provider health fixes.

Covers:
- RIS case law moved from ``/v1/case-law`` to ``/v1/rechtsprechung`` (new
  filter names, German detail fields, bare court codes).
- RIS legislation list items are expressions with ``exampleOfWork``.
- Source errors (search endpoint down, SPA never rendered) mark the run as
  ``partial`` instead of a clean ``ok`` with zero cases.
- Juris extended-search retries.
- Per-provider ``LOOKBACK_DAYS`` exposed via the capability registry.
- NS stops paginating once results are older than ``date_from``.
"""

import json
import os

import pytest
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.de.ris import RISProvider
from oldp_ingestor.providers.de.ris_cases import CASE_LAW_PATH, RISCaseProvider
from oldp_ingestor.providers.de.ris_common import RISBaseClient

# ---------------------------------------------------------------------------
# RIS case law: /v1/rechtsprechung
# ---------------------------------------------------------------------------


def test_ris_case_law_path_is_rechtsprechung():
    assert CASE_LAW_PATH == "/v1/rechtsprechung"


def test_ris_cases_uses_new_endpoints_and_german_detail(monkeypatch):
    """Full get_cases flow against the /v1/rechtsprechung payload shape."""
    requested: list[str] = []

    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "KORE1",
                    "courtName": "BVerwG",
                    "fileNumbers": ["1 C 2.26"],
                    "decisionDate": "2026-11-17",
                    "documentType": "Urteil",
                    "ecli": "ECLI:DE:BVerwG:2026:1",
                    "headline": "BVerwG, Urteil vom 17. November 2026",
                }
            }
        ],
        "view": {},
    }
    detail = {
        "leitsatz": "",
        "orientierungssatz": "Der Orientierungssatz.",
        "tenor": "Die Revision wird zurückgewiesen.",
    }

    def mock_get_json(self, path, **params):
        requested.append(path)
        if path == "/v1/rechtsprechung":
            return list_response
        if path == "/v1/rechtsprechung/courts":
            # Abbreviation-only label as served by the live API.
            return [{"id": "BVerwG", "label": "BVerwG", "count": 1}]
        if path == "/v1/rechtsprechung/KORE1":
            return detail
        raise AssertionError(f"unexpected path {path}")

    def mock_get_text(self, path):
        requested.append(path)
        assert path == "/v1/rechtsprechung/KORE1.html"
        return "<html><body><p>Entscheidungstext</p></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    cases = RISCaseProvider(request_delay=0).get_cases()

    assert len(cases) == 1
    case = cases[0]
    # Bare code mapped to the canonical OLDP name, not the API's "BVerwG".
    assert case["court_name"] == "Bundesverwaltungsgericht"
    # Empty leitsatz is skipped; orientierungssatz wins over tenor.
    assert case["abstract"] == "Der Orientierungssatz."
    assert case["source_url"].endswith("/v1/rechtsprechung/KORE1.html")
    assert not any("case-law" in p for p in requested)


@pytest.mark.parametrize(
    "detail, expected",
    [
        ({"leitsatz": "L", "orientierungssatz": "O", "tenor": "T"}, "L"),
        ({"orientierungssatz": "O", "tenor": "T"}, "O"),
        ({"sonstigerOrientierungssatz": "S", "tenor": "T"}, "S"),
        ({"tenor": "T"}, "T"),
        # Legacy English payload keeps working.
        ({"guidingPrinciple": "G", "headnote": "H"}, "G"),
        ({"leitsatz": None, "tenor": "  "}, None),
    ],
)
def test_ris_build_abstract_priority(detail, expected):
    assert RISCaseProvider._build_abstract(detail) == expected


@pytest.mark.parametrize(
    "code, name",
    [
        ("BGH", "Bundesgerichtshof"),
        ("BFH", "Bundesfinanzhof"),
        ("BVerwG", "Bundesverwaltungsgericht"),
        ("BPatG", "Bundespatentgericht"),
        ("BAG", "Bundesarbeitsgericht"),
        ("BSG", "Bundessozialgericht"),
        ("BVerfG", "Bundesverfassungsgericht"),
        ("BGH Karlsruhe", "Bundesgerichtshof"),  # legacy seat-suffixed code
    ],
)
def test_ris_resolve_court_name_overrides(code, name):
    provider = RISCaseProvider(request_delay=0)
    provider._court_labels = {}
    assert provider._resolve_court_name(code) == name


def test_ris_lookup_fetch_reads_german_detail_fields(monkeypatch):
    detail = {
        "gericht": "BGH",
        "aktenzeichenListe": ["VIII ZR 180/26"],
        "datum": "2026-11-17",
        "dokumenttyp": "Beschluss",
        "ecli": "ECLI:DE:BGH:2026:X",
        "kurztitel": "BGH, Beschluss vom 17. November 2026 - VIII ZR 180/26",
        "tenor": "Der Antrag wird zurückgewiesen.",
    }

    monkeypatch.setattr(RISBaseClient, "_get_json", lambda self, path, **p: detail)
    monkeypatch.setattr(
        RISBaseClient,
        "_get_text",
        lambda self, path: "<html><body><p>Volltext der Entscheidung</p></body></html>",
    )

    case = RISCaseProvider(request_delay=0).lookup_fetch("KORE611922026")

    assert case["court_name"] == "Bundesgerichtshof"
    assert case["file_number"] == "VIII ZR 180/26"
    assert case["date"] == "2026-11-17"
    assert case["type"] == "Beschluss"
    assert case["title"].startswith("BGH, Beschluss")
    assert case["abstract"] == "Der Antrag wird zurückgewiesen."
    assert case["source_url"].endswith("/v1/rechtsprechung/KORE611922026.html")


# ---------------------------------------------------------------------------
# RIS legislation: expression items with exampleOfWork
# ---------------------------------------------------------------------------


def test_ris_laws_new_expression_shape(monkeypatch):
    expression_id = "/v1/legislation/eli/bund/bgbl-1/2026/343a/2026-11-16/1/deu"
    list_response = {
        "totalItems": 1,
        "member": [
            {
                "item": {
                    "@id": expression_id,
                    "name": "Verordnung zur Konkretisierung ...",
                    "abbreviation": "BSWAV",
                    "exampleOfWork": {"legislationDate": "2026-11-16"},
                }
            }
        ],
        "view": {},
    }
    detail = {
        "@id": expression_id,
        "hasPart": [
            {"eId": "praeambel-n1_formel-n1", "name": "", "headline": "Eingangsformel"},
            {"eId": "art-z1", "name": "§ 1", "headline": "Anwendungsbereich"},
        ],
        "encoding": [
            {
                "encodingFormat": "text/html",
                "contentUrl": f"{expression_id}/2026-11-17/regelungstext-1.html",
            }
        ],
    }
    fetched_text: list[str] = []

    def mock_get_json(self, path, **params):
        if path == "/v1/legislation":
            return list_response
        assert path == expression_id
        return detail

    def mock_get_text(self, path):
        fetched_text.append(path)
        return "<html><body><p>Artikeltext</p></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISProvider(request_delay=0)
    books = provider.get_law_books()
    assert books == [
        {
            "code": "BSWAV",
            "title": "Verordnung zur Konkretisierung ...",
            "revision_date": "2026-11-16",
        }
    ]

    laws = provider.get_laws("BSWAV", "2026-11-16")
    assert [(law["section"], law["title"]) for law in laws] == [
        ("Eingangsformel", ""),
        ("§ 1", "Anwendungsbereich"),
    ]
    assert laws[1]["content"] == "<p>Artikeltext</p>"
    assert fetched_text[1].endswith("/regelungstext-1/art-z1.html")


# ---------------------------------------------------------------------------
# Source errors -> partial status
# ---------------------------------------------------------------------------


def test_record_source_error_counts_per_instance(caplog):
    class P(CaseProvider):
        pass

    a, b = P(), P()
    with caplog.at_level("ERROR"):
        a.record_source_error("Search failed: %s", "boom")
    assert a.source_errors == 1
    assert b.source_errors == 0
    assert "Search failed: boom" in caplog.text


def test_sn_ovg_search_failure_is_source_error(monkeypatch):
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise requests.HTTPError("500 Server Error")

    monkeypatch.setattr(SnOvgCaseProvider, "_request_with_retry", mock_request)

    provider = SnOvgCaseProvider(request_delay=0)
    assert provider.get_cases() == []
    assert provider.source_errors == 1


def test_sn_verfgh_search_failure_is_source_error(monkeypatch):
    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(SnVerfghCaseProvider, "_request_with_retry", mock_request)

    provider = SnVerfghCaseProvider(request_delay=0)
    assert provider.get_cases() == []
    assert provider.source_errors == 1


def test_cmd_cases_source_errors_mark_run_partial(monkeypatch, tmp_path):
    from oldp_ingestor import cli

    class BrokenSource(CaseProvider):
        def iter_cases(self):
            self.record_source_error("Search failed: %s", "500")
            return iter(())

    monkeypatch.setattr(cli, "_make_case_provider", lambda args: BrokenSource())
    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda **kw: object()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "sn-ovg"
        limit = None
        verbose = False
        results_dir = rdir

    assert cli.cmd_cases(FakeArgs()) == 1

    with open(os.path.join(rdir, "cases_sn-ovg.json")) as f:
        data = json.load(f)
    assert data["status"] == "partial"
    assert data["errors"] == 1
    assert data["created"] == 0


# ---------------------------------------------------------------------------
# Juris extended search retries
# ---------------------------------------------------------------------------


def _juris_provider(monkeypatch, fail_times: int):
    from oldp_ingestor.providers.de import juris
    from oldp_ingestor.providers.de.juris import BbBeCaseProvider

    class FakeTimeout(Exception):
        pass

    monkeypatch.setattr(juris, "_transient_playwright_errors", lambda: (FakeTimeout,))

    state = {"attempts": 0, "pages_opened": 0, "pages_closed": 0}

    class FakeLocator:
        def count(self):
            return 0  # no "next" button -> single results page

    class FakePage:
        def content(self):
            return '<a href="/bsbe/document/DOC1/">x</a>'

        def locator(self, selector):
            return FakeLocator()

        def close(self):
            state["pages_closed"] += 1

    class FakeContext:
        def new_page(self):
            state["pages_opened"] += 1
            return FakePage()

    def fake_open(self, page, search_url):
        state["attempts"] += 1
        if state["attempts"] <= fail_times:
            raise FakeTimeout("Page.goto: Timeout 30000ms exceeded.")

    def fake_ensure_browser(self):
        self._context = FakeContext()

    monkeypatch.setattr(BbBeCaseProvider, "_ensure_browser", fake_ensure_browser)
    monkeypatch.setattr(BbBeCaseProvider, "_open_search_results", fake_open)
    # _submit_search_with_dates imports ``time`` locally.
    monkeypatch.setattr("time.sleep", lambda s: None)

    provider = BbBeCaseProvider(date_from="2026-11-01", request_delay=0)
    return provider, state


def test_juris_search_retries_then_succeeds(monkeypatch):
    provider, state = _juris_provider(monkeypatch, fail_times=2)

    pages = list(provider._submit_search_with_dates())

    assert state["attempts"] == 3
    assert pages and "DOC1" in pages[0][0]
    assert provider.source_errors == 0
    assert state["pages_opened"] == state["pages_closed"] == 3


def test_juris_search_gives_up_and_records_source_error(monkeypatch):
    provider, state = _juris_provider(monkeypatch, fail_times=99)

    pages = list(provider._submit_search_with_dates())

    assert pages == []
    assert state["attempts"] == provider.SEARCH_ATTEMPTS
    assert provider.source_errors == 1
    assert state["pages_opened"] == state["pages_closed"]


# ---------------------------------------------------------------------------
# Lookback windows
# ---------------------------------------------------------------------------


def test_registry_exposes_lookback_days():
    from oldp_ingestor.providers import registry

    caps = registry.capabilities("cases")["cases"]
    assert caps["sn-verfgh"]["lookback_days"] == 90
    assert caps["sn"]["lookback_days"] == 90
    assert caps["ns"]["lookback_days"] == 60
    # Providers without an explicit window use the wrapper default.
    assert caps["by"]["lookback_days"] is None


# ---------------------------------------------------------------------------
# NS early stop on date-sorted results
# ---------------------------------------------------------------------------


_NS_CASE_TMPL = """<html><body>
    <article class="wkde-case wkde-document">
    <section class="wkde-bibliography"><dl>
        <dt>Gericht</dt><dd>AG Hannover</dd>
        <dt>Datum</dt><dd>{date}</dd>
        <dt>Aktenzeichen</dt><dd>5 C {n}/26</dd>
    </dl></section>
    <section class="wkde-document-body">
        <div class="tenor"><p>Die Klage wird abgewiesen.</p></div>
    </section>
    </article>
</body></html>"""


def test_ns_stops_after_pages_older_than_date_from(monkeypatch):
    from oldp_ingestor.providers.de.ns import NS_STALE_PAGES_STOP, NsCaseProvider

    # Page 0: recent; pages 1+: older than date_from. Without the early
    # stop the provider would walk all NS_MAX_PAGE pages.
    search_pages: list[int] = []

    class FakeResp:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        if url.endswith("/search"):
            page = int(kwargs["params"]["page"])
            search_pages.append(page)
            return FakeResp(
                f'<a href="/browse/document/{page:08x}-0000-0000-0000-00000000000a">a</a>'
                f'<a href="/browse/document/{page:08x}-0000-0000-0000-00000000000b">b</a>'
            )
        page = int(url.rsplit("/", 1)[1][:8], 16)
        date = "10.11.2026" if page == 0 else "01.09.2026"
        return FakeResp(_NS_CASE_TMPL.format(date=date, n=page))

    monkeypatch.setattr(NsCaseProvider, "_request_with_retry", mock_request)

    provider = NsCaseProvider(date_from="2026-10-01", request_delay=0)
    cases = list(provider.iter_cases())

    assert len(cases) == 2
    assert all(c["date"] == "2026-11-10" for c in cases)
    assert search_pages == list(range(NS_STALE_PAGES_STOP + 1))
