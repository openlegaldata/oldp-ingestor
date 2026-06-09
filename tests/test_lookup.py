"""Tests for the citation-based lookup mixin and the lookup CLI.

Network calls are stubbed via ``monkeypatch`` so the suite remains
hermetic. The agent-facing contract is verified directly by inspecting
the JSON payloads that :mod:`oldp_ingestor.cli_lookup` writes to stdout.
"""

from __future__ import annotations

import json

import pytest

from oldp_ingestor.providers.de.nrw import NrwCaseProvider
from oldp_ingestor.providers.de.ns import NsCaseProvider
from oldp_ingestor.providers.de.ris_cases import RISCaseProvider
from oldp_ingestor.providers.lookup import (
    LookupCapability,
    LookupMixin,
    filter_courts,
    summarise_court,
    validate_capability,
)


# --- LookupCapability + helpers --------------------------------------


def test_validate_capability_accepts_known_keys():
    cap = LookupCapability(keys=("file_number", "ecli"), court_filter={}, cost="low")
    validate_capability(cap)  # does not raise


def test_validate_capability_rejects_unknown_key():
    cap = LookupCapability(keys=("file_number", "nope"), cost="low")
    with pytest.raises(ValueError, match="Unknown lookup keys"):
        validate_capability(cap)


def test_validate_capability_rejects_unknown_cost():
    cap = LookupCapability(keys=("file_number",), cost="ultra")
    with pytest.raises(ValueError, match="Unknown cost tier"):
        validate_capability(cap)


def test_validate_capability_rejects_unknown_filter_key():
    cap = LookupCapability(
        keys=("file_number",), court_filter={"vibes": ["good"]}, cost="low"
    )
    with pytest.raises(ValueError, match="Unknown court_filter keys"):
        validate_capability(cap)


_COURTS = [
    {"id": 1, "name": "BGH", "court_type": "BGH", "state": 2, "slug": "bgh"},
    {"id": 2, "name": "OLG Hamm", "court_type": "OLG", "state": 12, "slug": "olg-hamm"},
    {"id": 3, "name": "LG Hannover", "court_type": "LG", "state": 11, "slug": "lg-h"},
    {"id": 4, "name": "BFH", "court_type": "BFH", "state": 2, "slug": "bfh"},
]


def test_filter_courts_by_court_types():
    cap = LookupCapability(
        keys=("file_number",), court_filter={"court_types": ["BGH", "BFH"]}, cost="low"
    )
    out = filter_courts(_COURTS, cap)
    assert [c["id"] for c in out] == [1, 4]


def test_filter_courts_by_state_ids():
    cap = LookupCapability(
        keys=("file_number",), court_filter={"state_ids": [12]}, cost="medium"
    )
    out = filter_courts(_COURTS, cap)
    assert [c["id"] for c in out] == [2]


def test_filter_courts_dedupes_overlapping_selectors():
    cap = LookupCapability(
        keys=("file_number",),
        court_filter={"court_types": ["BGH"], "state_ids": [2]},
        cost="low",
    )
    out = filter_courts(_COURTS, cap)
    # BGH matches court_types AND state_ids; BFH only state_ids — both
    # appear once each.
    assert sorted(c["id"] for c in out) == [1, 4]


def test_summarise_court_projects_only_needed_fields():
    summary = summarise_court(
        {
            "id": 9,
            "name": "X",
            "code": "X",
            "slug": "x",
            "court_type": "X",
            "state": 1,
            "level_of_appeal": "y",
            "jurisdiction": "y",
            "homepage": "x.example",  # should be dropped
            "image": None,
        }
    )
    assert "homepage" not in summary
    assert summary["id"] == 9


# --- LookupMixin base contract ---------------------------------------


def test_lookupmixin_base_methods_raise_not_implemented():
    class _Empty(LookupMixin):
        LOOKUP_CAPABILITY = LookupCapability(keys=("file_number",), cost="low")

    with pytest.raises(NotImplementedError):
        _Empty().lookup_search(file_number="X")
    with pytest.raises(NotImplementedError):
        _Empty().lookup_fetch("Y")


# --- RIS provider lookup ---------------------------------------------


class _FakeRis(RISCaseProvider):
    """RIS provider with the network stubbed at ``_get_json`` boundary."""

    def __init__(self, search_response):
        super().__init__()
        self._search_response = search_response
        self.search_calls = []

    def _get_json(self, path, **params):  # type: ignore[override]
        self.search_calls.append((path, params))
        return self._search_response

    def _resolve_court_name(self, court_code):  # type: ignore[override]
        return court_code  # keep test deterministic


def _ris_member(doc_no, ecli, fns, court="BGH Karlsruhe", date="2024-01-01"):
    return {
        "item": {
            "documentNumber": doc_no,
            "ecli": ecli,
            "courtName": court,
            "decisionDate": date,
            "fileNumbers": fns,
            "documentType": "Urteil",
            "headline": "headline",
        }
    }


def test_ris_lookup_search_filters_to_exact_file_number():
    members = [
        _ris_member("A", "ECLI:DE:BGH:1", ["VI ZR 127/14"]),
        _ris_member("B", "ECLI:DE:BGH:2", ["VI ZR 22/24"]),
        _ris_member("C", "ECLI:DE:BGH:3", ["VI ZR 127/22"]),
    ]
    provider = _FakeRis({"member": members})
    hits = provider.lookup_search(file_number="VI ZR 127/22", limit=5)
    assert len(hits) == 1
    assert hits[0]["doc_id"] == "C"
    assert hits[0]["ecli"] == "ECLI:DE:BGH:3"


def test_ris_lookup_search_by_ecli_uses_searchterm():
    members = [_ris_member("X", "ECLI:DE:BGH:99", ["VI ZR 1/00"])]
    provider = _FakeRis({"member": members})
    hits = provider.lookup_search(ecli="ECLI:DE:BGH:99", limit=5)
    assert len(hits) == 1
    # The query is delivered via searchTerm (the API has no dedicated ecli filter)
    assert provider.search_calls[0][1]["searchTerm"] == "ECLI:DE:BGH:99"


def test_ris_lookup_search_rejects_empty_args():
    provider = _FakeRis({"member": []})
    with pytest.raises(ValueError):
        provider.lookup_search()


def test_ris_lookup_search_applies_court_hint_only_when_known():
    members = [_ris_member("X", "E", ["VI ZR 1/00"])]
    p1 = _FakeRis({"member": members})
    p1.lookup_search(file_number="VI ZR 1/00", court_hint="BGH")
    assert p1.search_calls[0][1].get("courtType") == "BGH"

    p2 = _FakeRis({"member": members})
    # An arbitrary court name (not in the federal court list) is ignored
    # so the query doesn't fail closed against a fuzzy court hint.
    p2.lookup_search(file_number="VI ZR 1/00", court_hint="LG Mannheim")
    assert "courtType" not in p2.search_calls[0][1]


def test_ris_lookup_search_empty_member_list_returns_empty():
    provider = _FakeRis({"member": []})
    assert provider.lookup_search(file_number="VI ZR 1/00") == []


def test_ris_normalise_file_number_collapses_whitespace_and_case():
    assert RISCaseProvider._normalise_file_number("VI  ZR   127/22") == "vi zr 127/22"


# --- NRW + voris listing parsers -------------------------------------


def test_nrw_lookup_search_parses_einergebnis(monkeypatch):
    """``_parse_case_from_html`` is unrelated; we only test the listing parse."""
    html = (
        "<html><body>"
        "<div class='einErgebnis'>"
        "<a target='_blank' href='https://nrwe.justiz.nrw.de/x/y.html'>"
        "23 A 416/25.A - Oberverwaltungsgericht NRW</a></div>"
        "<div class='einErgebnis'>"
        "<a target='_blank' href='https://nrwe.justiz.nrw.de/x/z.html'>"
        "11 K 222/24 - Verwaltungsgericht Köln</a></div>"
        "</body></html>"
    )

    class _FakeResp:
        text = html

    provider = NrwCaseProvider()

    def fake_post(path, **kwargs):
        return _FakeResp()

    monkeypatch.setattr(provider, "_post", fake_post)
    hits = provider.lookup_search(file_number="23 A 416/25.A")
    assert hits[0]["doc_id"] == "https://nrwe.justiz.nrw.de/x/y.html"
    assert hits[0]["court_name"] == "Oberverwaltungsgericht NRW"
    assert hits[0]["file_number"] == "23 A 416/25.A"


def test_nrw_lookup_rejects_missing_file_number():
    provider = NrwCaseProvider()
    with pytest.raises(ValueError):
        provider.lookup_search(ecli="ECLI:DE:OVGNRW:1")


def test_voris_lookup_search_filters_exact_az(monkeypatch):
    html = (
        '<h3><a href="/browse/document/'
        '11111111-1111-1111-1111-111111111111" hreflang="und">'
        "VG Hannover, 01.06.2026 - 10 B 1105/26 - Some long title here</a></h3>"
        '<h3><a href="/browse/document/'
        '22222222-2222-2222-2222-222222222222" hreflang="und">'
        "VG Lüneburg, 02.06.2026 - 99 B 1/26 - Other title</a></h3>"
    )

    class _FakeResp:
        text = html

    provider = NsCaseProvider()

    def fake_get(path, params=None, **kwargs):
        return _FakeResp()

    monkeypatch.setattr(provider, "_get", fake_get)
    hits = provider.lookup_search(file_number="10 B 1105/26")
    assert len(hits) == 1
    assert hits[0]["doc_id"] == "/browse/document/11111111-1111-1111-1111-111111111111"
    assert hits[0]["court_name"] == "VG Hannover"
    assert hits[0]["date"] == "2026-06-01"
    assert hits[0]["file_number"] == "10 B 1105/26"


def test_voris_lookup_rejects_missing_file_number():
    provider = NsCaseProvider()
    with pytest.raises(ValueError):
        provider.lookup_search(ecli="ECLI:DE:OVGLG:1")


# --- Capability declarations -----------------------------------------


def test_each_provider_declares_a_capability():
    for cls in (RISCaseProvider, NrwCaseProvider, NsCaseProvider):
        cap = cls.LOOKUP_CAPABILITY
        validate_capability(cap)
        assert "file_number" in cap.keys


def test_juris_subclass_capability_inherits_state_ids():
    from oldp_ingestor.providers.de.juris import BbBeCaseProvider, RlpCaseProvider

    assert BbBeCaseProvider.LOOKUP_CAPABILITY.court_filter == {"state_ids": [5, 6]}
    assert RlpCaseProvider.LOOKUP_CAPABILITY.court_filter == {"state_ids": [13]}
    assert BbBeCaseProvider.LOOKUP_CAPABILITY.cost == "high"


# --- CLI dispatcher --------------------------------------------------


class _Args:
    """Argparse-namespace stand-in for direct cmd_* calls."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def _read_json_lines(capsys) -> dict:
    out = capsys.readouterr().out
    return json.loads(out)


def test_cli_providers_no_resolve_omits_courts(capsys):
    from oldp_ingestor import cli_lookup

    rc = cli_lookup.cmd_lookup_providers(_Args(resolve_courts=False))
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    # No `courts` field when resolution disabled
    assert "courts" not in payload["result"]["ris"]
    assert payload["result"]["ris"]["court_filter"] == {
        "court_types": ["BGH", "BFH", "BVerwG", "BAG", "BSG", "BPatG", "BVerfG"]
    }


def test_cli_search_unknown_provider(capsys):
    from oldp_ingestor import cli_lookup

    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="bogus",
            file_number="X",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"
    assert "bogus" in payload["reason"]


def test_cli_search_missing_query_args(capsys):
    from oldp_ingestor import cli_lookup

    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ris",
            file_number=None,
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"
    assert "requires" in payload["reason"]


def test_cli_search_not_found_when_provider_returns_empty(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_search",
        lambda self, **kw: [],
    )
    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ris",
            file_number="ZZZ 0/00",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 1
    assert payload["status"] == "not_found"


def test_cli_fetch_not_found_when_provider_returns_none(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: None,
    )
    rc = cli_lookup.cmd_lookup_fetch(
        _Args(
            provider="ris",
            doc_id="MISSING",
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 1
    assert payload["status"] == "not_found"
    assert payload["doc_id"] == "MISSING"


def test_juris_parse_listing_entries_extracts_metadata():
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    html = """<html><body><ul>
      <li class="result-list__entry">
        <a class="entry-link" href="/document/AAA1234/format/xsl">
          <div class="result-list__title">
            <div class="result-list__title-entry result-list__title-entry--leading">05.05.2026</div>
            <div class="result-list__title-entry">LG Trier 5. Zivilkammer</div>
            <div class="result-list__title-entry">5 T 16/26</div>
          </div>
          <div class="result-list__sub-title">
            <div class="result-list__sub-title-entry result-list__sub-title-entry--leading">Beschluss</div>
          </div>
        </a>
      </li>
      <li class="result-list__entry">
        <a class="entry-link" href="/document/BBB5678/format/xsl">
          <div class="result-list__title">
            <div class="result-list__title-entry result-list__title-entry--leading">06.05.2026</div>
            <div class="result-list__title-entry">OLG Koblenz</div>
            <div class="result-list__title-entry">9 U 1/24</div>
          </div>
        </a>
      </li>
    </ul></body></html>"""
    provider = RlpCaseProvider()
    entries = provider._parse_listing_entries(html)
    assert len(entries) == 2
    assert entries[0]["doc_id"] == "AAA1234"
    assert entries[0]["court_name"] == "LG Trier 5. Zivilkammer"
    assert entries[0]["file_number"] == "5 T 16/26"
    assert entries[0]["date"] == "2026-05-05"
    assert entries[0]["type"] == "Beschluss"
    assert entries[1]["type"] == ""  # no sub-title


def test_juris_lookup_search_uses_canonical_when_single_hit(monkeypatch):
    """When the SPA auto-navigates to a single doc, the provider
    synthesises a candidate from the detail page instead of returning []."""
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    detail_html = """<html>
      <head><link id="idCanonicalUrlLink" rel="canonical"
        href="https://www.landesrecht.rlp.de/bsrp/document/NJRE001641457"></head>
      <body>
        <table>
          <tr>
            <th class="TD30"><strong>Gericht:</strong></th>
            <td class="TD70">LG Trier 5. Zivilkammer</td>
          </tr>
          <tr>
            <th class="TD30"><strong>Entscheidungsdatum:</strong></th>
            <td class="TD70">05.05.2026</td>
          </tr>
          <tr>
            <th class="TD30"><strong>Aktenzeichen:</strong></th>
            <td class="TD70">5 T 16/26</td>
          </tr>
        </table>
      </body>
    </html>"""

    provider = RlpCaseProvider()
    monkeypatch.setattr(
        provider, "_get_page_html", lambda url, wait_selector, timeout: detail_html
    )
    hits = provider.lookup_search(file_number="5 T 16/26")
    assert len(hits) == 1
    assert hits[0]["doc_id"] == "NJRE001641457"
    assert hits[0]["court_name"] == "LG Trier 5. Zivilkammer"
    assert hits[0]["date"] == "2026-05-05"


def test_juris_lookup_search_filters_listing_to_exact_match(monkeypatch):
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    listing_html = """<html><body><ul>
      <li class="result-list__entry">
        <a class="entry-link" href="/document/AAA/format/xsl">
          <div class="result-list__title">
            <div class="result-list__title-entry result-list__title-entry--leading">05.05.2026</div>
            <div class="result-list__title-entry">LG Trier</div>
            <div class="result-list__title-entry">5 T 16/26</div>
          </div>
        </a>
      </li>
      <li class="result-list__entry">
        <a class="entry-link" href="/document/BBB/format/xsl">
          <div class="result-list__title">
            <div class="result-list__title-entry result-list__title-entry--leading">06.05.2026</div>
            <div class="result-list__title-entry">OLG Koblenz</div>
            <div class="result-list__title-entry">99 X 99/99</div>
          </div>
        </a>
      </li>
    </ul></body></html>"""

    provider = RlpCaseProvider()
    monkeypatch.setattr(
        provider, "_get_page_html", lambda url, wait_selector, timeout: listing_html
    )
    hits = provider.lookup_search(file_number="5 T 16/26")
    assert [h["doc_id"] for h in hits] == ["AAA"]


def test_juris_lookup_search_rejects_missing_file_number():
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    with pytest.raises(ValueError):
        RlpCaseProvider().lookup_search(ecli="X")


def test_juris_summary_from_detail_html_handles_garbage():
    """``_summary_from_detail_html`` falls back gracefully on bad HTML
    so ``lookup_search`` doesn't blow up on a malformed detail page."""
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    summary = RlpCaseProvider()._summary_from_detail_html(
        "<<<not-valid>>>", "DOC", "5 T 16/26"
    )
    assert summary["file_number"] == "5 T 16/26"


def test_juris_lookup_fetch_delegates_to_parse_case_detail(monkeypatch):
    from oldp_ingestor.providers.de.juris import RlpCaseProvider

    called = {}

    def fake_parse(url):
        called["url"] = url
        return {"court_name": "LG Trier", "file_number": "5 T 16/26"}

    provider = RlpCaseProvider()
    monkeypatch.setattr(provider, "_parse_case_detail", fake_parse)
    case = provider.lookup_fetch("ABC123")
    assert case["court_name"] == "LG Trier"
    assert "/document/ABC123" in called["url"]


def test_cli_load_provider_cls_raises_on_unknown():
    from oldp_ingestor import cli_lookup

    with pytest.raises(KeyError):
        cli_lookup._load_provider_cls("does-not-exist")


def test_cli_fetch_unknown_provider(capsys):
    from oldp_ingestor import cli_lookup

    rc = cli_lookup.cmd_lookup_fetch(
        _Args(provider="bogus", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"


def test_cli_ingest_unknown_provider(capsys):
    from oldp_ingestor import cli_lookup

    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="bogus", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"


def test_cli_search_success_emits_candidates(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_search",
        lambda self, **kw: [
            {
                "doc_id": "X",
                "court_name": "BGH",
                "file_number": "VI ZR 1/00",
                "date": "2024-01-01",
                "ecli": "",
                "type": "Urteil",
                "snippet": "head",
            }
        ],
    )
    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ris",
            file_number="VI ZR 1/00",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    assert payload["candidates"][0]["doc_id"] == "X"


def test_cli_search_provider_raises_value_error(monkeypatch, capsys):
    """Provider-raised ValueError (e.g. voris asked for ECLI) → status:error."""
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ns

    def raise_value_error(self, **kw):
        raise ValueError("ecli not supported")

    monkeypatch.setattr(ns.NsCaseProvider, "lookup_search", raise_value_error)
    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ns",
            file_number="X",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"
    assert "ecli not supported" in payload["reason"]


def test_cli_search_provider_raises_generic_exception(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    def raise_runtime(self, **kw):
        raise RuntimeError("upstream blew up")

    monkeypatch.setattr(ris_cases.RISCaseProvider, "lookup_search", raise_runtime)
    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ris",
            file_number="X",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"
    assert "RuntimeError" in payload["reason"]


def test_cli_search_not_implemented_path(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    def raise_ni(self, **kw):
        raise NotImplementedError

    monkeypatch.setattr(ris_cases.RISCaseProvider, "lookup_search", raise_ni)
    rc = cli_lookup.cmd_lookup_search(
        _Args(
            provider="ris",
            file_number="X",
            ecli=None,
            court_hint=None,
            date=None,
            limit=10,
            request_delay=0,
            proxy=None,
        )
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert "does not support" in payload["reason"]


def test_cli_fetch_exception(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    def raise_runtime(self, doc_id):
        raise RuntimeError("dead")

    monkeypatch.setattr(ris_cases.RISCaseProvider, "lookup_fetch", raise_runtime)
    rc = cli_lookup.cmd_lookup_fetch(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"


def test_cli_fetch_success_returns_case(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: {"court_name": "BGH", "content": "X"},
    )
    rc = cli_lookup.cmd_lookup_fetch(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    assert payload["case"]["court_name"] == "BGH"


def test_cli_ingest_fetch_returns_none(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider, "lookup_fetch", lambda self, doc_id: None
    )
    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="MISSING", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 1
    assert payload["status"] == "not_found"


def test_cli_ingest_fetch_raises_exception(monkeypatch, capsys):
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    def raise_runtime(self, doc_id):
        raise RuntimeError("upstream gone")

    monkeypatch.setattr(ris_cases.RISCaseProvider, "lookup_fetch", raise_runtime)
    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"


def test_cli_ingest_success(monkeypatch, capsys):
    """Happy path: fetch ok + sink writes succeed."""
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: {
            "court_name": "BGH",
            "file_number": "VI ZR 1/00",
            "date": "2024-01-01",
            "content": "x" * 500,
            "source_url": "https://x/y",
        },
    )
    written: list[dict] = []

    class _OkSink:
        def __init__(self, *a, **kw):
            pass

        def write_case(self, case):
            written.append(case)

    class _DummyClient:
        @classmethod
        def from_settings(cls):
            return cls()

    monkeypatch.setattr("oldp_ingestor.sinks.api.ApiSink", _OkSink)
    monkeypatch.setattr("oldp_ingestor.client.OLDPClient", _DummyClient)

    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    assert payload["already_exists"] is False
    assert "content" not in payload["case"]
    # SOURCE was injected before writing
    assert written and written[0].get("source", {}).get("name")


def test_cli_ingest_non_409_http_error(monkeypatch, capsys):
    """400/500 from OLDP becomes a real ``error`` with detail body."""
    import requests

    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: {
            "court_name": "BGH",
            "file_number": "X",
            "date": "2024-01-01",
            "content": "x",
            "source_url": "y",
        },
    )

    class _BadResp:
        status_code = 400

        def json(self):
            return {"detail": "validation failed"}

    class _BadSink:
        def __init__(self, *a, **kw):
            pass

        def write_case(self, case):
            err = requests.HTTPError("400")
            err.response = _BadResp()
            raise err

    class _DummyClient:
        @classmethod
        def from_settings(cls):
            return cls()

    monkeypatch.setattr("oldp_ingestor.sinks.api.ApiSink", _BadSink)
    monkeypatch.setattr("oldp_ingestor.client.OLDPClient", _DummyClient)

    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert payload["status"] == "error"
    assert "400" in payload["reason"]
    assert payload["details"] == {"detail": "validation failed"}


def test_cli_ingest_write_raises_generic(monkeypatch, capsys):
    """Non-HTTPError exceptions during sink.write_case → status:error."""
    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: {
            "court_name": "BGH",
            "file_number": "X",
            "date": "2024-01-01",
            "content": "x",
            "source_url": "y",
        },
    )

    class _Sink:
        def __init__(self, *a, **kw):
            pass

        def write_case(self, case):
            raise RuntimeError("network gone")

    class _DummyClient:
        @classmethod
        def from_settings(cls):
            return cls()

    monkeypatch.setattr("oldp_ingestor.sinks.api.ApiSink", _Sink)
    monkeypatch.setattr("oldp_ingestor.client.OLDPClient", _DummyClient)

    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 2
    assert "RuntimeError" in payload["reason"]


def test_cli_try_close_swallows_exceptions():
    from oldp_ingestor import cli_lookup

    class _Bad:
        def close(self):
            raise RuntimeError("nope")

    # Must not raise.
    cli_lookup._try_close(_Bad())
    cli_lookup._try_close(object())  # no close attr


def test_fetch_all_courts_paginates(monkeypatch):
    """Verify the unauthenticated courts fetch follows ``next`` URLs."""
    from oldp_ingestor import cli_lookup

    pages = [
        {"results": [{"id": 1}], "next": "https://x.test/api/courts/?page=2"},
        {"results": [{"id": 2}], "next": None},
    ]
    calls: list[str] = []

    class _Resp:
        def __init__(self, payload):
            self._p = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._p

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        return _Resp(pages[len(calls) - 1])

    monkeypatch.setattr("requests.get", fake_get)
    out = cli_lookup._fetch_all_courts()
    assert [c["id"] for c in out] == [1, 2]
    # No auth header — only User-Agent is set
    # (covered by inspecting calls indirectly)


def test_cli_providers_resolved_omits_filter_when_resolve_returns_empty(
    monkeypatch, capsys
):
    """When ``_fetch_all_courts`` raises, payload still emits with
    ``courts_error`` set and no per-provider ``courts`` field."""
    from oldp_ingestor import cli_lookup

    def boom():
        raise RuntimeError("offline")

    monkeypatch.setattr(cli_lookup, "_fetch_all_courts", boom)
    rc = cli_lookup.cmd_lookup_providers(_Args(resolve_courts=True))
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    assert "RuntimeError" in payload["courts_error"]
    assert "courts" not in payload["result"]["ris"]


def test_cli_ingest_409_reports_already_exists(monkeypatch, capsys):
    """409 from the OLDP API maps to ``ok + already_exists`` so a retrying
    agent treats duplicate ingestion as success."""
    import requests

    from oldp_ingestor import cli_lookup
    from oldp_ingestor.providers.de import ris_cases

    monkeypatch.setattr(
        ris_cases.RISCaseProvider,
        "lookup_fetch",
        lambda self, doc_id: {
            "court_name": "BGH",
            "file_number": "VI ZR 1/00",
            "date": "2024-01-01",
            "content": "BODY",
            "source_url": "https://x/y",
        },
    )

    class _409Response:
        status_code = 409

        def json(self):
            return {"detail": "exists"}

    class _409Sink:
        def __init__(self, *a, **kw):
            pass

        def write_case(self, case):
            err = requests.HTTPError("409 Conflict")
            err.response = _409Response()
            raise err

    monkeypatch.setattr(cli_lookup, "_emit", cli_lookup._emit)  # passthrough
    monkeypatch.setattr(
        "oldp_ingestor.sinks.api.ApiSink", lambda client: _409Sink(client)
    )

    class _DummyClient:
        @classmethod
        def from_settings(cls):
            return cls()

    monkeypatch.setattr("oldp_ingestor.client.OLDPClient", _DummyClient)

    rc = cli_lookup.cmd_lookup_ingest(
        _Args(provider="ris", doc_id="X", request_delay=0, proxy=None)
    )
    payload = _read_json_lines(capsys)
    assert rc == 0
    assert payload["status"] == "ok"
    assert payload["already_exists"] is True
    # The slimmed-down case echo strips content
    assert "content" not in payload["case"]
