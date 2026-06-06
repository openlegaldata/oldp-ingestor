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
