"""JurisApiCaseProvider: the juris state portals over their JSON API."""

import json

import pytest
import requests

from oldp_ingestor.providers.de.juris_api import BwApiCaseProvider, JurisApiCaseProvider

PAGE = (
    "<html><head><meta name='juris-version' content='bsbw - V08_37_00'></head></html>"
)
HEAD = """<table>
<tr><th class="TD30"><strong>Gericht:</strong></th><td class="TD70">VG Stuttgart 11. Kammer</td></tr>
<tr><th class="TD30"><strong>Entscheidungsdatum:</strong></th><td class="TD70">26.05.2014</td></tr>
<tr><th class="TD30"><strong>Aktenzeichen:</strong></th><td class="TD70">11 K 4547/13</td></tr>
<tr><th class="TD30"><strong>ECLI:</strong></th><td class="TD70">ECLI:DE:VGSTUTT:2014:0526.11K4547.13.0A</td></tr>
<tr><th class="TD30"><strong>Dokumenttyp:</strong></th><td class="TD70">Urteil</td></tr>
</table>"""
TEXT = (
    '<!--hlIgnoreOff--><div class="docLayoutText"><dl class="RspDL"><dt>'
    '<span class="unsichtbar">Randnummer</span><a name="rd_1">1</a></dt>'
    '<dd><p data-juris-link="{&#34;visited&#34;:true}">Der Kläger begehrt.</p></dd></dl></div>'
)


def hit(doc_id, date="26.05.2014"):
    return {
        "docId": doc_id,
        "date": date,
        "titleList": ["VG Stuttgart 11. Kammer", "11 K 4547/13"],
        "subtitleList": ["Urteil"],
    }


class FakeResp:
    def __init__(self, status=200, body=None, text=""):
        self.status_code = status
        self.content = json.dumps(body).encode() if body is not None else text.encode()
        self.text = text or self.content.decode()

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)


@pytest.fixture
def portal(monkeypatch):
    """A fake portal: two listing pages of hits, then an empty one."""
    calls = []
    state = {"pages": [[hit("NJRE1"), hit("NJRE2", "01.01.2010")], [hit("NJRE3")]]}

    def request(self, method, url, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return FakeResp(text=PAGE)
        body = kwargs.get("json") or {}
        if url.endswith("/init"):
            return FakeResp(body={"csrfToken": "tok-1"})
        if url.endswith("/search"):
            start = body["searchTasks"]["RESULT_LIST"]["start"]
            page = (start - 1) // JurisApiCaseProvider.PAGE_SIZE
            pages = state["pages"]
            return FakeResp(
                body={"hits": 3, "resultList": pages[page] if page < len(pages) else []}
            )
        if url.endswith("/document"):
            return FakeResp(body={"head": HEAD, "text": TEXT})
        raise AssertionError(url)

    monkeypatch.setattr(JurisApiCaseProvider, "_request_with_retry", request)
    return calls


def test_lists_all_pages_and_parses_each_document(portal):
    cases = BwApiCaseProvider(request_delay=0).get_cases()
    assert [c["source_url"] for c in cases] == [
        "https://www.landesrecht-bw.de/bsbw/document/NJRE1",
        "https://www.landesrecht-bw.de/bsbw/document/NJRE2",
        "https://www.landesrecht-bw.de/bsbw/document/NJRE3",
    ]
    case = cases[0]
    assert case["court_name"] == "VG Stuttgart 11. Kammer"
    assert case["file_number"] == "11 K 4547/13"
    assert case["date"] == "2014-05-26"
    assert case["ecli"] == "ECLI:DE:VGSTUTT:2014:0526.11K4547.13.0A"
    assert case["type"] == "Urteil"
    assert '<a name="rd_1">1</a>' in case["content"]
    assert "data-juris" not in case["content"] and "Randnummer" not in case["content"]


def test_one_session_and_the_token_on_every_call(portal):
    BwApiCaseProvider(request_delay=0).get_cases()
    gets = [c for c in portal if c[0] == "GET"]
    inits = [c for c in portal if c[1].endswith("/init")]
    assert len(gets) == 1 and len(inits) == 1
    for method, url, kwargs in portal:
        if method == "POST" and not url.endswith("/init"):
            assert kwargs["headers"]["X-CSRF-TOKEN"] == "tok-1"
            assert kwargs["headers"]["JURIS-PORTALID"] == "bsbw"
            assert kwargs["json"]["clientVersion"] == "bsbw - V08_37_00"


def test_date_filter_skips_documents_before_fetching(portal):
    cases = BwApiCaseProvider(date_from="2014-01-01", request_delay=0).get_cases()
    assert [c["source_url"][-5:] for c in cases] == ["NJRE1", "NJRE3"]
    assert not any(c[2].get("json", {}).get("docId") == "NJRE2" for c in portal)


def test_limit(portal):
    assert len(BwApiCaseProvider(limit=2, request_delay=0).get_cases()) == 2


def test_expired_session_signs_in_again(monkeypatch):
    tokens = iter(["tok-1", "tok-2"])
    seen = []

    def request(self, method, url, **kwargs):
        if method == "GET":
            return FakeResp(text=PAGE)
        if url.endswith("/init"):
            return FakeResp(body={"csrfToken": next(tokens)})
        token = kwargs["headers"]["X-CSRF-TOKEN"]
        seen.append(token)
        if token == "tok-1":
            resp = FakeResp(500, body={"msgId": "security_notAuthenticated"})
            resp.raise_for_status()
        return FakeResp(body={"head": HEAD, "text": TEXT})

    monkeypatch.setattr(JurisApiCaseProvider, "_request_with_retry", request)
    raw = BwApiCaseProvider(request_delay=0).fetch_document("NJRE1")
    assert seen == ["tok-1", "tok-2"] and b"VG Stuttgart" in raw


def test_every_state_portal_has_an_api_variant():
    from oldp_ingestor.providers import registry

    browser = registry.juris_case_classes()
    api = registry.juris_api_case_classes()
    assert sorted(n.replace("juris-api-", "") for n in api) == sorted(
        n.replace("juris-", "") for n in browser
    )
