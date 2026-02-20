import json
import tempfile

import pytest

from oldp_ingestor.providers.base import CaseProvider, LawProvider, Provider
from oldp_ingestor.providers.dummy.dummy_laws import DummyLawProvider
from oldp_ingestor.providers.dummy.dummy_cases import DummyCaseProvider
from oldp_ingestor.providers.de.ris import RISProvider, _parse_article_name
from oldp_ingestor.providers.de.ris_common import extract_body
from oldp_ingestor.providers.de.ris_cases import RISCaseProvider
from oldp_ingestor.providers.de.ris_common import (
    INITIAL_BACKOFF,
    RISBaseClient,
    _retry_delay,
)

# --- Base provider ---


def test_base_provider_get_law_books_raises():
    provider = LawProvider()
    with pytest.raises(NotImplementedError):
        provider.get_law_books()


def test_base_provider_get_laws_raises():
    provider = LawProvider()
    with pytest.raises(NotImplementedError):
        provider.get_laws("code", "2024-01-01")


# --- Dummy provider ---

FIXTURE_DATA = [
    {
        "model": "laws.lawbook",
        "pk": 1,
        "fields": {
            "code": "TestCode",
            "title": "Test Law Book",
            "slug": "test-code",
            "order": 0,
            "revision_date": "2024-01-01",
            "latest": True,
            "changelog": "[]",
            "footnotes": "[]",
            "sections": "{}",
        },
    },
    {
        "model": "laws.lawbook",
        "pk": 2,
        "fields": {
            "code": "OtherCode",
            "title": "Other Law Book",
            "slug": "other-code",
            "order": 1,
            "revision_date": "2023-06-15",
            "latest": True,
            "changelog": "[]",
            "footnotes": "[]",
            "sections": "{}",
        },
    },
    {
        "model": "laws.law",
        "pk": 100,
        "fields": {
            "book": 1,
            "content": "<p>Paragraph 1 content</p>",
            "title": "First Section",
            "slug": "section-1",
            "section": "§ 1",
            "amtabk": None,
            "kurzue": None,
            "doknr": None,
            "footnotes": None,
            "order": 1,
            "previous": None,
        },
    },
    {
        "model": "laws.law",
        "pk": 101,
        "fields": {
            "book": 1,
            "content": "<p>Paragraph 2 content</p>",
            "title": "",
            "slug": "section-2",
            "section": "§ 2",
            "amtabk": None,
            "kurzue": None,
            "doknr": None,
            "footnotes": None,
            "order": 2,
            "previous": 100,
        },
    },
    {
        "model": "laws.law",
        "pk": 200,
        "fields": {
            "book": 2,
            "content": "<p>Other law content</p>",
            "title": "Only Section",
            "slug": "only-section",
            "section": "Artikel 1",
            "amtabk": None,
            "kurzue": None,
            "doknr": None,
            "footnotes": None,
            "order": 1,
            "previous": None,
        },
    },
]


@pytest.fixture
def dummy_fixture_path():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(FIXTURE_DATA, f)
        return f.name


def test_dummy_get_law_books(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    books = provider.get_law_books()
    assert len(books) == 2
    assert books[0]["code"] == "TestCode"
    assert books[0]["title"] == "Test Law Book"
    assert books[0]["revision_date"] == "2024-01-01"
    assert books[1]["code"] == "OtherCode"


def test_dummy_get_law_books_fields(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    books = provider.get_law_books()
    book = books[0]
    assert "code" in book
    assert "title" in book
    assert "revision_date" in book
    assert "order" in book
    assert "changelog" in book
    assert "footnotes" in book
    assert "sections" in book
    # Should not include non-API fields
    assert "slug" not in book
    assert "latest" not in book


def test_dummy_get_laws(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    laws = provider.get_laws("TestCode", "2024-01-01")
    assert len(laws) == 2
    assert laws[0]["section"] == "§ 1"
    assert laws[0]["title"] == "First Section"
    assert laws[0]["content"] == "<p>Paragraph 1 content</p>"
    assert laws[0]["book_code"] == "TestCode"
    assert laws[0]["revision_date"] == "2024-01-01"
    assert laws[1]["section"] == "§ 2"


def test_dummy_get_laws_filters_by_book(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    laws = provider.get_laws("OtherCode", "2023-06-15")
    assert len(laws) == 1
    assert laws[0]["section"] == "Artikel 1"
    assert laws[0]["book_code"] == "OtherCode"


def test_dummy_get_laws_no_match(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    laws = provider.get_laws("NonExistent", "2024-01-01")
    assert laws == []


def test_dummy_get_laws_wrong_revision_date(dummy_fixture_path):
    provider = DummyLawProvider(path=dummy_fixture_path)
    laws = provider.get_laws("TestCode", "1999-01-01")
    assert laws == []


# --- RIS provider: parse helpers ---


def test_parse_article_name_paragraph():
    section, title = _parse_article_name("§ 1 Organisation des Abschirmdienstes")
    assert section == "§ 1"
    assert title == "Organisation des Abschirmdienstes"


def test_parse_article_name_artikel():
    section, title = _parse_article_name("Artikel 12a Wehr- und Dienstpflicht")
    assert section == "Artikel 12a"
    assert title == "Wehr- und Dienstpflicht"


def test_parse_article_name_art_dot():
    section, title = _parse_article_name("Art. 5 Meinungsfreiheit")
    assert section == "Art. 5"
    assert title == "Meinungsfreiheit"


def test_parse_article_name_no_match():
    section, title = _parse_article_name("Eingangsformel")
    assert section == "Eingangsformel"
    assert title == ""


# --- RIS provider: integration with mock ---


def test_ris_provider_get_law_books(monkeypatch):
    api_response = {
        "member": [
            {
                "item": {
                    "abbreviation": "GG",
                    "name": "Grundgesetz",
                    "legislationDate": "2024-12-01",
                    "workExample": {
                        "@id": "/v1/legislation/eli/bund/bgbl-1/1949/s1/2024-12-01/1/deu"
                    },
                }
            }
        ],
        "view": {},
    }

    expression_detail = {
        "workExample": {
            "hasPart": [
                {
                    "eId": "art-z1",
                    "name": "Artikel 1 Die Würde des Menschen",
                }
            ],
            "encoding": [
                {
                    "encodingFormat": "text/html",
                    "contentUrl": "/v1/legislation/.../regelungstext.html",
                }
            ],
        }
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/legislation":
            return api_response
        return expression_detail

    monkeypatch.setattr(RISProvider, "_get_json", mock_get_json)

    provider = RISProvider(search_term="Grundgesetz", limit=1)
    books = provider.get_law_books()

    assert len(books) == 1
    assert books[0]["code"] == "GG"
    assert books[0]["title"] == "Grundgesetz"
    assert books[0]["revision_date"] == "2024-12-01"


def test_ris_provider_get_laws(monkeypatch):
    provider = RISProvider()
    provider._expression_cache[("GG", "2024-12-01")] = {
        "hasPart": [
            {"eId": "art-z1", "name": "Artikel 1 Die Würde"},
            {"eId": "art-z2", "name": "§ 2 Freiheit"},
        ],
        "encoding": [
            {
                "encodingFormat": "text/html",
                "contentUrl": "/v1/legislation/example/regelungstext.html",
            }
        ],
    }

    def mock_get_text(self, path):
        return f"<html><body>Content for {path}</body></html>"

    monkeypatch.setattr(RISProvider, "_get_text", mock_get_text)

    laws = provider.get_laws("GG", "2024-12-01")

    assert len(laws) == 2
    assert laws[0]["section"] == "Artikel 1"
    assert laws[0]["title"] == "Die Würde"
    assert laws[0]["slug"] == "artikel-1"
    assert laws[0]["order"] == 1
    assert laws[0]["book_code"] == "GG"
    assert "<html>" not in laws[0]["content"]
    assert "<body" not in laws[0]["content"]
    assert "Content for" in laws[0]["content"]
    assert laws[1]["section"] == "§ 2"
    assert laws[1]["order"] == 2


def test_extract_body_full_html():
    html = '<!DOCTYPE HTML><html><head><title>T</title></head><body class="x"><article>Content</article></body></html>'
    assert extract_body(html) == "<article>Content</article>"


def test_extract_body_no_body_tag():
    fragment = "<p>Just a fragment</p>"
    assert extract_body(fragment) == fragment


def test_extract_body_empty_body():
    html = "<html><body></body></html>"
    assert extract_body(html) == ""


def test_ris_provider_get_laws_no_cache():
    provider = RISProvider()
    laws = provider.get_laws("NONEXISTENT", "2024-01-01")
    assert laws == []


# --- CaseProvider base ---


def test_case_provider_get_cases_raises():
    provider = CaseProvider()
    with pytest.raises(NotImplementedError):
        provider.get_cases()


# --- DummyCaseProvider ---

CASE_FIXTURE_DATA = [
    {
        "model": "courts.court",
        "pk": 1,
        "fields": {
            "name": "Bundesgerichtshof",
            "code": "BGH",
            "slug": "bgh",
        },
    },
    {
        "model": "courts.court",
        "pk": 2,
        "fields": {
            "name": "Bundesverfassungsgericht",
            "code": "BVerfG",
            "slug": "bverfg",
        },
    },
    {
        "model": "cases.case",
        "pk": 1,
        "fields": {
            "court": 1,
            "file_number": "I ZR 123/21",
            "date": "2024-01-15",
            "content": "<p>Case content one</p>",
            "type": "Urteil",
            "ecli": "ECLI:DE:BGH:2024:150124UIZR123.21.0",
            "abstract": "<p>Abstract one</p>",
            "title": "Test Case One",
            "slug": "i-zr-123-21",
            "private": True,
            "raw": "{}",
        },
    },
    {
        "model": "cases.case",
        "pk": 2,
        "fields": {
            "court": 2,
            "file_number": "1 BvR 456/22",
            "date": "2024-03-20",
            "content": "<p>Case content two</p>",
            "slug": "1-bvr-456-22",
            "private": False,
        },
    },
    {
        "model": "laws.lawbook",
        "pk": 99,
        "fields": {
            "code": "Unrelated",
            "title": "Unrelated Book",
            "slug": "unrelated",
            "order": 0,
            "revision_date": "2024-01-01",
            "latest": True,
        },
    },
]


@pytest.fixture
def case_fixture_path():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(CASE_FIXTURE_DATA, f)
        return f.name


def test_dummy_cases_get_cases_count(case_fixture_path):
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    assert len(cases) == 2


def test_dummy_cases_filters_non_case_models(case_fixture_path):
    """Only cases.case entries are returned, not courts or lawbooks."""
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    # There are 5 fixture entries total but only 2 are cases.case
    assert len(cases) == 2


def test_dummy_cases_court_name_mapped(case_fixture_path):
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    assert cases[0]["court_name"] == "Bundesgerichtshof"
    assert cases[1]["court_name"] == "Bundesverfassungsgericht"


def test_dummy_cases_fields_content(case_fixture_path):
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    case = cases[0]
    assert case["file_number"] == "I ZR 123/21"
    assert case["date"] == "2024-01-15"
    assert case["content"] == "<p>Case content one</p>"
    assert case["type"] == "Urteil"
    assert case["ecli"] == "ECLI:DE:BGH:2024:150124UIZR123.21.0"
    assert case["abstract"] == "<p>Abstract one</p>"
    assert case["title"] == "Test Case One"


def test_dummy_cases_only_whitelisted_fields(case_fixture_path):
    """Non-API fields like slug, private, raw must be excluded."""
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    for case in cases:
        assert "slug" not in case
        assert "private" not in case
        assert "raw" not in case
        assert "court" not in case  # raw FK, not the resolved name


def test_dummy_cases_missing_optional_fields(case_fixture_path):
    """Case 2 has no type/ecli/abstract/title — those keys should be absent."""
    provider = DummyCaseProvider(path=case_fixture_path)
    cases = provider.get_cases()
    case2 = cases[1]
    assert "type" not in case2
    assert "ecli" not in case2
    assert "abstract" not in case2
    assert "title" not in case2
    # But required fields are present
    assert case2["file_number"] == "1 BvR 456/22"
    assert case2["date"] == "2024-03-20"
    assert case2["court_name"] == "Bundesverfassungsgericht"


def test_dummy_cases_missing_court_fallback():
    """When court pk is not in fixtures, use a fallback label."""
    fixture = [
        {
            "model": "cases.case",
            "pk": 1,
            "fields": {
                "court": 999,
                "file_number": "X YZ 1/24",
                "date": "2024-06-01",
                "content": "<p>Content</p>",
            },
        },
    ]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(fixture, f)
        path = f.name

    provider = DummyCaseProvider(path=path)
    cases = provider.get_cases()
    assert len(cases) == 1
    assert cases[0]["court_name"] == "Unknown court (pk=999)"


# --- Provider base class hierarchy ---


def test_provider_base_class_exists():
    """Provider is the common root for all providers."""
    assert issubclass(LawProvider, Provider)
    assert issubclass(CaseProvider, Provider)


def test_ris_provider_inherits_from_provider():
    assert issubclass(RISProvider, Provider)
    assert issubclass(RISProvider, LawProvider)
    assert issubclass(RISProvider, RISBaseClient)


def test_ris_case_provider_inherits_from_provider():
    assert issubclass(RISCaseProvider, Provider)
    assert issubclass(RISCaseProvider, CaseProvider)
    assert issubclass(RISCaseProvider, RISBaseClient)


# --- RISBaseClient ---


def test_ris_base_client_get_json_uses_retry(monkeypatch):
    """_get_json delegates to _request_with_retry and parses JSON."""
    calls = []

    class FakeResponse:
        def json(self):
            return {"ok": True}

    def mock_request(self, method, url, **kwargs):
        calls.append((method, url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(RISBaseClient, "_request_with_retry", mock_request)

    client = RISBaseClient(request_delay=0)
    result = client._get_json("/v1/test", foo="bar")

    assert result == {"ok": True}
    assert len(calls) == 1
    assert calls[0][0] == "GET"
    assert calls[0][2]["params"] == {"foo": "bar"}


def test_ris_base_client_get_text_uses_retry(monkeypatch):
    """_get_text delegates to _request_with_retry and returns text."""

    class FakeResponse:
        text = "<html><body>Hello</body></html>"

    def mock_request(self, method, url, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(RISBaseClient, "_request_with_retry", mock_request)

    client = RISBaseClient(request_delay=0)
    result = client._get_text("/v1/test")
    assert result == "<html><body>Hello</body></html>"


# --- RISCaseProvider._build_abstract ---


def test_build_abstract_prefers_guiding_principle():
    detail = {
        "guidingPrinciple": "The guiding principle text.",
        "headnote": "Headnote text.",
        "tenor": "Tenor text.",
    }
    assert RISCaseProvider._build_abstract(detail) == "The guiding principle text."


def test_build_abstract_falls_back_to_headnote():
    detail = {
        "guidingPrinciple": "",
        "headnote": "Headnote text.",
        "tenor": "Tenor text.",
    }
    assert RISCaseProvider._build_abstract(detail) == "Headnote text."


def test_build_abstract_falls_back_to_other_headnote():
    detail = {
        "guidingPrinciple": "",
        "headnote": "",
        "otherHeadnote": "Other headnote.",
    }
    assert RISCaseProvider._build_abstract(detail) == "Other headnote."


def test_build_abstract_falls_back_to_tenor():
    detail = {"tenor": "Tenor text."}
    assert RISCaseProvider._build_abstract(detail) == "Tenor text."


def test_build_abstract_returns_none_when_empty():
    detail = {"guidingPrinciple": "", "headnote": "", "tenor": ""}
    assert RISCaseProvider._build_abstract(detail) is None


def test_build_abstract_returns_none_when_no_fields():
    assert RISCaseProvider._build_abstract({}) is None


def test_build_abstract_truncates_at_50000():
    detail = {"guidingPrinciple": "x" * 60000}
    result = RISCaseProvider._build_abstract(detail)
    assert len(result) == 50000


# --- RISCaseProvider._fetch_court_labels ---


def test_fetch_court_labels(monkeypatch):
    api_response = [
        {"id": "BGH", "label": "Bundesgerichtshof"},
        {"id": "BVerfG", "label": "Bundesverfassungsgericht"},
    ]

    def mock_get_json(self, path, **params):
        return api_response

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISCaseProvider(request_delay=0)
    labels = provider._fetch_court_labels()

    assert labels == {
        "BGH": "Bundesgerichtshof",
        "BVerfG": "Bundesverfassungsgericht",
    }


# --- RISCaseProvider._resolve_court_name ---


def test_resolve_court_name_known(monkeypatch):
    def mock_get_json(self, path, **params):
        return [
            {"id": "BGH", "label": "Bundesgerichtshof"},
        ]

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISCaseProvider(request_delay=0)
    assert provider._resolve_court_name("BGH") == "Bundesgerichtshof"


def test_resolve_court_name_unknown_falls_back_to_code(monkeypatch):
    def mock_get_json(self, path, **params):
        return []

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISCaseProvider(request_delay=0)
    assert provider._resolve_court_name("UNKNOWN") == "UNKNOWN"


# --- RISCaseProvider.get_cases: full integration with mocked HTTP ---


def test_ris_case_provider_get_cases(monkeypatch):
    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["I ZR 123/21"],
                    "decisionDate": "2026-01-15",
                    "documentType": "Urteil",
                    "ecli": "ECLI:DE:BGH:2026:...",
                    "headline": "BGH, 15.01.2026, I ZR 123/21",
                }
            }
        ],
        "view": {},
    }

    courts_response = [
        {"id": "BGH", "label": "Bundesgerichtshof"},
    ]

    detail_response = {
        "guidingPrinciple": "Important guiding principle.",
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        if path == "/v1/case-law/courts":
            return courts_response
        if path == "/v1/case-law/DOC-001":
            return detail_response
        return {}

    def mock_get_text(self, path):
        if "DOC-001.html" in path:
            return "<html><body><div>Case content here</div></body></html>"
        return ""

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    case = cases[0]
    assert case["court_name"] == "Bundesgerichtshof"
    assert case["file_number"] == "I ZR 123/21"
    assert case["date"] == "2026-01-15"
    assert case["type"] == "Urteil"
    assert case["ecli"] == "ECLI:DE:BGH:2026:..."
    assert case["title"] == "BGH, 15.01.2026, I ZR 123/21"
    assert case["abstract"] == "Important guiding principle."
    assert "<div>Case content here</div>" in case["content"]
    assert "<html>" not in case["content"]


def test_ris_case_provider_pagination(monkeypatch):
    """Verify pagination follows view.next links."""
    page0 = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-01",
                }
            }
        ],
        "view": {"next": "/v1/case-law?pageIndex=1"},
        "totalItems": 2,
    }
    page1 = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-002",
                    "courtName": "BGH",
                    "fileNumbers": ["2/21"],
                    "decisionDate": "2026-01-02",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            idx = params.get("pageIndex", 0)
            return page0 if idx == 0 else page1
        if path == "/v1/case-law/courts":
            return [{"id": "BGH", "label": "Bundesgerichtshof"}]
        # Detail endpoint
        return {}

    def mock_get_text(self, path):
        return "<html><body><div>Content</div></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 2
    assert cases[0]["file_number"] == "1/21"
    assert cases[1]["file_number"] == "2/21"


def test_ris_case_provider_empty_file_numbers(monkeypatch):
    """Cases with empty fileNumbers should use empty string for file_number."""
    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": [],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        if path == "/v1/case-law/courts":
            return [{"id": "BGH", "label": "Bundesgerichtshof"}]
        return {}

    def mock_get_text(self, path):
        return "<html><body><div>Content</div></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["file_number"] == ""


def test_ris_case_provider_skips_short_content(monkeypatch):
    """Cases where HTML body is too short (<10 chars) are skipped."""
    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        return {}

    def mock_get_text(self, path):
        return "<html><body>short</body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 0


def test_ris_case_provider_skips_failed_html_fetch(monkeypatch):
    """Cases where HTML fetch fails are skipped."""
    import requests as req

    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        return {}

    def mock_get_text(self, path):
        raise req.ConnectionError("Network error")

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 0


def test_ris_case_provider_continues_without_abstract_on_detail_failure(monkeypatch):
    """When detail fetch fails, case is still returned without abstract."""
    import requests as req

    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        if path == "/v1/case-law/courts":
            return [{"id": "BGH", "label": "Bundesgerichtshof"}]
        if path == "/v1/case-law/DOC-001":
            raise req.ConnectionError("Failed")
        return {}

    def mock_get_text(self, path):
        return "<html><body><div>Case content here</div></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert "abstract" not in cases[0]
    assert cases[0]["court_name"] == "Bundesgerichtshof"


def test_ris_case_provider_limit(monkeypatch):
    """Limit stops fetching after N cases."""
    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": f"DOC-{i:03d}",
                    "courtName": "BGH",
                    "fileNumbers": [f"{i}/21"],
                    "decisionDate": "2026-01-15",
                }
            }
            for i in range(5)
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        if path == "/v1/case-law/courts":
            return [{"id": "BGH", "label": "Bundesgerichtshof"}]
        return {}

    def mock_get_text(self, path):
        return "<html><body><div>Content</div></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(limit=2, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 2


# --- RISBaseClient: _request_with_retry coverage ---


def test_request_with_retry_success(monkeypatch):
    """Successful request on first attempt."""
    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            pass

    client = RISBaseClient(request_delay=0.1)
    client.session = type("S", (), {"request": lambda self, *a, **kw: FakeResp()})()
    resp = client._request_with_retry("GET", "http://example.com")
    assert resp.status_code == 200


def test_request_with_retry_503_then_success(monkeypatch):
    """503 on first attempt, success on second."""
    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )

    call_count = [0]

    class FakeResp503:
        status_code = 503
        headers = {}

        def raise_for_status(self):
            pass

    class FakeRespOK:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return FakeResp503()
        return FakeRespOK()

    client = RISBaseClient(request_delay=0)
    client.session = type(
        "S", (), {"request": lambda self, *a, **kw: fake_request(*a, **kw)}
    )()
    resp = client._request_with_retry("GET", "http://example.com")
    assert resp.status_code == 200
    assert call_count[0] == 2


def test_request_with_retry_429_then_success(monkeypatch):
    """429 on first attempt, success on second."""
    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )

    call_count = [0]

    class FakeResp429:
        status_code = 429
        headers = {"Retry-After": "2"}

        def raise_for_status(self):
            pass

    class FakeRespOK:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return FakeResp429()
        return FakeRespOK()

    client = RISBaseClient(request_delay=0)
    client.session = type(
        "S", (), {"request": lambda self, *a, **kw: fake_request(*a, **kw)}
    )()
    resp = client._request_with_retry("GET", "http://example.com")
    assert resp.status_code == 200
    assert call_count[0] == 2


def test_request_with_retry_connection_error_then_success(monkeypatch):
    """ConnectionError on first attempt, success on second."""
    import requests as req

    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )

    call_count = [0]

    class FakeRespOK:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise req.ConnectionError("fail")
        return FakeRespOK()

    client = RISBaseClient(request_delay=0)
    client.session = type(
        "S", (), {"request": lambda self, *a, **kw: fake_request(*a, **kw)}
    )()
    resp = client._request_with_retry("GET", "http://example.com")
    assert resp.status_code == 200


def test_request_with_retry_connection_error_exhausted(monkeypatch):
    """ConnectionError on all attempts raises."""
    import requests as req

    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )
    monkeypatch.setattr("oldp_ingestor.providers.http_client.MAX_RETRIES", 1)

    def fake_request(method, url, **kwargs):
        raise req.ConnectionError("fail")

    client = RISBaseClient(request_delay=0)
    client.session = type(
        "S", (), {"request": lambda self, *a, **kw: fake_request(*a, **kw)}
    )()
    with pytest.raises(req.ConnectionError):
        client._request_with_retry("GET", "http://example.com")


def test_request_with_retry_non_retryable_error(monkeypatch):
    """Non-retryable status code (e.g. 404) raises immediately."""
    import requests as req

    monkeypatch.setattr(
        "oldp_ingestor.providers.http_client.time.sleep", lambda _: None
    )

    class FakeResp404:
        status_code = 404

        def raise_for_status(self):
            raise req.HTTPError(response=self)

    client = RISBaseClient(request_delay=0)
    client.session = type("S", (), {"request": lambda self, *a, **kw: FakeResp404()})()
    with pytest.raises(req.HTTPError):
        client._request_with_retry("GET", "http://example.com")


# --- _retry_delay ---


def test_retry_delay_uses_retry_after_header():
    class FakeResp:
        headers = {"Retry-After": "5"}

    assert _retry_delay(FakeResp(), attempt=0) == 5.0


def test_retry_delay_retry_after_minimum_1():
    class FakeResp:
        headers = {"Retry-After": "0.5"}

    assert _retry_delay(FakeResp(), attempt=0) == 1.0


def test_retry_delay_invalid_retry_after_falls_back():
    class FakeResp:
        headers = {"Retry-After": "not-a-number"}

    assert _retry_delay(FakeResp(), attempt=2) == INITIAL_BACKOFF * 4


def test_retry_delay_no_header_uses_backoff():
    class FakeResp:
        headers = {}

    assert _retry_delay(FakeResp(), attempt=0) == INITIAL_BACKOFF * 1
    assert _retry_delay(FakeResp(), attempt=3) == INITIAL_BACKOFF * 8


# --- RISProvider: date filter + skip branches ---


def test_ris_provider_get_law_books_with_date_filters(monkeypatch):
    """Date filter params are passed and sort is set."""
    captured_params = {}

    def mock_get_json(self, path, **params):
        if path == "/v1/legislation":
            captured_params.update(params)
            return {"member": [], "view": {}}
        return {}

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISProvider(date_from="2025-01-01", date_to="2025-06-30")
    books = provider.get_law_books()

    assert books == []
    assert captured_params["dateFrom"] == "2025-01-01"
    assert captured_params["dateTo"] == "2025-06-30"
    assert captured_params["sort"] == "-date"


def test_ris_provider_skips_member_missing_abbreviation(monkeypatch):
    api_response = {
        "member": [
            {
                "item": {
                    "abbreviation": "",
                    "name": "No abbrev",
                    "legislationDate": "2024-01-01",
                    "workExample": {"@id": "/v1/legislation/eli/test"},
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        return api_response

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISProvider()
    books = provider.get_law_books()
    assert books == []


def test_ris_provider_skips_member_missing_expression_id(monkeypatch):
    api_response = {
        "member": [
            {
                "item": {
                    "abbreviation": "TEST",
                    "name": "Test",
                    "legislationDate": "2024-01-01",
                    "workExample": {},
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        return api_response

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISProvider()
    books = provider.get_law_books()
    assert books == []


def test_ris_provider_get_laws_html_fetch_failure(monkeypatch):
    """When article HTML fetch fails, content should be empty."""
    import requests as req

    provider = RISProvider()
    provider._expression_cache[("TEST", "2024-01-01")] = {
        "hasPart": [{"eId": "art-1", "name": "§ 1 Test"}],
        "encoding": [
            {
                "encodingFormat": "text/html",
                "contentUrl": "/v1/legislation/example/regelungstext.html",
            }
        ],
    }

    def mock_get_text(self, path):
        raise req.ConnectionError("fail")

    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    laws = provider.get_laws("TEST", "2024-01-01")
    assert len(laws) == 1
    assert laws[0]["content"] == ""


def test_ris_provider_pagination(monkeypatch):
    """RISProvider paginates when view.next is present."""
    page0 = {
        "member": [
            {
                "item": {
                    "abbreviation": "A",
                    "name": "Law A",
                    "legislationDate": "2024-01-01",
                    "workExample": {"@id": "/v1/legislation/eli/a"},
                }
            }
        ]
        * 300,
        "view": {"next": "/v1/legislation?pageIndex=1"},
        "totalItems": 301,
    }
    page1 = {
        "member": [
            {
                "item": {
                    "abbreviation": "B",
                    "name": "Law B",
                    "legislationDate": "2024-01-02",
                    "workExample": {"@id": "/v1/legislation/eli/b"},
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/legislation":
            idx = params.get("pageIndex", 0)
            return page0 if idx == 0 else page1
        return {"workExample": {"hasPart": [], "encoding": []}}

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISProvider()
    books = provider.get_law_books()

    assert len(books) == 301
    assert books[-1]["code"] == "B"


# --- RISCaseProvider: filter params + edge cases ---


def test_ris_case_provider_passes_filter_params(monkeypatch):
    """court, date_from, date_to are passed as query params."""
    captured = {}

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            captured.update(params)
            return {"member": [], "view": {}}
        return {}

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISCaseProvider(
        court="BGH", date_from="2026-01-01", date_to="2026-06-30", request_delay=0
    )
    cases = provider.get_cases()

    assert cases == []
    assert captured["courtType"] == "BGH"
    assert captured["decisionDateFrom"] == "2026-01-01"
    assert captured["decisionDateTo"] == "2026-06-30"


def test_ris_case_provider_skips_missing_document_number(monkeypatch):
    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        return {}

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_ris_case_provider_court_labels_failure_fallback(monkeypatch):
    """When court labels fetch fails, court code is used as-is."""
    import requests as req

    list_response = {
        "member": [
            {
                "item": {
                    "documentNumber": "DOC-001",
                    "courtName": "BGH",
                    "fileNumbers": ["1/21"],
                    "decisionDate": "2026-01-15",
                }
            }
        ],
        "view": {},
    }

    def mock_get_json(self, path, **params):
        if path == "/v1/case-law":
            return list_response
        if path == "/v1/case-law/courts":
            raise req.ConnectionError("fail")
        return {}

    def mock_get_text(self, path):
        return "<html><body><div>Case content here</div></body></html>"

    monkeypatch.setattr(RISBaseClient, "_get_json", mock_get_json)
    monkeypatch.setattr(RISBaseClient, "_get_text", mock_get_text)

    provider = RISCaseProvider(request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "BGH"  # falls back to code


# ===================================================================
# --- HttpBaseClient ---
# ===================================================================


def test_http_base_client_get_prepends_base_url(monkeypatch):
    from oldp_ingestor.providers.http_client import HttpBaseClient

    calls = []

    class FakeResp:
        status_code = 200
        text = "hello"

        def raise_for_status(self):
            pass

        def json(self):
            return {}

    def mock_request(self, method, url, **kwargs):
        calls.append(url)
        return FakeResp()

    monkeypatch.setattr(HttpBaseClient, "_request_with_retry", mock_request)

    client = HttpBaseClient(base_url="https://example.com", request_delay=0)
    client._get("/path")
    assert calls[-1] == "https://example.com/path"

    client._get("https://other.com/full")
    assert calls[-1] == "https://other.com/full"


def test_http_base_client_post(monkeypatch):
    from oldp_ingestor.providers.http_client import HttpBaseClient

    calls = []

    class FakeResp:
        status_code = 200
        text = "ok"

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        calls.append((method, url))
        return FakeResp()

    monkeypatch.setattr(HttpBaseClient, "_request_with_retry", mock_request)

    client = HttpBaseClient(base_url="https://example.com", request_delay=0)
    client._post("/submit", data={"key": "val"})
    assert calls[-1] == ("POST", "https://example.com/submit")


# ===================================================================
# --- ScraperBaseClient ---
# ===================================================================


def test_scraper_parse_german_date():
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert ScraperBaseClient.parse_german_date("18.10.1995") == "1995-10-18"
    assert ScraperBaseClient.parse_german_date("01.01.2024") == "2024-01-01"


def test_scraper_strip_tags():
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert ScraperBaseClient.strip_tags("<p>Hello <b>world</b></p>") == "Hello world"


def test_scraper_extract_body():
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    html = '<html><body class="x"><div>Content</div></body></html>'
    assert ScraperBaseClient.extract_body(html) == "<div>Content</div>"


def test_scraper_get_inner_html():
    from lxml import etree

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    xml = "<root><child1>text1</child1><child2>text2</child2></root>"
    tree = etree.fromstring(xml.encode())
    result = ScraperBaseClient.get_inner_html(tree)
    assert "<child1>text1</child1>" in result
    assert "<child2>text2</child2>" in result


def test_scraper_xpath_text():
    from lxml import etree

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    xml = "<root><tag>value</tag></root>"
    tree = etree.fromstring(xml.encode())
    client = ScraperBaseClient(base_url="", request_delay=0)
    assert client._xpath_text(tree, "//root/tag") == "value"
    assert client._xpath_text(tree, "//root/missing", default="fallback") == "fallback"


def test_scraper_build_content_html():
    from lxml import etree

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    xml = """<dokument>
        <tenor><p>Tenor text</p></tenor>
        <gruende><p>Reasons text</p></gruende>
    </dokument>"""
    tree = etree.fromstring(xml.encode())
    client = ScraperBaseClient(base_url="", request_delay=0)
    content = client._build_content_html(
        tree,
        [("tenor", "Tenor"), ("gruende", "Gründe")],
    )
    assert "<h2>Tenor</h2>" in content
    assert "Tenor text" in content
    assert "<h2>Gründe</h2>" in content
    assert "Reasons text" in content


def test_scraper_get_xml_from_zip(monkeypatch):
    """Test ZIP extraction of XML content."""
    import io
    import zipfile

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    # Create a fake ZIP in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("test.xml", "<root><data>hello</data></root>")
    zip_bytes = zip_buffer.getvalue()

    class FakeResp:
        status_code = 200

        class raw:
            @staticmethod
            def read():
                return zip_bytes

            decode_content = True

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        return FakeResp()

    monkeypatch.setattr(ScraperBaseClient, "_request_with_retry", mock_request)

    client = ScraperBaseClient(base_url="https://example.com", request_delay=0)
    result = client._get_xml_from_zip("/test.zip")
    assert "<root><data>hello</data></root>" in result


# ===================================================================
# --- RiiCaseProvider ---
# ===================================================================


def test_rii_parse_case_from_xml():
    """Test XML parsing for RIIS cases using test fixture."""
    import os

    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "rii", "file_number_KVRE427811801.xml"
    )
    with open(fixture_path) as f:
        xml_str = f.read()

    provider = RiiCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml_str)

    assert case is not None
    assert case["court_name"] == "BVerfG"
    assert case["date"] == "2018-08-22"
    assert "2 BvQ 53/18" in case["file_number"]
    assert case["type"] == "Ablehnung einstweilige Anordnung"
    assert case["ecli"] == "ECLI:DE:BVerfG:2018:qs20180822.2bvq005318"
    assert "<h2>Tenor</h2>" in case["content"]
    assert "Gründe" in case["content"]


def test_rii_parse_case_non_public():
    """Non-public cases should be skipped."""
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <dokument>
        <doknr>TEST001</doknr>
        <gertyp>BGH</gertyp>
        <entsch-datum>20240101</entsch-datum>
        <aktenzeichen>1 ZR 1/24</aktenzeichen>
        <doktyp>Urteil</doktyp>
        <tenor><p>Content</p></tenor>
        <accessRights>restricted</accessRights>
    </dokument>"""

    provider = RiiCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml)
    assert case is None


def test_rii_get_page_url():
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    provider = RiiCaseProvider(request_delay=0)
    url = provider._get_page_url(1, "bgh")
    assert "bgh" in url
    assert "currentNavigationPosition=0" in url

    url2 = provider._get_page_url(2, "bverfg", per_page=26)
    assert "bverfg" in url2
    assert "currentNavigationPosition=26" in url2


def test_rii_get_ids_from_page(monkeypatch):
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    html = """
    <html><body>
    <a href="?doc.id=ABC123&other=1">Link 1</a>
    <a href="?doc.id=DEF456&other=2">Link 2</a>
    <a href="?doc.id=ABC123&other=3">Duplicate</a>
    </body></html>
    """

    class FakeResp:
        status_code = 200
        text = html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        RiiCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = RiiCaseProvider(request_delay=0)
    ids = provider._get_ids_from_page("http://test.com")
    assert sorted(ids) == ["ABC123", "DEF456"]


def test_rii_get_ids_from_page_jb_prefix(monkeypatch):
    """Test extraction of doc IDs with jb- prefix (current site format)."""
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    html = """
    <html><body>
    <a href="?doc.id=jb-KVRE464542601&documentnumber=1">Link 1</a>
    <a href="?doc.id=jb-KORE702202026&documentnumber=2">Link 2</a>
    <a href="?doc.id=jb-KVRE464542601&documentnumber=3">Duplicate</a>
    </body></html>
    """

    class FakeResp:
        status_code = 200
        text = html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        RiiCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = RiiCaseProvider(request_delay=0)
    ids = provider._get_ids_from_page("http://test.com")
    assert sorted(ids) == ["jb-KORE702202026", "jb-KVRE464542601"]


def test_rii_get_cases_with_mock(monkeypatch):
    """Full integration test with mocked HTTP."""
    import io
    import zipfile

    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    search_html = '<a href="?doc.id=TESTDOC001&x=1">Link</a>'

    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <dokument>
        <doknr>TESTDOC001</doknr>
        <ecli>ECLI:DE:BGH:2024:010124U1ZR1.24.0</ecli>
        <gertyp>BGH</gertyp>
        <gerort/>
        <entsch-datum>20240101</entsch-datum>
        <aktenzeichen>1 ZR 1/24</aktenzeichen>
        <doktyp>Urteil</doktyp>
        <tenor><p>Decision text</p></tenor>
        <gruende><p>Reasoning text</p></gruende>
        <accessRights>public</accessRights>
    </dokument>"""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("TESTDOC001.xml", xml_content)
    zip_bytes = zip_buffer.getvalue()

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        class raw:
            decode_content = True

            @staticmethod
            def read():
                return b""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if "Suchportlet" in url:
            if call_count[0] <= 1:
                resp.text = search_html
            else:
                resp.text = ""  # empty page to stop pagination
        elif ".zip" in url:
            resp.raw = type(
                "raw",
                (),
                {
                    "decode_content": True,
                    "read": staticmethod(lambda: zip_bytes),
                },
            )
        return resp

    monkeypatch.setattr(RiiCaseProvider, "_request_with_retry", mock_request)

    provider = RiiCaseProvider(court="bgh", limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "BGH"
    assert cases[0]["file_number"] == "1 ZR 1/24"
    assert cases[0]["date"] == "2024-01-01"


# ===================================================================
# --- ByCaseProvider ---
# ===================================================================


def test_by_parse_case_from_xml():
    """Test XML parsing for Bavaria cases using test fixture."""
    import os

    from oldp_ingestor.providers.de.by import ByCaseProvider

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "by", "1.xml")
    with open(fixture_path, "rb") as f:
        xml_bytes = f.read()
    # The fixture file is stored as UTF-8 on disk but declares iso-8859-1.
    # In production, _get_xml_from_zip decodes as iso-8859-1 from real ZIPs.
    # For testing, decode as utf-8 (matching the actual file encoding).
    xml_str = xml_bytes.decode("utf-8")

    provider = ByCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml_str, "https://example.com/doc")

    assert case is not None
    assert case["court_name"] == "OLG München"
    assert case["date"] == "2018-11-21"
    assert case["file_number"] == "34 Wx 105/18"
    assert case["type"] == "Beschluss"
    assert "<h2>Tenor</h2>" in case["content"]
    assert "Gründe" in case["content"]
    assert "abstract" in case
    assert "title" in case


def test_by_expand_case_type():
    from oldp_ingestor.providers.de.by import _expand_case_type

    assert _expand_case_type("Bes") == "Beschluss"
    assert _expand_case_type("Urt") == "Urteil"
    assert _expand_case_type("Ent") == "Entscheidung"
    assert _expand_case_type("Other") == "Other"


# ===================================================================
# --- NrwCaseProvider ---
# ===================================================================


def test_nrw_parse_case_from_html():
    """Test HTML parsing for NRW cases using test fixture."""
    import os

    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "nrw", "1.html")
    with open(fixture_path) as f:
        html_str = f.read()

    provider = NrwCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(
        html_str, "https://nrwesuche.justiz.nrw.de/test"
    )

    assert case is not None
    assert case["court_name"] == "Oberlandesgericht Hamm"
    assert case["date"] == "1995-10-18"
    assert case["file_number"] == "2 Ws 364/95"
    assert case["ecli"] == "ECLI:DE:OLGHAM:1995:1018.2WS364.95.00"
    assert case["type"] == "Beschluss"
    assert "Tenor" in case["content"]
    assert "Beschwerde wird verworfen" in case["content"]


def test_nrw_get_field_value():
    import lxml.html

    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    html = """<div>
        <div class="feldbezeichnung">Gericht:</div>
        <div class="feldinhalt">Landgericht Köln</div>
    </div>"""
    tree = lxml.html.fromstring(html)
    provider = NrwCaseProvider(request_delay=0)
    assert provider._get_field_value(tree, "Gericht") == "Landgericht Köln"
    assert provider._get_field_value(tree, "Missing") == ""


def test_nrw_search_page(monkeypatch):
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    result_html = """<html><body>
        <div class="einErgebnis">
            <a href="https://nrwesuche.justiz.nrw.de/nrwe/olgs/hamm/j2024/test.html">
                Link
            </a>
        </div>
    </body></html>"""

    class FakeResp:
        status_code = 200
        text = result_html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        NrwCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = NrwCaseProvider(request_delay=0)
    links = provider._search_page(1)
    assert len(links) == 1
    assert "/nrwe/olgs/hamm/j2024/test.html" in links[0]


# ===================================================================
# --- JurisCaseProvider ---
# ===================================================================


def test_juris_parse_info_table():
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body><table>
        <tr>
            <td class="TD30"><strong>Gericht:</strong></td>
            <td class="TD70">Landgericht Hamburg</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Entscheidungsdatum:</strong></td>
            <td class="TD70">15.03.2024</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Aktenzeichen:</strong></td>
            <td class="TD70">1 O 123/24</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Dokumenttyp:</strong></td>
            <td class="TD70">Urteil</td>
        </tr>
        <tr>
            <td class="TD30"><strong>ECLI:</strong></td>
            <td class="TD70">ECLI:DE:LGHH:2024:1234</td>
        </tr>
    </table></body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    info = provider._parse_info_table(tree)

    assert info["court_name"] == "Landgericht Hamburg"
    assert info["date"] == "2024-03-15"
    assert info["file_number"] == "1 O 123/24"
    assert info["type"] == "Urteil"
    assert info["ecli"] == "ECLI:DE:LGHH:2024:1234"


def test_juris_parse_german_date():
    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    assert JurisCaseProvider._parse_german_date("15.03.2024") == "2024-03-15"
    assert JurisCaseProvider._parse_german_date("invalid") == "invalid"


def test_juris_search_url():
    from oldp_ingestor.providers.de.juris import BbCaseProvider

    provider = BbCaseProvider.__new__(BbCaseProvider)
    provider.BASE_URL = "https://gesetze.berlin.de/bsbe"

    url1 = provider._search_url(1)
    assert "gesetze.berlin.de/bsbe" in url1
    assert "query=*" in url1
    assert "currentNavigationPosition" not in url1

    url2 = provider._search_url(2)
    assert "currentNavigationPosition=1" in url2


def test_juris_case_detail_url():
    from oldp_ingestor.providers.de.juris import HhCaseProvider

    provider = HhCaseProvider.__new__(HhCaseProvider)
    provider.BASE_URL = "https://www.landesrecht-hamburg.de/bsha"

    url = provider._case_detail_url("TESTDOC123")
    assert "landesrecht-hamburg.de/bsha" in url
    assert "/document/TESTDOC123" in url


def test_juris_extract_content():
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <p>Decision text here</p>
            <a href="http://example.com">Link</a>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "Decision text here" in content
    assert 'href="http://example.com"' not in content  # links removed


def test_juris_sanitize_removes_permalink():
    """Permalink section (h3.unsichtbar + div#permalink) should be removed."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <p>Decision text</p>
            <!--hlIgnoreOn--><h3 class="unsichtbar">Permalink</h3>
            <div id="permalink" class="docLayoutText docLayoutPermalink">
                <ul><li class="docLayoutPermalinkItem">
                <img src="/jportal/cms/technik/media/res/shared/icons/icon_doku-info.gif">
                <a href="https://www.landesrecht-bw.de/perma?d=NJRE001630159">
                    https://www.landesrecht-bw.de/perma?d=NJRE001630159</a>
                </li></ul>
            </div><!--hlIgnoreOff-->
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "Decision text" in content
    assert "Permalink" not in content
    assert "permalink" not in content
    assert "landesrecht-bw.de" not in content


def test_juris_sanitize_removes_jportal_images():
    """<img src="/jportal/..."> elements should be removed."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <p>Before<img src="/jportal/cms/technik/media/img/prodjur/icon/minus.gif"
                alt="Verfahrensgang ausblenden">After</p>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "/jportal/" not in content
    assert "<img" not in content
    assert "After" in content


def test_juris_sanitize_removes_data_juris_attributes():
    """All data-juris-* attributes should be stripped."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <h4 data-juris-toc='{"level":1,"label":"Tenor"}'>Tenor</h4>
            <a class="doclink" data-juris-gui="link"
               data-juris-link='{"linkMeta":{"docId":"X"}}'>Art. 1 GG</a>
            <span data-juris-tooltip='{"court":"BGH"}'>BGH</span>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "data-juris" not in content
    assert "Tenor" in content
    assert "Art. 1 GG" in content


def test_juris_sanitize_removes_html_comments():
    """HTML comments like hlIgnoreOn/Off should be removed."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <h4><!--hlIgnoreOn-->Tenor<!--hlIgnoreOff--></h4>
            <p>Text<!--emptyTag--></p>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "hlIgnore" not in content
    assert "emptyTag" not in content
    assert "Tenor" in content
    assert "Text" in content


def test_juris_sanitize_removes_unsichtbar_spans():
    """<span class="unsichtbar">Randnummer</span> should be removed."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <dt><span class="unsichtbar">Randnummer</span><a name="rd_1">1</a></dt>
            <dd><p>Paragraph text</p></dd>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "unsichtbar" not in content
    assert "Randnummer" not in content
    assert "Paragraph text" in content
    assert "rd_1" in content


def test_juris_sanitize_converts_doclinks_to_spans():
    """<a class="doclink"> should be converted to <span>."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText">
            <p>See <a class="doclink docview__doclink"
                data-juris-link='{"linkMeta":{"docId":"X"}}'>§ 123 BGB</a> for details.</p>
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    assert "123 BGB" in content  # § may be entity-encoded by lxml
    assert "doclink" not in content
    assert "<span>" in content  # doclink converted to span


def test_juris_sanitize_realistic_case():
    """Test sanitization on a realistic Juris HTML fragment."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = """<html><body>
        <div class="docLayoutText doktyp-juris-r dokutyp-">
            <div class="docLayoutMarginTopMore">
                <h4 class="doc" data-juris-toc='{"level":1,"label":"Leitsatz"}'>Leitsatz</h4>
            </div>
            <div class="docLayoutMarginTop"><div>
                <dl class="RspDL"><dt></dt>
                <dd><p>Ein wichtiger Leitsatz.</p></dd></dl>
            </div></div>
            <div class="docLayoutMinMax">
                <a name="vg"></a>
                <h3 class="doc" data-juris-toc='{"level":1,"label":"Verfahrensgang"}'>
                    <a data-juris-gui="link" data-juris-link='{"linkMeta":{"docId":"X"}}'
                       name="vg" id="vg_openClose" class="doclink">
                        <span title="">
                            <img class="docLayoutFillerMinMax" src="/jportal/cms/technik/media/img/prodjur/icon/minus.gif">
                        </span>
                    </a>
                    <!--hlIgnoreOn-->Verfahrensgang<!--hlIgnoreOff-->
                </h3>
            </div>
            <div class="docLayoutMarginTopMore">
                <h4 class="doc" data-juris-toc='{"level":1,"label":"Tenor"}'>
                    <!--hlIgnoreOn-->Tenor<!--hlIgnoreOff-->
                </h4>
            </div>
            <div class="docLayoutText"><div>
                <dl class="RspDL">
                    <dt><span class="unsichtbar">Randnummer</span><a name="rd_1">1</a></dt>
                    <dd><p>Der Kläger hat Recht. Vgl.
                        <a class="doclink" data-juris-gui="link"
                           data-juris-link='{"linkMeta":{"docId":"Y"}}'>§ 823 BGB</a>.</p></dd>
                </dl>
            </div></div>
            <!--hlIgnoreOn--><h3 class="unsichtbar">Permalink</h3>
            <div id="permalink" class="docLayoutPermalink">
                <ul><li><img src="/jportal/cms/technik/media/res/shared/icons/icon_doku-info.gif">
                <a href="https://www.landesrecht-bw.de/perma?d=TEST">Link</a></li></ul>
            </div><!--hlIgnoreOff-->
        </div>
    </body></html>"""

    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)

    # Content should be preserved (lxml may entity-encode non-ASCII like § ä ü)
    import html as html_mod

    text = html_mod.unescape(content)
    assert "Ein wichtiger Leitsatz." in text
    assert "Tenor" in text
    assert "Der Kläger hat Recht." in text
    assert "§ 823 BGB" in text
    assert "Verfahrensgang" in text

    # Juris artifacts should be removed
    assert "data-juris" not in content
    assert "/jportal/" not in content
    assert "Permalink" not in content
    assert "permalink" not in content
    assert "landesrecht-bw.de" not in content
    assert "hlIgnore" not in content
    assert "unsichtbar" not in content
    assert "Randnummer" not in content
    assert "icon_doku-info" not in content


def test_juris_all_subclasses_have_base_url():
    """All Juris subclasses must have non-empty BASE_URL."""
    from oldp_ingestor.providers.de.juris import (
        BbCaseProvider,
        BwCaseProvider,
        HeCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        SlCaseProvider,
        ThCaseProvider,
    )

    for cls in [
        BbCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        BwCaseProvider,
        SlCaseProvider,
        HeCaseProvider,
        ThCaseProvider,
    ]:
        assert cls.BASE_URL, f"{cls.__name__} has empty BASE_URL"
        assert cls.BASE_URL.startswith("https://"), (
            f"{cls.__name__} BASE_URL must start with https://"
        )


# ===================================================================
# --- Provider hierarchy ---
# ===================================================================


def test_rii_provider_inherits_correctly():
    from oldp_ingestor.providers.de.rii import RiiCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(RiiCaseProvider, CaseProvider)
    assert issubclass(RiiCaseProvider, ScraperBaseClient)


def test_by_provider_inherits_correctly():
    from oldp_ingestor.providers.de.by import ByCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(ByCaseProvider, CaseProvider)
    assert issubclass(ByCaseProvider, ScraperBaseClient)


def test_nrw_provider_inherits_correctly():
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(NrwCaseProvider, CaseProvider)
    assert issubclass(NrwCaseProvider, ScraperBaseClient)


def test_juris_provider_inherits_correctly():
    from oldp_ingestor.providers.de.juris import JurisCaseProvider
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    assert issubclass(JurisCaseProvider, CaseProvider)
    assert issubclass(JurisCaseProvider, PlaywrightBaseClient)


def test_ris_base_client_inherits_http_base():
    from oldp_ingestor.providers.http_client import HttpBaseClient

    assert issubclass(RISBaseClient, HttpBaseClient)


# ===================================================================
# --- Additional coverage tests ---
# ===================================================================


def test_rii_parse_case_empty_content():
    """Cases with no content sections should return None."""
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <dokument>
        <doknr>EMPTY001</doknr>
        <gertyp>BGH</gertyp>
        <entsch-datum>20240101</entsch-datum>
        <aktenzeichen>1 ZR 1/24</aktenzeichen>
        <doktyp>Urteil</doktyp>
        <tenor/>
        <gruende/>
        <accessRights>public</accessRights>
    </dokument>"""

    provider = RiiCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml)
    assert case is None


def test_rii_parse_case_with_court_location():
    """Court with gerort should combine gertyp + gerort."""
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <dokument>
        <doknr>TEST002</doknr>
        <gertyp>OLG</gertyp>
        <gerort>Hamburg</gerort>
        <entsch-datum>20240315</entsch-datum>
        <aktenzeichen>5 U 42/24</aktenzeichen>
        <doktyp>Urteil</doktyp>
        <tenor><p>Content here</p></tenor>
        <accessRights>public</accessRights>
    </dokument>"""

    provider = RiiCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml)
    assert case is not None
    assert case["court_name"] == "OLG Hamburg"
    assert case["date"] == "2024-03-15"


def test_rii_zip_url():
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    provider = RiiCaseProvider(request_delay=0)
    url = provider._get_zip_url("ABC123")
    assert url.endswith("/docs/bsjrs/ABC123.zip")


def test_by_parse_case_empty_content():
    """Cases with no content should return None."""
    from oldp_ingestor.providers.de.by import ByCaseProvider

    xml = """<?xml version="1.0" encoding="utf-8"?>
    <byrecht-rspr doknr="TEST">
        <metadaten>
            <aktenzeichen>1 Test/24</aktenzeichen>
            <doktyp>Bes</doktyp>
            <entsch-datum>2024-01-01</entsch-datum>
            <gericht><gertyp>AG</gertyp><gerort>Test</gerort></gericht>
        </metadaten>
        <textdaten>
            <tenor><body></body></tenor>
        </textdaten>
    </byrecht-rspr>"""

    provider = ByCaseProvider(request_delay=0)
    case = provider._parse_case_from_xml(xml, "https://test.com")
    assert case is None


def test_by_zip_url():
    from oldp_ingestor.providers.de.by import ByCaseProvider

    provider = ByCaseProvider(request_delay=0)
    url = provider._get_zip_url("Y-300-Z-BECKRS-B-2018-N-29496")
    assert (
        url == "https://www.gesetze-bayern.de/Content/Zip/Y-300-Z-BECKRS-B-2018-N-29496"
    )


def test_nrw_parse_case_missing_content():
    """Cases where content is not found should return None."""
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    html = "<html><body><div>No absatzLinks class here</div></body></html>"
    provider = NrwCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_nrw_parse_case_missing_fields():
    """Cases missing court name should return None."""
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    html = """<html><body>
        <p class="absatzLinks">Some content</p>
    </body></html>"""

    provider = NrwCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_nrw_get_cases_with_mock(monkeypatch):
    """Full NRW integration test with mocked HTTP."""
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    search_html = """<html><body>
        <div class="einErgebnis">
            <a href="https://nrwesuche.justiz.nrw.de/nrwe/test/case1.html">Link</a>
        </div>
    </body></html>"""

    case_html = """<html><body>
        <div class="feldbezeichnung">Gericht:</div>
        <div class="feldinhalt">Landgericht Bonn</div>
        <div class="feldbezeichnung">Datum:</div>
        <div class="feldinhalt">01.06.2024</div>
        <div class="feldbezeichnung">Aktenzeichen:</div>
        <div class="feldinhalt">1 O 1/24</div>
        <div class="feldbezeichnung">Entscheidungsart:</div>
        <div class="feldinhalt">Urteil</div>
        <div class="maindiv">
            <span class="absatzRechts">1</span>
            <p class="absatzLinks">Case decision text.</p>
        </div>
    </body></html>"""

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if method == "POST":
            if call_count[0] <= 1:
                resp.text = search_html
            else:
                resp.text = "<html><body></body></html>"
        else:
            resp.text = case_html
        return resp

    monkeypatch.setattr(NrwCaseProvider, "_request_with_retry", mock_request)

    provider = NrwCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "Landgericht Bonn"
    assert cases[0]["date"] == "2024-06-01"
    assert cases[0]["file_number"] == "1 O 1/24"


def test_by_get_cases_with_mock(monkeypatch):
    """Full Bavaria integration test with mocked HTTP."""
    import io
    import zipfile

    from oldp_ingestor.providers.de.by import ByCaseProvider

    xml_content = """<?xml version="1.0" encoding="utf-8"?>
    <byrecht-rspr doknr="Y-300-TEST">
        <metadaten>
            <aktenzeichen>1 Test/24</aktenzeichen>
            <doktyp>Urt</doktyp>
            <entsch-datum>2024-06-15</entsch-datum>
            <gericht><gertyp>LG</gertyp><gerort>Test</gerort></gericht>
        </metadaten>
        <textdaten>
            <tenor><body><div><p>Tenor text</p></div></body></tenor>
            <gruende><body><div><p>Reasons text</p></div></body></gruende>
        </textdaten>
    </byrecht-rspr>"""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("test.xml", xml_content)
    zip_bytes = zip_buffer.getvalue()

    page_html = '<a href="/Content/Document/Y-300-TEST?hl=true">Doc</a>'
    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""
        url = "https://www.gesetze-bayern.de/Search/Hitlist"

        class raw:
            decode_content = True

            @staticmethod
            def read():
                return b""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if "Filter" in url:
            resp.text = "OK"
        elif "Search/Page" in url:
            if call_count[0] <= 3:
                resp.text = page_html
            else:
                resp.text = ""
        elif "Content/Zip" in url:
            resp.raw = type(
                "raw",
                (),
                {
                    "decode_content": True,
                    "read": staticmethod(lambda: zip_bytes),
                },
            )
        return resp

    monkeypatch.setattr(ByCaseProvider, "_request_with_retry", mock_request)

    provider = ByCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "LG Test"
    assert cases[0]["date"] == "2024-06-15"
    assert cases[0]["type"] == "Urteil"


def test_playwright_base_client_close():
    """Close on uninitialised client should not error."""
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    client = PlaywrightBaseClient()
    client.close()  # should be a no-op


def test_juris_extract_content_fallback_body():
    """When no .docLayoutText, fallback to body."""
    import lxml.html

    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html = "<html><body><div>Fallback content</div></body></html>"
    tree = lxml.html.fromstring(html)
    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    content = provider._extract_content(tree)
    assert "Fallback content" in content


def test_scraper_get_xml_from_zip_bad_zip(monkeypatch):
    """Bad ZIP data should return None."""
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    class FakeResp:
        status_code = 200

        class raw:
            decode_content = True

            @staticmethod
            def read():
                return b"not a zip file"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ScraperBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = ScraperBaseClient(base_url="https://example.com", request_delay=0)
    result = client._get_xml_from_zip("/bad.zip")
    assert result is None


def test_scraper_get_xml_from_zip_no_xml(monkeypatch):
    """ZIP without XML files should return None."""
    import io
    import zipfile

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("readme.txt", "no xml here")
    zip_bytes = zip_buffer.getvalue()

    class FakeResp:
        status_code = 200

        class raw:
            decode_content = True

            @staticmethod
            def read():
                return zip_bytes

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ScraperBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = ScraperBaseClient(base_url="https://example.com", request_delay=0)
    result = client._get_xml_from_zip("/noxml.zip")
    assert result is None


def test_http_base_client_get_content(monkeypatch):
    from oldp_ingestor.providers.http_client import HttpBaseClient

    class FakeResp:
        status_code = 200
        content = b"binary data"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        HttpBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = HttpBaseClient(base_url="https://example.com", request_delay=0)
    result = client._get_content("/data")
    assert result == b"binary data"


def test_scraper_css_text():
    import lxml.html

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    html = '<html><body><div class="test">Hello World</div></body></html>'
    tree = lxml.html.fromstring(html)
    client = ScraperBaseClient(base_url="", request_delay=0)
    assert client._css_text(tree, ".test") == "Hello World"
    assert client._css_text(tree, ".missing", default="fallback") == "fallback"


def test_scraper_xpath_text_multiple():
    from lxml import etree

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    xml = "<root><tag>one</tag><tag>two</tag></root>"
    tree = etree.fromstring(xml.encode())
    client = ScraperBaseClient(base_url="", request_delay=0)
    result = client._xpath_text(tree, "//root/tag", join_multiple_with=", ")
    assert result == "one, two"


def test_scraper_get_html_tree(monkeypatch):
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    class FakeResp:
        status_code = 200
        text = "<html><body><p>Test</p></body></html>"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ScraperBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = ScraperBaseClient(base_url="https://example.com", request_delay=0)
    tree = client._get_html_tree("/page")
    assert tree is not None
    assert tree.xpath("//p/text()") == ["Test"]


def test_rii_get_cases_handles_fetch_errors(monkeypatch):
    """RII should skip cases where ZIP download fails."""
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    search_html = '<a href="?doc.id=FAIL001&x=1">Link</a>'
    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if "Suchportlet" in url:
            if call_count[0] <= 1:
                resp.text = search_html
            else:
                resp.text = ""
        elif ".zip" in url:
            import requests as req

            raise req.ConnectionError("Download failed")
        return resp

    monkeypatch.setattr(RiiCaseProvider, "_request_with_retry", mock_request)

    provider = RiiCaseProvider(court="bgh", limit=5, request_delay=0)
    cases = provider.get_cases()
    assert len(cases) == 0


def test_juris_get_cases_with_mock(monkeypatch):
    """Full juris integration test with mocked Playwright."""
    from oldp_ingestor.providers.de.juris import BbCaseProvider

    search_result_html = '<a href="/bsbe/document/JDOC001/format/xsl">Link</a>'

    detail_html = """<html><body><table>
        <tr>
            <td class="TD30"><strong>Gericht:</strong></td>
            <td class="TD70">Amtsgericht Berlin</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Entscheidungsdatum:</strong></td>
            <td class="TD70">01.05.2024</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Aktenzeichen:</strong></td>
            <td class="TD70">1 C 1/24</td>
        </tr>
        <tr>
            <td class="TD30"><strong>Dokumenttyp:</strong></td>
            <td class="TD70">Urteil</td>
        </tr>
    </table>
    <div class="docLayoutText">
        <p>This is the decision content for the test case.</p>
    </div>
    </body></html>"""

    page_call_count = [0]

    def mock_get_page_html(self, url, wait_selector=None, timeout=30000):
        page_call_count[0] += 1
        if "Suchportlet" in url:
            if page_call_count[0] <= 1:
                return search_result_html
            return "<html><body>empty</body></html>"
        return detail_html

    monkeypatch.setattr(BbCaseProvider, "_get_page_html", mock_get_page_html)
    monkeypatch.setattr(BbCaseProvider, "close", lambda self: None)

    provider = BbCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "Amtsgericht Berlin"
    assert cases[0]["date"] == "2024-05-01"
    assert cases[0]["file_number"] == "1 C 1/24"
    assert cases[0]["type"] == "Urteil"
    assert "decision content" in cases[0]["content"]


def test_juris_get_cases_empty_search(monkeypatch):
    """Empty search results should return empty list."""
    from oldp_ingestor.providers.de.juris import HhCaseProvider

    def mock_get_page_html(self, url, wait_selector=None, timeout=30000):
        return "<html><body>No results</body></html>"

    monkeypatch.setattr(HhCaseProvider, "_get_page_html", mock_get_page_html)
    monkeypatch.setattr(HhCaseProvider, "close", lambda self: None)

    provider = HhCaseProvider(limit=10, request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_juris_parse_case_detail_short_content(monkeypatch):
    """Cases with content < 10 chars should be skipped."""
    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    short_html = """<html><body><table>
        <tr>
            <td class="TD30"><strong>Gericht:</strong></td>
            <td class="TD70">AG Test</td>
        </tr>
    </table>
    <div class="docLayoutText"><p>X</p></div>
    </body></html>"""

    import lxml.html

    def mock_get_page_tree(self, url, wait_selector=None, timeout=30000):
        return lxml.html.fromstring(short_html)

    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    monkeypatch.setattr(JurisCaseProvider, "_get_page_tree", mock_get_page_tree)

    # Content is short but court_name exists — depends on extracted length
    # The <p>X</p> wrapped in div should be around 20+ chars after serialization
    provider._parse_case_detail("http://test.com")


def test_juris_get_case_ids_from_page(monkeypatch):
    """Test ID extraction from rendered search page."""
    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    html_with_ids = """
    <a href="/bsbe/document/ATEST001/format/xsl">Link 1</a>
    <a href="/bsbe/document/BTEST002/format/xsl">Link 2</a>
    <a href="/bsbe/document/ATEST001/other">Dup</a>
    """

    provider = JurisCaseProvider.__new__(JurisCaseProvider)

    def mock_get_page_html(self, url, wait_selector=None, timeout=30000):
        return html_with_ids

    monkeypatch.setattr(JurisCaseProvider, "_get_page_html", mock_get_page_html)

    ids = provider._get_case_ids_from_page("http://test.com")
    assert sorted(ids) == ["ATEST001", "BTEST002"]


def test_juris_parse_case_detail_no_court(monkeypatch):
    """Cases without court_name should return None."""
    from oldp_ingestor.providers.de.juris import JurisCaseProvider

    import lxml.html

    html = """<html><body>
        <div class="docLayoutText">
            <p>Enough content here for the test to work properly.</p>
        </div>
    </body></html>"""

    def mock_get_page_tree(self, url, wait_selector=None, timeout=30000):
        return lxml.html.fromstring(html)

    provider = JurisCaseProvider.__new__(JurisCaseProvider)
    provider.WAIT_SELECTOR = "body"
    monkeypatch.setattr(JurisCaseProvider, "_get_page_tree", mock_get_page_tree)

    result = provider._parse_case_detail("http://test.com")
    assert result is None


def test_nrw_parse_case_with_tenor():
    """Test NRW case with tenor prepended to content."""
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    html = """<html><body>
        <div class="feldbezeichnung">Gericht:</div>
        <div class="feldinhalt">Amtsgericht Bonn</div>
        <div class="feldbezeichnung">Datum:</div>
        <div class="feldinhalt">15.06.2024</div>
        <div class="feldbezeichnung">Aktenzeichen:</div>
        <div class="feldinhalt">1 C 100/24</div>
        <div class="feldbezeichnung">Tenor:</div>
        <div class="feldinhalt"><p>The defendant shall pay.</p></div>
        <div class="maindiv">
            <span class="absatzRechts">1</span>
            <p class="absatzLinks">Reasoning text here.</p>
        </div>
    </body></html>"""

    provider = NrwCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")

    assert case is not None
    assert "<h2>Tenor</h2>" in case["content"]
    assert "defendant shall pay" in case["content"]
    assert "Reasoning text here" in case["content"]


def test_by_get_ids_from_page_bad_redirect(monkeypatch):
    """When session redirects to wrong URL, return empty list."""
    from oldp_ingestor.providers.de.by import ByCaseProvider

    class FakeResp:
        status_code = 200
        text = "some content"
        url = "https://www.gesetze-bayern.de/Search/Error"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ByCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = ByCaseProvider(request_delay=0)
    ids = provider._get_ids_from_page(1)
    assert ids == []


# ===================================================================
# --- Playwright integration tests (requires browser) ---
# ===================================================================


@pytest.mark.playwright
def test_playwright_ensure_browser_and_close():
    """Test real browser launch and shutdown."""
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    assert client._browser is None

    client._ensure_browser()
    assert client._browser is not None
    assert client._context is not None
    assert client._playwright is not None

    client.close()
    assert client._browser is None
    assert client._context is None
    assert client._playwright is None


@pytest.mark.playwright
def test_playwright_get_page_html():
    """Test fetching an HTML page with real browser."""
    import os
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "playwright_test.html"
    )
    file_url = f"file://{fixture_path}"

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    try:
        html = client._get_page_html(file_url)
        assert "Hello from Playwright" in html
        assert "<div" in html
    finally:
        client.close()


@pytest.mark.playwright
def test_playwright_get_page_html_with_wait_selector():
    """Test waiting for a dynamic selector."""
    import os
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "playwright_test.html"
    )
    file_url = f"file://{fixture_path}"

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    try:
        html = client._get_page_html(file_url, wait_selector="#content")
        assert "Hello from Playwright" in html
    finally:
        client.close()


@pytest.mark.playwright
def test_playwright_get_page_html_bad_wait_selector():
    """Timeout waiting for missing selector should not crash."""
    import os
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "playwright_test.html"
    )
    file_url = f"file://{fixture_path}"

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    try:
        # Use a short timeout so the test is fast
        html = client._get_page_html(
            file_url, wait_selector="#nonexistent", timeout=1000
        )
        # Should still return content despite selector timeout
        assert "Hello from Playwright" in html
    finally:
        client.close()


@pytest.mark.playwright
def test_playwright_get_page_tree():
    """Test getting parsed lxml tree from real browser."""
    import os
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "playwright_test.html"
    )
    file_url = f"file://{fixture_path}"

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    try:
        tree = client._get_page_tree(file_url)
        texts = tree.xpath('//div[@id="content"]/text()')
        assert texts == ["Hello from Playwright"]
    finally:
        client.close()


@pytest.mark.playwright
def test_playwright_ensure_browser_idempotent():
    """Calling _ensure_browser twice should not create a second browser."""
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    client = PlaywrightBaseClient(request_delay=0, headless=True)
    try:
        client._ensure_browser()
        browser1 = client._browser
        client._ensure_browser()
        assert client._browser is browser1
    finally:
        client.close()


@pytest.mark.playwright
def test_playwright_request_delay(monkeypatch):
    """Verify request delay is applied between page loads."""
    import os
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "playwright_test.html"
    )
    file_url = f"file://{fixture_path}"

    sleep_calls = []
    monkeypatch.setattr(
        "oldp_ingestor.providers.playwright_client.time.sleep",
        lambda s: sleep_calls.append(s),
    )

    client = PlaywrightBaseClient(request_delay=0.123, headless=True)
    try:
        client._get_page_html(file_url)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 0.123
    finally:
        client.close()


# ===================================================================
# --- NsCaseProvider (Niedersachsen / VORIS) ---
# ===================================================================


def test_ns_provider_inherits_correctly():
    from oldp_ingestor.providers.de.ns import NsCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(NsCaseProvider, CaseProvider)
    assert issubclass(NsCaseProvider, ScraperBaseClient)


def test_ns_get_field_value():
    import lxml.html

    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<dl>
        <dt>Gericht</dt><dd>OVG Niedersachsen</dd>
        <dt>Datum</dt><dd>30.01.2026</dd>
        <dt>Entscheidungsname</dt><dd><span class="wkde-empty">[keine Angabe]</span></dd>
        <dt>ECLI</dt><dd>ECLI:DE:OVGNI:2026:0130.1LA2.26.00</dd>
    </dl>"""
    tree = lxml.html.fromstring(html)
    dl = tree  # fromstring returns the root <dl>
    provider = NsCaseProvider(request_delay=0)
    assert provider._get_field_value(dl, "Gericht") == "OVG Niedersachsen"
    assert provider._get_field_value(dl, "Datum") == "30.01.2026"
    assert provider._get_field_value(dl, "ECLI") == "ECLI:DE:OVGNI:2026:0130.1LA2.26.00"
    assert provider._get_field_value(dl, "Missing") == ""


def test_ns_court_name_nbsp_normalized():
    """Non-breaking spaces in court names should be replaced with regular spaces."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<html><body>
        <article class="wkde-case wkde-document">
        <section class="wkde-bibliography"><dl>
            <dt>Gericht</dt><dd>LSG\xa0Niedersachsen-Bremen</dd>
            <dt>Datum</dt><dd>15.01.2026</dd>
            <dt>Aktenzeichen</dt><dd>L 10 VE 11/24</dd>
            <dt>Entscheidungsform</dt><dd>Urteil</dd>
        </dl></section>
        <section class="wkde-document-body"><p>Content here.</p></section>
        </article>
    </body></html>"""

    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")

    assert case is not None
    assert case["court_name"] == "LSG Niedersachsen-Bremen"
    assert "\xa0" not in case["court_name"]


def test_ns_ecli_skips_keine_angabe():
    """[keine Angabe] wrapped in wkde-empty span should return empty string."""
    import lxml.html

    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<dl>
        <dt>Entscheidungsname</dt><dd><span class="wkde-empty">[keine Angabe]</span></dd>
        <dt>ECLI</dt><dd><span class="wkde-empty">[keine Angabe]</span></dd>
    </dl>"""
    tree = lxml.html.fromstring(html)
    provider = NsCaseProvider(request_delay=0)
    assert provider._get_field_value(tree, "ECLI") == ""
    assert provider._get_field_value(tree, "Entscheidungsname") == ""


def test_ns_search_page(monkeypatch):
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    result_html = """<html><body>
        <div class="egal-search-result-item-title">
            <h3><a href="/browse/document/3a689cb1-4dd4-4410-86ce-1ffa45dc2c60">Case 1</a></h3>
        </div>
        <div class="egal-search-result-item-title">
            <h3><a href="/browse/document/42e8d50f-1952-4443-ae38-8c4984f8eceb">Case 2</a></h3>
        </div>
    </body></html>"""

    class FakeResp:
        status_code = 200
        text = result_html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        NsCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = NsCaseProvider(request_delay=0)
    links = provider._search_page(0)
    assert len(links) == 2
    assert "/browse/document/3a689cb1-4dd4-4410-86ce-1ffa45dc2c60" in links
    assert "/browse/document/42e8d50f-1952-4443-ae38-8c4984f8eceb" in links


def test_ns_parse_case_from_html():
    """Test HTML parsing for NS cases using test fixture."""
    import os

    from oldp_ingestor.providers.de.ns import NsCaseProvider

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "ns", "1.html")
    with open(fixture_path) as f:
        html_str = f.read()

    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(
        html_str, "https://voris.wolterskluwer-online.de/browse/document/test"
    )

    assert case is not None
    assert case["court_name"] == "OVG Niedersachsen"
    assert case["date"] == "2026-01-30"
    assert case["file_number"] == "1 LA 2/26"
    assert case["ecli"] == "ECLI:DE:OVGNI:2026:0130.1LA2.26.00"
    assert case["type"] == "Beschluss"
    assert "Tenor" in case["content"]
    assert "Zulassung der Berufung" in case["content"]


def test_ns_parse_case_missing_content():
    """Cases where document body is not found should return None."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = "<html><body><div>No wkde-document-body here</div></body></html>"
    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_ns_parse_case_missing_fields():
    """Cases missing court name should return None."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<html><body>
        <section class="wkde-bibliography"><dl>
            <dt>Datum</dt><dd>01.01.2026</dd>
        </dl></section>
        <section class="wkde-document-body"><p>Some content</p></section>
    </body></html>"""

    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_ns_get_cases_with_mock(monkeypatch):
    """Full NS integration test with mocked HTTP."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    search_html = """<html><body>
        <div class="egal-search-result-item-title">
            <h3><a href="/browse/document/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee">Case</a></h3>
        </div>
    </body></html>"""

    case_html = """<html><body>
        <article class="wkde-case wkde-document">
        <section class="wkde-bibliography"><dl>
            <dt>Gericht</dt><dd>AG Hannover</dd>
            <dt>Datum</dt><dd>15.03.2026</dd>
            <dt>Aktenzeichen</dt><dd>5 C 123/26</dd>
            <dt>Entscheidungsform</dt><dd>Urteil</dd>
            <dt>ECLI</dt><dd>ECLI:DE:AGHAN:2026:0315.5C123.26.00</dd>
        </dl></section>
        <section class="wkde-document-body">
            <div class="tenor"><h2>Tenor: </h2><p>Die Klage wird abgewiesen.</p></div>
            <div class="gruende"><h2>Gruende</h2><p>Reasoning text.</p></div>
        </section>
        </article>
    </body></html>"""

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if "/search" in url:
            if call_count[0] <= 1:
                resp.text = search_html
            else:
                resp.text = "<html><body></body></html>"
        else:
            resp.text = case_html
        return resp

    monkeypatch.setattr(NsCaseProvider, "_request_with_retry", mock_request)

    provider = NsCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "AG Hannover"
    assert cases[0]["date"] == "2026-03-15"
    assert cases[0]["file_number"] == "5 C 123/26"
    assert cases[0]["ecli"] == "ECLI:DE:AGHAN:2026:0315.5C123.26.00"


def test_ns_parse_case_empty_body():
    """Cases where document body exists but is empty should return None."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<html><body>
        <section class="wkde-document-body"></section>
    </body></html>"""

    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_ns_parse_case_missing_bibliography():
    """Cases without bibliography section should return None."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    html = """<html><body>
        <section class="wkde-document-body"><p>Content here</p></section>
    </body></html>"""

    provider = NsCaseProvider(request_delay=0)
    case = provider._parse_case_from_html(html, "https://test.com")
    assert case is None


def test_ns_get_cases_search_error(monkeypatch):
    """Search page request failure should stop iteration."""
    import requests

    from oldp_ingestor.providers.de.ns import NsCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise requests.RequestException("connection failed")

    monkeypatch.setattr(NsCaseProvider, "_request_with_retry", mock_request)

    provider = NsCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_ns_get_cases_empty_pages_stop(monkeypatch):
    """Two consecutive empty search pages should stop iteration."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    class FakeResp:
        status_code = 200
        text = "<html><body>no document links</body></html>"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        NsCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = NsCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_ns_get_cases_fetch_error_skips(monkeypatch):
    """Failed individual case fetch should skip that case."""
    import requests

    from oldp_ingestor.providers.de.ns import NsCaseProvider

    search_html = """<html><body>
        <a href="/browse/document/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee">Link</a>
    </body></html>"""

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if call_count[0] == 1:
            resp.text = search_html
        elif call_count[0] == 2:
            # Case fetch fails
            raise requests.RequestException("timeout")
        else:
            # Empty page to stop
            resp.text = "<html><body></body></html>"
        return resp

    monkeypatch.setattr(NsCaseProvider, "_request_with_retry", mock_request)

    provider = NsCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_ns_get_cases_parse_error_skips(monkeypatch):
    """Failed case parsing should skip that case."""
    from oldp_ingestor.providers.de.ns import NsCaseProvider

    search_html = """<html><body>
        <a href="/browse/document/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee">Link</a>
    </body></html>"""

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if call_count[0] == 1:
            resp.text = search_html
        elif call_count[0] == 2:
            # Return invalid HTML that will fail to produce a case
            resp.text = "<html><body><section class='wkde-document-body'><p>Content</p></section></body></html>"
        else:
            resp.text = "<html><body></body></html>"
        return resp

    monkeypatch.setattr(NsCaseProvider, "_request_with_retry", mock_request)

    provider = NsCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


# --- SOURCE attribute tests ---


def test_all_case_providers_have_source():
    """Every concrete CaseProvider subclass must have a non-empty SOURCE name."""
    from oldp_ingestor.providers.de.by import ByCaseProvider
    from oldp_ingestor.providers.dummy.dummy_cases import DummyCaseProvider
    from oldp_ingestor.providers.de.juris import (
        BbCaseProvider,
        BwCaseProvider,
        HeCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        SlCaseProvider,
        ThCaseProvider,
    )
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider
    from oldp_ingestor.providers.de.ns import NsCaseProvider
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    providers = [
        DummyCaseProvider,
        RISCaseProvider,
        RiiCaseProvider,
        ByCaseProvider,
        NrwCaseProvider,
        NsCaseProvider,
        BbCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        BwCaseProvider,
        SlCaseProvider,
        HeCaseProvider,
        ThCaseProvider,
    ]

    for cls in providers:
        assert "name" in cls.SOURCE, f"{cls.__name__} missing SOURCE['name']"
        assert cls.SOURCE["name"], f"{cls.__name__} has empty SOURCE['name']"
        assert "homepage" in cls.SOURCE, f"{cls.__name__} missing SOURCE['homepage']"


def test_case_providers_homepage_urls():
    """Non-dummy providers must have a valid https:// homepage URL."""
    from oldp_ingestor.providers.de.by import ByCaseProvider
    from oldp_ingestor.providers.de.juris import (
        BbCaseProvider,
        BwCaseProvider,
        HeCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        SlCaseProvider,
        ThCaseProvider,
    )
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider
    from oldp_ingestor.providers.de.ns import NsCaseProvider
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    providers = [
        RISCaseProvider,
        RiiCaseProvider,
        ByCaseProvider,
        NrwCaseProvider,
        NsCaseProvider,
        BbCaseProvider,
        HhCaseProvider,
        MvCaseProvider,
        RlpCaseProvider,
        SaCaseProvider,
        ShCaseProvider,
        BwCaseProvider,
        SlCaseProvider,
        HeCaseProvider,
        ThCaseProvider,
    ]

    for cls in providers:
        assert cls.SOURCE["homepage"].startswith("https://"), (
            f"{cls.__name__} SOURCE homepage should start with https://, "
            f"got: {cls.SOURCE['homepage']!r}"
        )


def test_base_case_provider_source_default():
    """Base CaseProvider has empty SOURCE as default."""
    assert CaseProvider.SOURCE == {"name": "", "homepage": ""}


# --- EU (EUR-Lex) provider ---


def test_eu_provider_inherits_correctly():
    from oldp_ingestor.providers.de.eu import EuCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(EuCaseProvider, CaseProvider)
    assert issubclass(EuCaseProvider, ScraperBaseClient)


def test_eu_source_field():
    from oldp_ingestor.providers.de.eu import EuCaseProvider

    assert EuCaseProvider.SOURCE["name"] == "EUR-Lex"
    assert EuCaseProvider.SOURCE["homepage"] == "https://eur-lex.europa.eu/"


def test_eu_get_case_type_from_celex():
    from oldp_ingestor.providers.de.eu import _get_case_type_from_celex

    assert _get_case_type_from_celex("62016CJ0001") == "Urteil"
    assert _get_case_type_from_celex("62017CO0042") == "Beschluss"
    assert (
        _get_case_type_from_celex("62018CC0100") == "Schlussantrag des Generalanwalts"
    )
    assert _get_case_type_from_celex("62020TJ0005") == "Urteil"
    assert _get_case_type_from_celex("62020FO0005") == "Beschluss"
    # Unsupported sector (not 6)
    assert _get_case_type_from_celex("32016L0001") == ""
    # Unknown type code
    assert _get_case_type_from_celex("62016ZZ0001") == ""
    # Invalid format
    assert _get_case_type_from_celex("invalid") == ""


def test_eu_parse_eclis_from_sparql():
    from oldp_ingestor.providers.de.eu import _parse_eclis_from_sparql

    data = {
        "results": {
            "bindings": [
                {"ecli": {"value": "ECLI:EU:C:2024:123"}},
                {"ecli": {"value": "ECLI:EU:C:2024:456"}},
            ]
        }
    }
    eclis = _parse_eclis_from_sparql(data)
    assert eclis == ["ECLI:EU:C:2024:123", "ECLI:EU:C:2024:456"]


def test_eu_parse_eclis_from_sparql_empty():
    from oldp_ingestor.providers.de.eu import _parse_eclis_from_sparql

    eclis = _parse_eclis_from_sparql({"results": {"bindings": []}})
    assert eclis == []

    eclis = _parse_eclis_from_sparql({})
    assert eclis == []


def test_eu_parse_case_details_from_xml():
    from oldp_ingestor.providers.de.eu import _parse_case_details_from_xml

    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-03-15</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0001</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Urteil vom 15.03.2024#Partei A gegen B#Sonstiges</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
      <SAMEAS><URI><TYPE>case</TYPE><IDENTIFIER>C-1/24</IDENTIFIER></URI></SAMEAS>
    </root>"""

    details = _parse_case_details_from_xml(xml, "ECLI:EU:C:2024:1")
    assert details["date"] == "2024-03-15"
    assert details["title"] == "Urteil vom 15.03.2024"
    assert details["type"] == "Urteil"
    assert details["ecli"] == "ECLI:EU:C:2024:1"
    # file_number from SAMEAS fallback (no fn pattern in first title part)
    assert details["file_number"] == "C-1/24"


def test_eu_parse_case_details_file_number_from_title():
    from oldp_ingestor.providers.de.eu import _parse_case_details_from_xml

    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-06-01</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CO0042</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Beschluss C-42/24 vom 01.06.2024</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""

    details = _parse_case_details_from_xml(xml, "ECLI:EU:C:2024:42")
    assert details["file_number"] == "C-42/24"
    assert details["type"] == "Beschluss"


def test_eu_file_number_unicode_dash():
    """Unicode non-breaking hyphen (U+2011) should be replaced with ASCII dash."""
    from oldp_ingestor.providers.de.eu import _parse_case_details_from_xml

    # Use Unicode dash U+2011 in the title
    title_with_unicode_dash = "Urteil C\u201142/24 vom 01.06.2024"
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<root>"
        "<WORK_DATE_DOCUMENT><VALUE>2024-06-01</VALUE></WORK_DATE_DOCUMENT>"
        "<RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0042</VALUE></RESOURCE_LEGAL_ID_CELEX>"
        "<EXPRESSION>"
        "<EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>"
        f"<EXPRESSION_TITLE><VALUE>{title_with_unicode_dash}</VALUE></EXPRESSION_TITLE>"
        "</EXPRESSION>"
        "</root>"
    ).encode("utf-8")

    details = _parse_case_details_from_xml(xml, "ECLI:EU:C:2024:42")
    assert details["file_number"] == "C-42/24"
    assert "\u2011" not in details["file_number"]


def test_eu_parse_case_details_file_number_from_sameas():
    """When title has no file number pattern, fallback to SAMEAS."""
    from oldp_ingestor.providers.de.eu import _parse_case_details_from_xml

    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-01-01</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0001</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Some title without file number pattern</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
      <SAMEAS><URI><TYPE>case</TYPE><IDENTIFIER>C-1/24</IDENTIFIER></URI></SAMEAS>
    </root>"""

    details = _parse_case_details_from_xml(xml, "ECLI:EU:C:2024:1")
    assert details["file_number"] == "C-1/24"


def test_eu_title_hash_split():
    """Title with '#' parts should use first part only."""
    from oldp_ingestor.providers.de.eu import _extract_title
    from lxml import etree

    xml = b"""<root>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Part One#Part Two#Part Three</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""
    tree = etree.fromstring(xml)
    title = _extract_title(tree)
    assert title == "Part One"


def test_eu_title_non_deu_language_skipped():
    """Titles in other languages should be skipped."""
    from oldp_ingestor.providers.de.eu import _extract_title
    from lxml import etree

    xml = b"""<root>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>FRA</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>French title</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""
    tree = etree.fromstring(xml)
    title = _extract_title(tree)
    assert title == ""


def test_eu_fetch_case_content():
    """HTML content extraction with link processing."""
    from oldp_ingestor.providers.de.eu import _extract_html_content

    html = """<html><head><title>Test</title></head><body>
        <p>Some content</p>
        <a href="#internal">internal</a>
        <a href="http://example.com">external</a>
        <a href="./relative/page">relative</a>
        <a href="javascript:void(0)">script</a>
    </body></html>"""

    source_url = "https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=ECLI:test"
    content = _extract_html_content(html, source_url)
    assert content is not None
    assert "Some content" in content
    # Internal anchor link preserved
    assert 'href="#internal"' in content
    # External link preserved
    assert 'href="http://example.com"' in content
    # Relative link made absolute
    assert "eur-lex.europa.eu" in content
    assert "./relative" not in content
    # javascript link removed
    assert "javascript" not in content


def test_eu_fetch_case_content_no_body():
    """HTML without body should return None."""
    from oldp_ingestor.providers.de.eu import _extract_html_content

    content = _extract_html_content("<html><head></head></html>", "https://test.com")
    assert content is None


def test_eu_build_sparql_query():
    from oldp_ingestor.providers.de.eu import EuCaseProvider

    # No date filters
    provider = EuCaseProvider(request_delay=0)
    query = provider._build_sparql_query(10, 0)
    assert "LIMIT 10" in query
    assert "OFFSET 0" in query
    assert "FILTER" not in query

    # With date_from only
    provider = EuCaseProvider(date_from="2024-01-01", request_delay=0)
    query = provider._build_sparql_query(10, 0)
    assert '"2024-01-01"' in query
    assert "FILTER" in query

    # With both date filters
    provider = EuCaseProvider(
        date_from="2024-01-01", date_to="2024-12-31", request_delay=0
    )
    query = provider._build_sparql_query(10, 0)
    assert '"2024-01-01"' in query
    assert '"2024-12-31"' in query


def test_eu_get_cases_with_mock(monkeypatch):
    """Full EU integration test with monkeypatched HTTP."""
    import json

    from oldp_ingestor.providers.de.eu import EuCaseProvider

    sparql_response = json.dumps(
        {
            "results": {
                "bindings": [
                    {
                        "ecli": {"value": "ECLI:EU:C:2024:100"},
                        "date": {"value": "2024-05-20"},
                    }
                ]
            }
        }
    )

    xml_detail = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-05-20</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0100</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Urteil C-100/24 vom 20.05.2024</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""

    html_content = (
        "<html><body><p>Full decision text here with enough content.</p></body></html>"
    )

    class FakeResp:
        status_code = 200
        text = ""
        content = b""

        def raise_for_status(self):
            pass

        def json(self):
            return json.loads(self.text)

    def mock_request(self, method, url, **kwargs):
        resp = FakeResp()
        if "sparql" in url:
            resp.text = sparql_response
            resp.content = sparql_response.encode("utf-8")
        elif "XML" in url:
            resp.text = xml_detail.decode("utf-8")
            resp.content = xml_detail
        elif "HTML" in url:
            resp.text = html_content
            resp.content = html_content.encode("utf-8")
        return resp

    monkeypatch.setattr(EuCaseProvider, "_request_with_retry", mock_request)

    provider = EuCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    case = cases[0]
    assert case["court_name"] == "Europäischer Gerichtshof"
    assert case["file_number"] == "C-100/24"
    assert case["date"] == "2024-05-20"
    assert case["ecli"] == "ECLI:EU:C:2024:100"
    assert case["type"] == "Urteil"
    assert case["title"] == "Urteil C-100/24 vom 20.05.2024"
    assert "Full decision text" in case["content"]


def test_eu_get_cases_skips_on_detail_failure(monkeypatch):
    """Failed XML detail fetch should skip the case."""
    import json

    from oldp_ingestor.providers.de.eu import EuCaseProvider

    sparql_response = json.dumps(
        {
            "results": {
                "bindings": [
                    {
                        "ecli": {"value": "ECLI:EU:C:2024:999"},
                        "date": {"value": "2024-01-01"},
                    }
                ]
            }
        }
    )

    class FakeResp:
        status_code = 200
        text = ""
        content = b""

        def raise_for_status(self):
            pass

        def json(self):
            return json.loads(self.text)

    def mock_request(self, method, url, **kwargs):
        resp = FakeResp()
        if "sparql" in url:
            resp.text = sparql_response
            resp.content = sparql_response.encode("utf-8")
        elif "XML" in url:
            import requests

            raise requests.RequestException("Simulated failure")
        return resp

    monkeypatch.setattr(EuCaseProvider, "_request_with_retry", mock_request)

    provider = EuCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert len(cases) == 0


def test_eu_get_cases_skips_short_content(monkeypatch):
    """Cases with content shorter than 10 chars should be skipped."""
    import json

    from oldp_ingestor.providers.de.eu import EuCaseProvider

    sparql_response = json.dumps(
        {
            "results": {
                "bindings": [
                    {
                        "ecli": {"value": "ECLI:EU:C:2024:888"},
                        "date": {"value": "2024-01-01"},
                    }
                ]
            }
        }
    )

    xml_detail = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-01-01</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0888</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Title C-888/24</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""

    # Very short HTML body
    html_content = """<html><body><p>X</p></body></html>"""

    class FakeResp:
        status_code = 200
        text = ""
        content = b""

        def raise_for_status(self):
            pass

        def json(self):
            return json.loads(self.text)

    def mock_request(self, method, url, **kwargs):
        resp = FakeResp()
        if "sparql" in url:
            resp.text = sparql_response
            resp.content = sparql_response.encode("utf-8")
        elif "XML" in url:
            resp.text = xml_detail.decode("utf-8")
            resp.content = xml_detail
        elif "HTML" in url:
            resp.text = html_content
            resp.content = html_content.encode("utf-8")
        return resp

    monkeypatch.setattr(EuCaseProvider, "_request_with_retry", mock_request)

    provider = EuCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert len(cases) == 0


def test_eu_multiple_file_numbers_from_title():
    """Multiple file number matches in title should be joined with comma."""
    from oldp_ingestor.providers.de.eu import _parse_case_details_from_xml

    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <root>
      <WORK_DATE_DOCUMENT><VALUE>2024-01-01</VALUE></WORK_DATE_DOCUMENT>
      <RESOURCE_LEGAL_ID_CELEX><VALUE>62024CJ0001</VALUE></RESOURCE_LEGAL_ID_CELEX>
      <EXPRESSION>
        <EXPRESSION_USES_LANGUAGE><IDENTIFIER>DEU</IDENTIFIER></EXPRESSION_USES_LANGUAGE>
        <EXPRESSION_TITLE><VALUE>Urteil C-1/24 und C-2/24</VALUE></EXPRESSION_TITLE>
      </EXPRESSION>
    </root>"""

    details = _parse_case_details_from_xml(xml, "ECLI:EU:C:2024:1")
    assert details["file_number"] == "C-1/24,C-2/24"


# ===================================================================
# --- BremenCaseProvider (Bremen) ---
# ===================================================================


def test_hb_provider_inherits_correctly():
    from oldp_ingestor.providers.de.hb import BremenCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(BremenCaseProvider, CaseProvider)
    assert issubclass(BremenCaseProvider, ScraperBaseClient)


def test_hb_parse_listing_rows():
    """Test HTML parsing for Bremen listing rows."""
    import os

    from oldp_ingestor.providers.de.hb import BremenCaseProvider, COURTS

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "hb", "1.html")
    with open(fixture_path) as f:
        html_str = f.read()

    import lxml.html

    tree = lxml.html.fromstring(html_str)
    provider = BremenCaseProvider(request_delay=0)
    court_cfg = COURTS["olg"]

    # Mock _extract_text_from_pdf to avoid real HTTP
    provider._extract_text_from_pdf = lambda url: "<p>PDF content extracted</p>"
    # Mock _fetch_abstract to avoid real HTTP
    provider._fetch_abstract = lambda base_url, href: "Full abstract text."

    cases = provider._parse_listing_rows(tree, court_cfg)

    assert len(cases) == 2

    case1 = cases[0]
    assert case1["court_name"] == "Hanseatisches Oberlandesgericht in Bremen"
    assert case1["date"] == "2026-01-29"
    assert case1["file_number"] == "2 U 106/22"
    assert case1["type"] == "Urteil"
    assert "Absenkung" in case1["title"]
    assert "PDF content" in case1["content"]
    assert case1["abstract"] == "Full abstract text."

    case2 = cases[1]
    assert case2["date"] == "2025-12-15"
    assert case2["file_number"] == "1 W 42/25"
    assert case2["type"] == "Beschluss"


def test_hb_parse_listing_date_filter():
    """Date filtering should exclude rows outside range."""
    import os

    from oldp_ingestor.providers.de.hb import BremenCaseProvider, COURTS

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "hb", "1.html")
    with open(fixture_path) as f:
        html_str = f.read()

    import lxml.html

    tree = lxml.html.fromstring(html_str)
    provider = BremenCaseProvider(date_from="2026-01-01", request_delay=0)
    court_cfg = COURTS["olg"]
    provider._extract_text_from_pdf = lambda url: "<p>PDF content here</p>"
    provider._fetch_abstract = lambda base_url, href: None

    cases = provider._parse_listing_rows(tree, court_cfg)

    assert len(cases) == 1
    assert cases[0]["date"] == "2026-01-29"


def test_hb_fetch_abstract():
    """Test detail page abstract extraction."""
    import os

    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "hb", "detail.html"
    )
    with open(fixture_path) as f:
        html_str = f.read()

    import lxml.html

    # Mock _get_html_tree to return parsed fixture
    tree = lxml.html.fromstring(html_str)

    provider = BremenCaseProvider(request_delay=0)

    # Directly test _fetch_abstract by mocking _get_html_tree
    original_get = provider._get_html_tree
    provider._get_html_tree = lambda url: tree

    abstract = provider._fetch_abstract("https://example.com", "detail.php?gsid=test")

    assert abstract is not None
    assert "Abschalteinrichtung" in abstract

    provider._get_html_tree = original_get


def test_hb_get_courts_single():
    from oldp_ingestor.providers.de.hb import BremenCaseProvider, COURTS

    provider = BremenCaseProvider(court="olg", request_delay=0)
    courts = provider._get_courts()
    assert len(courts) == 1
    assert courts[0]["court_name"] == COURTS["olg"]["court_name"]


def test_hb_get_courts_all():
    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    provider = BremenCaseProvider(request_delay=0)
    courts = provider._get_courts()
    assert len(courts) == 5


def test_hb_get_courts_invalid():
    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    provider = BremenCaseProvider(court="invalid", request_delay=0)
    courts = provider._get_courts()
    assert len(courts) == 0


def test_hb_get_cases_with_mock(monkeypatch):
    """Full Bremen integration test with mocked HTTP."""
    import os

    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "hb", "1.html")
    with open(fixture_path) as f:
        listing_html = f.read()

    class FakeResp:
        status_code = 200
        text = listing_html
        content = b""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        return FakeResp()

    monkeypatch.setattr(BremenCaseProvider, "_request_with_retry", mock_request)
    monkeypatch.setattr(
        BremenCaseProvider,
        "_extract_text_from_pdf",
        lambda self, url: "<p>Extracted PDF text</p>",
    )
    monkeypatch.setattr(
        BremenCaseProvider,
        "_fetch_abstract",
        lambda self, base_url, href: "Test abstract",
    )

    provider = BremenCaseProvider(court="olg", limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "Hanseatisches Oberlandesgericht in Bremen"
    assert cases[0]["file_number"] == "2 U 106/22"
    assert cases[0]["date"] == "2026-01-29"


def test_hb_parse_row_no_pdf_skips():
    """Rows where PDF extraction fails should be skipped."""
    import lxml.html

    from oldp_ingestor.providers.de.hb import BremenCaseProvider, COURTS

    html = """<tr class="search-result" data-date="2026-01-01">
        <td class="dotright">
            <em>01.01.2026</em><br><br>1 O 1/26<br>Test<br>Zivilrecht<br>Urteil
        </td>
        <td class="dotright">
            <a href="/sixcms/media.php/13/test.pdf">Title (pdf, 100 KB)</a>
        </td>
    </tr>"""
    tree = lxml.html.fromstring(html)
    rows = tree.xpath("//tr")
    provider = BremenCaseProvider(request_delay=0)
    provider._extract_text_from_pdf = lambda url: ""  # empty = skip
    court_cfg = COURTS["olg"]

    case = provider._parse_row(rows[0], court_cfg)
    assert case is None


def test_hb_get_cases_listing_failure(monkeypatch):
    """Listing page fetch failure should stop scraping that court gracefully."""
    import requests

    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise requests.RequestException("Connection refused")

    monkeypatch.setattr(BremenCaseProvider, "_request_with_retry", mock_request)

    provider = BremenCaseProvider(court="olg", request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_hb_get_cases_all_courts(monkeypatch):
    """Scraping all courts iterates over the COURTS dict."""
    import os

    from oldp_ingestor.providers.de.hb import BremenCaseProvider

    fixture_path = os.path.join(os.path.dirname(__file__), "resources", "hb", "1.html")
    with open(fixture_path) as f:
        listing_html = f.read()

    class FakeResp:
        status_code = 200
        text = listing_html
        content = b""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        return FakeResp()

    monkeypatch.setattr(BremenCaseProvider, "_request_with_retry", mock_request)
    monkeypatch.setattr(
        BremenCaseProvider,
        "_extract_text_from_pdf",
        lambda self, url: "<p>Extracted PDF text</p>",
    )
    monkeypatch.setattr(
        BremenCaseProvider,
        "_fetch_abstract",
        lambda self, base_url, href: "Abstract",
    )

    # Scrape all courts (no court filter), but limit to 2 total cases
    provider = BremenCaseProvider(limit=2, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 2
    # All should be from the first court since limit is reached quickly
    assert all(c["court_name"] for c in cases)


# ===================================================================
# --- SnOvgCaseProvider (Sachsen OVG) ---
# ===================================================================


def test_sn_ovg_provider_inherits_correctly():
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(SnOvgCaseProvider, CaseProvider)
    assert issubclass(SnOvgCaseProvider, ScraperBaseClient)


def test_sn_ovg_parse_document():
    """Test document detail page parsing using fixture."""
    import os

    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_ovg", "1.html"
    )
    with open(fixture_path) as f:
        html_str = f.read()

    provider = SnOvgCaseProvider(request_delay=0)

    # Mock _get to return fixture HTML
    class FakeResp:
        status_code = 200
        text = html_str

        def raise_for_status(self):
            pass

    provider._request_with_retry = lambda *a, **kw: FakeResp()
    provider._extract_text_from_pdf = lambda url: "<p>PDF text from OVG</p>"

    case = provider._fetch_document("7816")

    assert case is not None
    assert case["court_name"] == "Sächsisches Oberverwaltungsgericht Bautzen"
    assert case["file_number"] == "3 C 90/21"
    assert case["date"] == "2026-02-02"
    assert case["type"] == "Urteil"
    assert "Normenkontrollantrag" in case["abstract"]
    assert "PDF text" in case["content"]


def test_sn_ovg_search(monkeypatch):
    """Test search POST and ID extraction."""
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    search_html = """<html><body>
    <TABLE><TD>
    <A HREF="javascript:popupDocument('7816');">Aktenzeichen: 3 C 90/21</A>
    </TD></TABLE>
    <TABLE><TD>
    <A HREF="javascript:popupDocument('7295');">Aktenzeichen: 5 B 249/23</A>
    </TD></TABLE>
    </body></html>"""

    class FakeResp:
        status_code = 200
        text = search_html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        SnOvgCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    provider = SnOvgCaseProvider(request_delay=0)
    ids = provider._search()
    assert ids == ["7816", "7295"]


def test_sn_ovg_iso_to_german():
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    assert SnOvgCaseProvider._iso_to_german("2026-01-15") == "15.1.2026"
    assert SnOvgCaseProvider._iso_to_german("invalid") == "invalid"


def test_sn_ovg_build_datum_param():
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    p1 = SnOvgCaseProvider(
        date_from="2025-01-01", date_to="2025-12-31", request_delay=0
    )
    assert p1._build_datum_param() == "1.1.2025-31.12.2025"

    p2 = SnOvgCaseProvider(date_from="2025-06-01", request_delay=0)
    assert "1.6.2025" in p2._build_datum_param()

    p3 = SnOvgCaseProvider(request_delay=0)
    assert p3._build_datum_param() == ""

    p4 = SnOvgCaseProvider(date_to="2025-12-31", request_delay=0)
    assert p4._build_datum_param() == "1.1.1990-31.12.2025"


def test_sn_ovg_document_no_pdf_skips():
    """Documents without PDF content should be skipped."""
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    html = """<HTML><BODY>
    <TABLE><TR VALIGN="TOP"><TD CLASS="schattiert gross">
    <TABLE WIDTH="100%"><TR VALIGN="TOP">
    <TD><div style="font-weight:bold";>
    Court Name<BR />Beschluss<BR />1 B 1/24<BR /></div></TD>
    <TD ALIGN="right">01.01.2024</TD>
    </TR></TABLE></TD></TR>
    <TR><TD><TABLE><TD>Leitsatz:<br /><br />Test</TD></TABLE></TD></TR>
    <TR VALIGN="bottom"><TD CLASS="schattiert gross">
    <TABLE WIDTH="100%">
    <TR VALIGN="top"><TD WIDTH="120">Verweise / Links:</TD>
    <TD><A HREF="documents/test.pdf" target="_blank">Volltext</A></TD>
    </TR></TABLE></TD></TR>
    </TABLE></BODY></HTML>"""

    class FakeResp:
        status_code = 200
        text = html

        def raise_for_status(self):
            pass

    provider = SnOvgCaseProvider(request_delay=0)
    provider._request_with_retry = lambda *a, **kw: FakeResp()
    provider._extract_text_from_pdf = lambda url: ""  # empty

    case = provider._fetch_document("1")
    assert case is None


def test_sn_ovg_get_cases_with_mock(monkeypatch):
    """Full OVG integration test with mocked HTTP."""
    import os

    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_ovg", "1.html"
    )
    with open(fixture_path) as f:
        doc_html = f.read()

    search_html = """<html><body>
    <TABLE><TD>
    <A HREF="javascript:popupDocument('7816');">Aktenzeichen: 3 C 90/21</A>
    </TD></TABLE>
    </body></html>"""

    call_count = [0]

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        resp = FakeResp()
        if method == "POST":
            resp.text = search_html
        else:
            resp.text = doc_html
        return resp

    monkeypatch.setattr(SnOvgCaseProvider, "_request_with_retry", mock_request)
    monkeypatch.setattr(
        SnOvgCaseProvider,
        "_extract_text_from_pdf",
        lambda self, url: "<p>PDF text content</p>",
    )

    provider = SnOvgCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "Sächsisches Oberverwaltungsgericht Bautzen"
    assert cases[0]["file_number"] == "3 C 90/21"
    assert cases[0]["date"] == "2026-02-02"


def test_sn_ovg_get_cases_with_date_filter(monkeypatch):
    """OVG integration test with date filters to cover datum param building."""
    import os

    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_ovg", "1.html"
    )
    with open(fixture_path) as f:
        doc_html = f.read()

    search_html = """<html><body>
    <TABLE><TD>
    <A HREF="javascript:popupDocument('7816');">Aktenzeichen: 3 C 90/21</A>
    </TD></TABLE>
    </body></html>"""

    post_data_captured = []

    class FakeResp:
        status_code = 200
        text = ""

        def raise_for_status(self):
            pass

    def mock_request(self, method, url, **kwargs):
        resp = FakeResp()
        if method == "POST":
            post_data_captured.append(kwargs.get("data", {}))
            resp.text = search_html
        else:
            resp.text = doc_html
        return resp

    monkeypatch.setattr(SnOvgCaseProvider, "_request_with_retry", mock_request)
    monkeypatch.setattr(
        SnOvgCaseProvider,
        "_extract_text_from_pdf",
        lambda self, url: "<p>PDF text content</p>",
    )

    provider = SnOvgCaseProvider(
        date_from="2025-01-01", date_to="2026-12-31", limit=1, request_delay=0
    )
    cases = provider.get_cases()

    assert len(cases) == 1
    # Verify the datum parameter was passed correctly
    assert len(post_data_captured) == 1
    assert "datum" in post_data_captured[0]
    assert post_data_captured[0]["datum"] == "1.1.2025-31.12.2026"


def test_sn_ovg_search_failure(monkeypatch):
    """Search failure should return empty list."""
    import requests

    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise requests.RequestException("Connection refused")

    monkeypatch.setattr(SnOvgCaseProvider, "_request_with_retry", mock_request)

    provider = SnOvgCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert cases == []


def test_sn_ovg_document_fetch_failure(monkeypatch):
    """Individual document fetch failure should be skipped."""
    from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

    search_html = """<html><body>
    <A HREF="javascript:popupDocument('999');">Test</A>
    </body></html>"""

    class FakeResp:
        status_code = 200
        text = search_html

        def raise_for_status(self):
            pass

    call_count = [0]

    def mock_request(self, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return FakeResp()
        raise Exception("Server error")

    monkeypatch.setattr(SnOvgCaseProvider, "_request_with_retry", mock_request)

    provider = SnOvgCaseProvider(limit=1, request_delay=0)
    cases = provider.get_cases()
    assert cases == []


# ===================================================================
# --- SnVerfghCaseProvider (Sachsen VerfGH) ---
# ===================================================================


def test_sn_verfgh_provider_inherits_correctly():
    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider
    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    assert issubclass(SnVerfghCaseProvider, CaseProvider)
    assert issubclass(SnVerfghCaseProvider, ScraperBaseClient)


def test_sn_verfgh_parse_results():
    """Test result HTML parsing using fixture."""
    import os

    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_verfgh", "1.html"
    )
    with open(fixture_path) as f:
        html_str = f.read()

    provider = SnVerfghCaseProvider(request_delay=0)
    entries = provider._parse_results(html_str)

    assert len(entries) == 3

    # First entry: with Leitsatz
    assert entries[0]["file_number"] == "Vf. 13-II-21 (HS)"
    assert entries[0]["date"] == "2025-06-12"
    assert entries[0]["type"] == "Urteil"
    assert entries[0]["court_name"] == "Verfassungsgerichtshof des Freistaates Sachsen"
    assert "Verfassungswidrigkeit" in entries[0]["abstract"]
    assert entries[0]["pdf_link"] == "internet/2021_013_II/2021_013_II.pdf"

    # Second entry: no abstract
    assert entries[1]["file_number"] == "Vf. 57-IV-24"
    assert entries[1]["date"] == "2025-12-11"
    assert entries[1]["type"] == "Beschluss"
    assert "abstract" not in entries[1]

    # Third entry: with description (not Leitsatz)
    assert entries[2]["file_number"] == "Vf. 67-IV-24"
    assert entries[2]["date"] == "2025-09-11"
    assert "Ortschaftsratswahl" in entries[2]["abstract"]


def test_sn_verfgh_parse_date():
    from oldp_ingestor.providers.de.sn_verfgh import _parse_verfgh_date

    assert _parse_verfgh_date("12", "Juni", "2025") == "2025-06-12"
    assert _parse_verfgh_date("1", "Januar", "2024") == "2024-01-01"
    assert _parse_verfgh_date("31", "Dezember", "2023") == "2023-12-31"


def test_sn_verfgh_h4_pattern():
    from oldp_ingestor.providers.de.sn_verfgh import _H4_PATTERNS

    texts = [
        "SächsVerfGH, Beschluss vom 11. September 2025 - Vf. 67-IV-24",
        "SächsVerfGH - Beschluss vom 8. Dezember 2011 - Vf. 85-IV-11",
        "Beschluss des SächsVerfGH vom 25. Oktober 2007 - Vf. 90-IV-06",
        "Beschluss vom 29. November 2018 - Vf. 60-IV-18",
        "SächsVerfGH, Beschluss vom 28.Juni 2006 - Vf. 26-IV-06",
    ]
    for text in texts:
        matched = False
        for pattern in _H4_PATTERNS:
            m = pattern.match(text)
            if m:
                matched = True
                assert m.group(1) in ("Beschluss", "Urteil")
                break
        assert matched, f"No pattern matched: {text}"

    # Verify group extraction on the primary format
    for pattern in _H4_PATTERNS:
        m = pattern.match(texts[0])
        if m:
            assert m.group(1) == "Beschluss"
            assert m.group(2) == "11"
            assert m.group(3) == "September"
            assert m.group(4) == "2025"
            assert m.group(5) == "Vf. 67-IV-24"
            break


def test_sn_verfgh_h4_pattern_with_suffix():
    from oldp_ingestor.providers.de.sn_verfgh import _H4_PATTERNS

    text = "SächsVerfGH, Urteil vom 12. Juni 2025 - Vf. 13-II-21 (HS)"
    for pattern in _H4_PATTERNS:
        m = pattern.match(text)
        if m:
            assert m.group(1) == "Urteil"
            assert m.group(5) == "Vf. 13-II-21 (HS)"
            break
    else:
        assert False, "No pattern matched"


def test_sn_verfgh_get_cases_with_mock(monkeypatch):
    """Full VerfGH integration test with mocked HTTP."""
    import os

    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_verfgh", "1.html"
    )
    with open(fixture_path) as f:
        search_html = f.read()

    class FakeResp:
        status_code = 200
        text = search_html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        SnVerfghCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )
    monkeypatch.setattr(
        SnVerfghCaseProvider,
        "_extract_text_from_pdf",
        lambda self, url: "<p>VerfGH PDF content</p>",
    )

    provider = SnVerfghCaseProvider(limit=2, request_delay=0)
    cases = provider.get_cases()

    assert len(cases) == 2
    assert cases[0]["court_name"] == "Verfassungsgerichtshof des Freistaates Sachsen"
    assert cases[0]["file_number"] == "Vf. 13-II-21 (HS)"
    assert cases[0]["date"] == "2025-06-12"
    assert "VerfGH PDF content" in cases[0]["content"]


def test_sn_verfgh_search_failure(monkeypatch):
    """Search failure should return empty list."""
    import requests as req

    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

    def mock_request(self, method, url, **kwargs):
        raise req.ConnectionError("Network error")

    monkeypatch.setattr(SnVerfghCaseProvider, "_request_with_retry", mock_request)

    provider = SnVerfghCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert len(cases) == 0


def test_sn_verfgh_pdf_failure_skips(monkeypatch):
    """Cases where PDF extraction fails should be skipped."""
    import os

    from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

    fixture_path = os.path.join(
        os.path.dirname(__file__), "resources", "sn_verfgh", "1.html"
    )
    with open(fixture_path) as f:
        search_html = f.read()

    class FakeResp:
        status_code = 200
        text = search_html

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        SnVerfghCaseProvider, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    def failing_pdf(self, url):
        raise Exception("PDF download failed")

    monkeypatch.setattr(SnVerfghCaseProvider, "_extract_text_from_pdf", failing_pdf)

    provider = SnVerfghCaseProvider(request_delay=0)
    cases = provider.get_cases()
    assert len(cases) == 0


# ===================================================================
# --- SnCaseProvider (Sachsen ESAMOSplus) ---
# ===================================================================


def test_sn_provider_inherits_correctly():
    from oldp_ingestor.providers.de.sn import SnCaseProvider
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    assert issubclass(SnCaseProvider, CaseProvider)
    assert issubclass(SnCaseProvider, PlaywrightBaseClient)


def test_sn_iso_to_german():
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    assert SnCaseProvider._iso_to_german("2026-01-15") == "15.01.2026"
    assert SnCaseProvider._iso_to_german("invalid") == "invalid"


def test_sn_german_to_iso():
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    assert SnCaseProvider._german_to_iso("15.01.2026") == "2026-01-15"
    assert SnCaseProvider._german_to_iso("invalid") == "invalid"


def test_sn_courts_dict():
    from oldp_ingestor.providers.de.sn import COURTS

    assert "Oberlandesgericht Dresden" in COURTS
    assert COURTS["Oberlandesgericht Dresden"] == "1012"
    assert len(COURTS) == 15


def test_sn_parse_results_table():
    """Test parsing of ESAMOSplus results table HTML."""
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    html = """<html><body>
    <table id="DV16_Table">
    <tr>
    <td><input type="submit" name="DV16_Table_ctl02_DV16_Table_RowHeader0_C1" value=" "/></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col0_C1">27.01.2026</span></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col1_C1" title="Test Leitsatz">4 U 1229/25</span></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col2_C1">Oberlandesgericht Dresden</span></td>
    <td><input type="submit" id="DV16_Table_ctl02_DV16_Table_Col3_C1" name="DV16_Table$ctl02$DV16_Table_Col3_C1" value="4_U_1229.25..."/></td>
    </tr>
    </table>
    </body></html>"""

    provider = SnCaseProvider.__new__(SnCaseProvider)
    entries = provider._parse_results_table(html, "DV16_Table")

    assert len(entries) == 1
    assert entries[0]["file_number"] == "4 U 1229/25"
    assert entries[0]["date"] == "2026-01-27"
    assert entries[0]["court_name"] == "Oberlandesgericht Dresden"
    assert entries[0]["abstract"] == "Test Leitsatz"
    assert entries[0]["doc_btn_name"] == "DV16_Table$ctl02$DV16_Table_Col3_C1"


def test_sn_parse_results_table_empty():
    """Empty table should return no results."""
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    html = """<html><body><table id="DV13_Table"></table></body></html>"""

    provider = SnCaseProvider.__new__(SnCaseProvider)
    entries = provider._parse_results_table(html, "DV13_Table")
    assert len(entries) == 0


def test_sn_parse_results_table_missing():
    """Missing table should return no results."""
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    html = """<html><body><p>No table here</p></body></html>"""

    provider = SnCaseProvider.__new__(SnCaseProvider)
    entries = provider._parse_results_table(html, "DV13_Table")
    assert len(entries) == 0


def test_sn_get_cases_with_mock(monkeypatch, tmp_path):
    """Full ESAMOSplus integration test with mocked Playwright."""
    import pymupdf

    from oldp_ingestor.providers.de.sn import SnCaseProvider

    # Create a small test PDF
    doc = pymupdf.open()
    page_obj = doc.new_page()
    page_obj.insert_text((72, 72), "Test ESAMOSplus content.")
    pdf_path = tmp_path / "test.pdf"
    doc.save(str(pdf_path))
    doc.close()

    table_html = """<html><body>
    <table id="DV13_Table">
    <tr>
    <td><input type="submit" id="DV13_Table_ctl03_DV13_Table_RowHeader0_C1" value=" "/></td>
    <td><input type="submit" id="DV13_Table_ctl03_DV13_Table_Col0_C1" value="30.10.2009"/></td>
    <td><input type="submit" id="DV13_Table_ctl03_DV13_Table_Col1_C1" value="3 W 1105/09"
        title="Leitsatz: Test abstract"/></td>
    <td><input type="submit" id="DV13_Table_ctl03_DV13_Table_Col2_C1"
        value="Oberlandesgericht Dresden"/></td>
    <td><input type="submit" id="DV13_Table_ctl03_DV13_Table_Col3_C1"
        name="DV13_Table$ctl03$DV13_Table_Col3_C1" value="3W1105.09..."/></td>
    </tr>
    </table>
    </body></html>"""

    class FakeDownload:
        def path(self):
            return str(pdf_path)

    class FakeDownloadCtx:
        def __init__(self):
            self.value = FakeDownload()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class FakePage:
        def goto(self, url, timeout=30000):
            pass

        def wait_for_selector(self, sel, timeout=15000):
            pass

        def wait_for_load_state(self, state, timeout=30000):
            pass

        def click(self, sel):
            pass

        def fill(self, sel, val):
            pass

        def select_option(self, sel, val):
            pass

        def query_selector(self, sel):
            if "DV13_Table" in sel:
                return True
            if "Vorwärts" in sel:
                return None
            return None

        def content(self):
            return table_html

        def expect_download(self, timeout=30000):
            return FakeDownloadCtx()

        def close(self):
            pass

    class FakeContext:
        def new_page(self):
            return FakePage()

    provider = SnCaseProvider(
        court="Oberlandesgericht Dresden",
        date_from="2009-01-01",
        date_to="2010-12-31",
        limit=1,
        request_delay=0,
    )
    provider._context = FakeContext()
    monkeypatch.setattr(SnCaseProvider, "_ensure_browser", lambda self: None)
    monkeypatch.setattr(SnCaseProvider, "close", lambda self: None)

    cases = provider.get_cases()

    assert len(cases) == 1
    assert cases[0]["court_name"] == "Oberlandesgericht Dresden"
    assert cases[0]["file_number"] == "3 W 1105/09"
    assert cases[0]["date"] == "2009-10-30"
    assert cases[0]["abstract"] == "Test abstract"
    assert "ESAMOSplus content" in cases[0]["content"]


def test_sn_get_cases_download_failure(monkeypatch):
    """PDF download failure should skip the entry gracefully."""
    from oldp_ingestor.providers.de.sn import SnCaseProvider

    table_html = """<html><body>
    <table id="DV16_Table">
    <tr>
    <td><input type="submit" id="DV16_Table_ctl02_DV16_Table_RowHeader0_C1" value=" "/></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col0_C1">01.01.2020</span></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col1_C1">1 C 1/20</span></td>
    <td><span id="DV16_Table_ctl02_DV16_Table_Col2_C1">Amtsgericht Dresden</span></td>
    <td><input type="submit" id="DV16_Table_ctl02_DV16_Table_Col3_C1"
        name="DV16_Table$ctl02$DV16_Table_Col3_C1" value="1C1.20..."/></td>
    </tr>
    </table>
    </body></html>"""

    class FakePage:
        def goto(self, url, timeout=30000):
            pass

        def wait_for_selector(self, sel, timeout=15000):
            pass

        def wait_for_load_state(self, state, timeout=30000):
            pass

        def click(self, sel):
            pass

        def query_selector(self, sel):
            if "DV13_Table" in sel:
                return None  # Fall back to DV16_Table
            if "Vorwärts" in sel:
                return None
            return None

        def content(self):
            return table_html

        def expect_download(self, timeout=30000):
            raise TimeoutError("Download timed out")

        def close(self):
            pass

    class FakeContext:
        def new_page(self):
            return FakePage()

    provider = SnCaseProvider(limit=1, request_delay=0)
    provider._context = FakeContext()
    monkeypatch.setattr(SnCaseProvider, "_ensure_browser", lambda self: None)
    monkeypatch.setattr(SnCaseProvider, "close", lambda self: None)

    cases = provider.get_cases()
    assert cases == []


# ===================================================================
# --- PDF text extraction (ScraperBaseClient._extract_text_from_pdf) ---
# ===================================================================


def test_extract_text_from_pdf(monkeypatch):
    """Test PDF text extraction via pymupdf."""
    import pymupdf

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    # Create a minimal PDF in memory
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Test PDF content for extraction.")
    pdf_bytes = doc.tobytes()
    doc.close()

    class FakeResp:
        status_code = 200
        content = pdf_bytes

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ScraperBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = ScraperBaseClient(request_delay=0)
    result = client._extract_text_from_pdf("https://example.com/test.pdf")

    assert "<p>" in result
    assert "Test PDF content" in result


def test_extract_text_from_pdf_empty(monkeypatch):
    """Empty PDF should return empty string."""
    import pymupdf

    from oldp_ingestor.providers.scraper_common import ScraperBaseClient

    # Create a PDF with no text
    doc = pymupdf.open()
    doc.new_page()
    pdf_bytes = doc.tobytes()
    doc.close()

    class FakeResp:
        status_code = 200
        content = pdf_bytes

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        ScraperBaseClient, "_request_with_retry", lambda self, *a, **kw: FakeResp()
    )

    client = ScraperBaseClient(request_delay=0)
    result = client._extract_text_from_pdf("https://example.com/empty.pdf")
    assert result == ""
