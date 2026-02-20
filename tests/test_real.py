"""Real integration tests that make actual network requests.

These tests are skipped by default. Run them with:

    make test-real
    # or
    pytest --run-real -m real -v

Each test fetches a minimal amount of data (limit=1 or 2) to verify
the full pipeline works end-to-end against live servers.
"""

import pytest

real = pytest.mark.real


# ===================================================================
# RIS API — Laws
# ===================================================================


@real
class TestRISProviderReal:
    """Real tests for RISProvider (legislation from RIS API)."""

    def test_get_law_books(self):
        """Fetch at least 1 law book from the RIS API."""
        from oldp_ingestor.providers.de.ris import RISProvider

        provider = RISProvider(limit=1, request_delay=0.5)
        books = provider.get_law_books()

        assert len(books) >= 1
        book = books[0]
        assert book["code"], "code must not be empty"
        assert book["title"], "title must not be empty"
        assert book["revision_date"], "revision_date must not be empty"

    def test_get_law_books_with_search_term(self):
        """Fetch law books filtered by search term."""
        from oldp_ingestor.providers.de.ris import RISProvider

        provider = RISProvider(search_term="BGB", limit=1, request_delay=0.5)
        books = provider.get_law_books()

        assert len(books) >= 1

    def test_get_law_books_with_date_filter(self):
        """Fetch law books filtered by date range."""
        from oldp_ingestor.providers.de.ris import RISProvider

        provider = RISProvider(
            date_from="2024-01-01", date_to="2024-12-31", limit=1, request_delay=0.5
        )
        books = provider.get_law_books()
        # May or may not return results depending on API state
        assert isinstance(books, list)

    def test_get_laws_for_book(self):
        """Fetch laws for a specific law book."""
        from oldp_ingestor.providers.de.ris import RISProvider

        provider = RISProvider(limit=1, request_delay=0.5)
        books = provider.get_law_books()
        assert len(books) >= 1

        book = books[0]
        laws = provider.get_laws(book["code"], book["revision_date"])

        assert isinstance(laws, list)
        if laws:
            law = laws[0]
            assert "section" in law
            assert "book_code" in law
            assert law["book_code"] == book["code"]


# ===================================================================
# RIS API — Cases
# ===================================================================


@real
class TestRISCaseProviderReal:
    """Real tests for RISCaseProvider (case law from RIS API)."""

    def test_get_cases(self):
        """Fetch at least 1 case from the RIS API."""
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        provider = RISCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_get_cases_with_court_filter(self):
        """Fetch cases filtered by court type."""
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        provider = RISCaseProvider(court="BGH", limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        # Court name should be resolved to full name
        assert (
            "BGH" in cases[0]["court_name"]
            or "Bundesgerichtshof" in cases[0]["court_name"]
        )

    def test_get_cases_with_date_filter(self):
        """Fetch cases filtered by decision date range."""
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        provider = RISCaseProvider(
            date_from="2024-01-01", date_to="2024-12-31", limit=1, request_delay=0.5
        )
        cases = provider.get_cases()
        assert isinstance(cases, list)

    def test_fetch_court_labels(self):
        """Fetch court labels from the RIS API."""
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        provider = RISCaseProvider(request_delay=0.5)
        labels = provider._fetch_court_labels()

        assert isinstance(labels, dict)
        assert len(labels) > 0
        # Should have at least BGH
        assert "BGH" in labels
        assert labels["BGH"]  # not empty

    def test_case_has_optional_fields(self):
        """Fetch a case and check that optional fields are populated."""
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        provider = RISCaseProvider(limit=2, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        # At least one case should have some optional fields
        has_type = any("type" in c for c in cases)
        has_file_number = any(c.get("file_number") for c in cases)
        assert has_type or has_file_number, "Expected at least some optional fields"


# ===================================================================
# RII — Federal courts (rechtsprechung-im-internet.de)
# ===================================================================


@real
class TestRiiCaseProviderReal:
    """Real tests for RiiCaseProvider (federal courts, ZIP/XML)."""

    def test_get_cases_single_court(self):
        """Fetch at least 1 case from BVerfG."""
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        provider = RiiCaseProvider(court="bverfg", limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_get_cases_bgh(self):
        """Fetch at least 1 case from BGH."""
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        provider = RiiCaseProvider(court="bgh", limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        assert cases[0]["court_name"]

    def test_get_ids_from_page(self):
        """Fetch document IDs from a real search results page."""
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        provider = RiiCaseProvider(request_delay=0.5)
        url = provider._get_page_url(1, "bverfg")
        ids = provider._get_ids_from_page(url)

        assert isinstance(ids, list)
        assert len(ids) > 0, "Expected at least some document IDs"

    def test_zip_download_and_parse(self):
        """Download a real ZIP file and parse the XML."""
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        provider = RiiCaseProvider(request_delay=0.5)

        # Get a real doc ID first
        url = provider._get_page_url(1, "bverfg")
        ids = provider._get_ids_from_page(url)
        assert len(ids) > 0

        doc_id = ids[0]
        zip_url = provider._get_zip_url(doc_id)
        xml_str = provider._get_xml_from_zip(zip_url)

        assert xml_str is not None
        assert "<?xml" in xml_str or "<dokument" in xml_str

    def test_case_has_ecli(self):
        """Cases from federal courts should typically have ECLI."""
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        provider = RiiCaseProvider(court="bverfg", limit=2, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        # At least one should have ECLI
        has_ecli = any("ecli" in c for c in cases)
        assert has_ecli, "Expected at least one case with ECLI"


# ===================================================================
# BY — Bavaria (gesetze-bayern.de)
# ===================================================================


@real
class TestByCaseProviderReal:
    """Real tests for ByCaseProvider (Bavaria, ZIP/XML)."""

    def test_get_cases(self):
        """Fetch at least 1 case from gesetze-bayern.de."""
        from oldp_ingestor.providers.de.by import ByCaseProvider

        provider = ByCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_get_ids_from_page(self):
        """Fetch document IDs from a real search page."""
        from oldp_ingestor.providers.de.by import ByCaseProvider

        provider = ByCaseProvider(request_delay=0.5)
        provider._init_search_session()
        ids = provider._get_ids_from_page(1)

        assert isinstance(ids, list)
        assert len(ids) > 0, "Expected at least some document IDs"

    def test_case_type_expanded(self):
        """Case types should be expanded (e.g. Beschluss, not Bes)."""
        from oldp_ingestor.providers.de.by import ByCaseProvider

        provider = ByCaseProvider(limit=2, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        for case in cases:
            if "type" in case:
                # Should not be abbreviated
                assert case["type"] not in ("Bes", "Urt", "Ent"), (
                    f"Case type not expanded: {case['type']}"
                )

    def test_case_date_format(self):
        """Dates should be in YYYY-MM-DD format."""
        import re

        from oldp_ingestor.providers.de.by import ByCaseProvider

        provider = ByCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        date = cases[0]["date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Bad date format: {date}"


# ===================================================================
# NRW — North Rhine-Westphalia (nrwesuche.justiz.nrw.de)
# ===================================================================


@real
class TestNrwCaseProviderReal:
    """Real tests for NrwCaseProvider (NRW, POST-based HTML)."""

    def test_get_cases(self):
        """Fetch at least 1 case from NRW."""
        from oldp_ingestor.providers.de.nrw import NrwCaseProvider

        provider = NrwCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_search_page(self):
        """Search page should return case links."""
        from oldp_ingestor.providers.de.nrw import NrwCaseProvider

        provider = NrwCaseProvider(request_delay=0.5)
        links = provider._search_page(1)

        assert isinstance(links, list)
        assert len(links) > 0, "Expected at least some case links"

    def test_case_has_ecli(self):
        """NRW cases should typically have ECLI."""
        from oldp_ingestor.providers.de.nrw import NrwCaseProvider

        provider = NrwCaseProvider(limit=2, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        has_ecli = any("ecli" in c for c in cases)
        assert has_ecli, "Expected at least one case with ECLI"

    def test_case_date_format(self):
        """Dates should be in YYYY-MM-DD format."""
        import re

        from oldp_ingestor.providers.de.nrw import NrwCaseProvider

        provider = NrwCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        date = cases[0]["date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Bad date format: {date}"


# ===================================================================
# Juris eJustiz portals (Playwright-based)
# ===================================================================


@real
class TestJurisCaseProviderReal:
    """Real tests for JurisCaseProvider (eJustiz portals via Playwright).

    Tests a selection of state portals to verify the Playwright-based
    scraping pipeline works end-to-end.
    """

    def test_bb_get_cases(self):
        """Fetch at least 1 case from Berlin-Brandenburg portal."""
        from oldp_ingestor.providers.de.juris import BbBeCaseProvider

        provider = BbBeCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_hh_get_cases(self):
        """Fetch at least 1 case from Hamburg portal."""
        from oldp_ingestor.providers.de.juris import HhCaseProvider

        provider = HhCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_bw_get_cases(self):
        """Fetch at least 1 case from Baden-Württemberg portal."""
        from oldp_ingestor.providers.de.juris import BwCaseProvider

        provider = BwCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_sh_get_cases(self):
        """Fetch at least 1 case from Schleswig-Holstein portal."""
        from oldp_ingestor.providers.de.juris import ShCaseProvider

        provider = ShCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_rlp_get_cases(self):
        """Fetch at least 1 case from Rhineland-Palatinate portal."""
        from oldp_ingestor.providers.de.juris import RlpCaseProvider

        provider = RlpCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_mv_get_cases(self):
        """Fetch at least 1 case from Mecklenburg-Vorpommern portal."""
        from oldp_ingestor.providers.de.juris import MvCaseProvider

        provider = MvCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_sa_get_cases(self):
        """Fetch at least 1 case from Saxony-Anhalt portal."""
        from oldp_ingestor.providers.de.juris import SaCaseProvider

        provider = SaCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_sl_get_cases(self):
        """Fetch at least 1 case from Saarland portal."""
        from oldp_ingestor.providers.de.juris import SlCaseProvider

        provider = SlCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_he_get_cases(self):
        """Fetch at least 1 case from Hessen portal."""
        from oldp_ingestor.providers.de.juris import HeCaseProvider

        provider = HeCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_th_get_cases(self):
        """Fetch at least 1 case from Thueringen portal."""
        from oldp_ingestor.providers.de.juris import ThCaseProvider

        provider = ThCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["content"], "content must not be empty"

    def test_bb_get_case_ids_from_page(self):
        """Fetch case IDs from a real Berlin-Brandenburg search page."""
        from oldp_ingestor.providers.de.juris import BbBeCaseProvider

        provider = BbBeCaseProvider(request_delay=1.0)
        try:
            url = provider._search_url(1)
            ids = provider._get_case_ids_from_page(url)

            assert isinstance(ids, list)
            assert len(ids) > 0, "Expected at least some document IDs"
        finally:
            provider.close()

    def test_case_date_format(self):
        """Dates from Juris portals should be in YYYY-MM-DD format."""
        import re

        from oldp_ingestor.providers.de.juris import BbBeCaseProvider

        provider = BbBeCaseProvider(limit=1, request_delay=1.0)
        cases = provider.get_cases()

        assert len(cases) >= 1
        date = cases[0]["date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Bad date format: {date}"


# ===================================================================
# PlaywrightBaseClient — direct browser tests
# ===================================================================


@real
class TestPlaywrightBaseClientReal:
    """Real tests for PlaywrightBaseClient (browser lifecycle)."""

    def test_fetch_real_webpage(self):
        """Fetch a real webpage and verify HTML content."""
        from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

        client = PlaywrightBaseClient(request_delay=0, headless=True)
        try:
            html = client._get_page_html("https://example.com")
            assert "<html" in html.lower()
            assert "Example Domain" in html
        finally:
            client.close()

    def test_fetch_real_webpage_as_tree(self):
        """Fetch a real webpage and parse to lxml tree."""
        from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

        client = PlaywrightBaseClient(request_delay=0, headless=True)
        try:
            tree = client._get_page_tree("https://example.com")
            titles = tree.xpath("//title/text()")
            assert len(titles) == 1
            assert "Example Domain" in titles[0]
        finally:
            client.close()


# ===================================================================
# HttpBaseClient / ScraperBaseClient — real HTTP
# ===================================================================


@real
class TestHttpBaseClientReal:
    """Real tests for HttpBaseClient (basic HTTP operations)."""

    def test_get_real_url(self):
        """Fetch a real URL via HttpBaseClient."""
        from oldp_ingestor.providers.http_client import HttpBaseClient

        client = HttpBaseClient(base_url="https://example.com", request_delay=0)
        resp = client._get("/")
        assert resp.status_code == 200
        assert "Example Domain" in resp.text

    def test_get_json_real(self):
        """Fetch real JSON from httpbin."""
        from oldp_ingestor.providers.http_client import HttpBaseClient

        client = HttpBaseClient(base_url="https://httpbin.org", request_delay=0)
        data = client._get_json("/get")
        assert "headers" in data


@real
class TestScraperBaseClientReal:
    """Real tests for ScraperBaseClient (HTML/XML scraping)."""

    def test_get_html_tree_real(self):
        """Fetch and parse a real HTML page."""
        from oldp_ingestor.providers.scraper_common import ScraperBaseClient

        client = ScraperBaseClient(base_url="https://example.com", request_delay=0)
        tree = client._get_html_tree("/")
        titles = tree.xpath("//title/text()")
        assert "Example Domain" in titles[0]


# ===================================================================
# NS — Lower Saxony (voris.wolterskluwer-online.de)
# ===================================================================


@real
class TestNsCaseProviderReal:
    """Real tests for NsCaseProvider (Niedersachsen, WK Drupal site)."""

    def test_fetch_one_case(self):
        """Fetch at least 1 case from VORIS."""
        from oldp_ingestor.providers.de.ns import NsCaseProvider

        provider = NsCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"], "court_name must not be empty"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10

    def test_search_page(self):
        """Search page should return document UUIDs."""
        from oldp_ingestor.providers.de.ns import NsCaseProvider

        provider = NsCaseProvider(request_delay=0.5)
        links = provider._search_page(0)

        assert isinstance(links, list)
        assert len(links) > 0, "Expected at least some document links"
        # All links should be UUID paths
        import re

        for link in links:
            assert re.match(r"/browse/document/[a-f0-9-]{36}$", link), (
                f"Unexpected link format: {link}"
            )

    def test_fetch_multiple_cases(self):
        """Fetch 2 cases and verify they are distinct."""
        from oldp_ingestor.providers.de.ns import NsCaseProvider

        provider = NsCaseProvider(limit=2, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 2
        assert cases[0]["file_number"] != cases[1]["file_number"]

    def test_case_content_length(self):
        """Verify case content is substantial."""
        from oldp_ingestor.providers.de.ns import NsCaseProvider

        provider = NsCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        assert len(cases[0]["content"]) >= 100, "Content seems too short"

    def test_case_date_format(self):
        """Dates should be in YYYY-MM-DD format."""
        import re

        from oldp_ingestor.providers.de.ns import NsCaseProvider

        provider = NsCaseProvider(limit=1, request_delay=0.5)
        cases = provider.get_cases()

        assert len(cases) >= 1
        date = cases[0]["date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Bad date format: {date}"


# ===================================================================
# EU — EUR-Lex (European Court of Justice)
# ===================================================================


@real
class TestEuCaseProviderReal:
    """Real tests for EuCaseProvider (EUR-Lex SOAP + HTML/XML)."""

    def test_get_cases(self):
        """Fetch at least 1 case from EUR-Lex."""
        from oldp_ingestor import settings
        from oldp_ingestor.providers.de.eu import EuCaseProvider

        provider = EuCaseProvider(
            username=settings.EURLEX_USER,
            password=settings.EURLEX_PASSWORD,
            limit=3,
            request_delay=0.5,
        )
        cases = provider.get_cases()

        assert len(cases) >= 1
        case = cases[0]
        assert case["court_name"] == "Europäischer Gerichtshof"
        assert case["file_number"], "file_number must not be empty"
        assert case["date"], "date must not be empty"
        assert case["content"], "content must not be empty"
        assert len(case["content"]) >= 10
        assert case["ecli"], "ecli must not be empty"

    def test_case_date_format(self):
        """Dates should be in YYYY-MM-DD format."""
        import re

        from oldp_ingestor import settings
        from oldp_ingestor.providers.de.eu import EuCaseProvider

        provider = EuCaseProvider(
            username=settings.EURLEX_USER,
            password=settings.EURLEX_PASSWORD,
            limit=1,
            request_delay=0.5,
        )
        cases = provider.get_cases()

        assert len(cases) >= 1
        date = cases[0]["date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Bad date format: {date}"
