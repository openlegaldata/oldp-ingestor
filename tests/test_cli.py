import json
import os
import subprocess
import sys
import tempfile

import pytest


def test_cli_help():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "laws" in result.stdout
    assert "info" in result.stdout


def test_cli_laws_help():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "laws", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--provider" in result.stdout
    assert "--path" in result.stdout
    assert "--search-term" in result.stdout
    assert "--limit" in result.stdout


def test_cli_laws_requires_provider():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "laws"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "--provider" in result.stderr


def test_cli_laws_dummy_requires_path(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    monkeypatch.setenv("OLDP_API_TOKEN", "test-token")

    from oldp_ingestor.cli import _make_law_provider

    class FakeArgs:
        provider = "dummy"
        path = None

    import pytest

    with pytest.raises(SystemExit):
        _make_law_provider(FakeArgs())


# --- Cases subcommand ---


def test_cli_help_includes_cases():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "cases" in result.stdout


def test_cli_cases_help():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "cases", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--provider" in result.stdout
    assert "--path" in result.stdout
    assert "--limit" in result.stdout
    assert "ris" in result.stdout
    assert "--court" in result.stdout
    assert "--date-from" in result.stdout
    assert "--date-to" in result.stdout
    assert "--request-delay" in result.stdout


def test_cli_cases_requires_provider():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "cases"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "--provider" in result.stderr


def test_cli_cases_dummy_requires_path(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    monkeypatch.setenv("OLDP_API_TOKEN", "test-token")

    from oldp_ingestor.cli import _make_case_provider

    class FakeArgs:
        provider = "dummy"
        path = None

    with pytest.raises(SystemExit):
        _make_case_provider(FakeArgs())


# --- _make_law_provider / _make_case_provider: ris branch ---


def test_make_law_provider_ris(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_law_provider
    from oldp_ingestor.providers.de.ris import RISProvider

    class FakeArgs:
        provider = "ris"
        search_term = "BGB"
        limit = 5
        date_from = "2025-01-01"
        date_to = "2025-12-31"
        request_delay = 0.5

    provider = _make_law_provider(FakeArgs())
    assert isinstance(provider, RISProvider)
    assert provider.search_term == "BGB"
    assert provider.limit == 5


def test_make_case_provider_ris(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

    class FakeArgs:
        provider = "ris"
        court = "BGH"
        date_from = "2026-01-01"
        date_to = "2026-06-30"
        limit = 10
        request_delay = 0.3

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, RISCaseProvider)
    assert provider.court == "BGH"


def test_make_law_provider_unknown(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_law_provider

    class FakeArgs:
        provider = "unknown_xyz"

    with pytest.raises(SystemExit):
        _make_law_provider(FakeArgs())


def test_make_case_provider_unknown(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider

    class FakeArgs:
        provider = "unknown_xyz"

    with pytest.raises(SystemExit):
        _make_case_provider(FakeArgs())


# --- cmd_laws integration ---


def _make_fixture_file(data):
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(data, f)
    f.close()
    return f.name


LAW_FIXTURES = [
    {
        "model": "laws.lawbook",
        "pk": 1,
        "fields": {
            "code": "TST",
            "title": "Test Book",
            "revision_date": "2024-01-01",
            "order": 0,
            "changelog": "[]",
            "footnotes": "[]",
            "sections": "{}",
        },
    },
    {
        "model": "laws.law",
        "pk": 1,
        "fields": {
            "book": 1,
            "content": "<p>Content</p>",
            "title": "First",
            "section": "§ 1",
            "slug": "s-1",
            "order": 1,
        },
    },
]

CASE_FIXTURES = [
    {
        "model": "courts.court",
        "pk": 1,
        "fields": {"name": "Bundesgerichtshof", "code": "BGH", "slug": "bgh"},
    },
    {
        "model": "cases.case",
        "pk": 1,
        "fields": {
            "court": 1,
            "file_number": "I ZR 1/21",
            "date": "2024-01-15",
            "content": "<p>Content</p>",
        },
    },
]


def test_cmd_laws_dummy_creates_and_skips(monkeypatch):
    """cmd_laws creates books/laws and handles 409 duplicates."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    post_calls = []

    class FakeResponse:
        status_code = 201

        def json(self):
            return {}

        def raise_for_status(self):
            pass

    class FakeResponse409:
        status_code = 409

        def json(self):
            return {}

    class FakeClient:
        def post(self, path, data):
            post_calls.append((path, data))
            if len(post_calls) == 1:
                return FakeResponse().json()  # book created
            # Second call: law — simulate 409
            exc = requests.HTTPError(response=FakeResponse409())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_laws(FakeArgs())
    assert len(post_calls) == 2
    assert post_calls[0][0] == "/api/law_books/"
    assert post_calls[1][0] == "/api/laws/"


def test_cmd_laws_book_409_skips_laws(monkeypatch):
    """When book already exists (409), its laws are skipped entirely."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    post_calls = []

    class FakeResponse409:
        status_code = 409

        def json(self):
            return {}

    class FakeClient:
        def post(self, path, data):
            post_calls.append(path)
            exc = requests.HTTPError(response=FakeResponse409())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_laws(FakeArgs())
    # Only the book POST happens; laws are skipped because book was 409
    assert post_calls == ["/api/law_books/"]


def test_cmd_laws_book_other_error(monkeypatch):
    """Non-409 error on book continues to next book."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    class FakeResponse500:
        status_code = 500

        def json(self):
            return {"detail": "error"}

        @property
        def text(self):
            return "error"

    class FakeClient:
        def post(self, path, data):
            exc = requests.HTTPError(response=FakeResponse500())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_laws(FakeArgs())  # should not raise


def test_cmd_laws_law_other_error(monkeypatch):
    """Non-409 error on law logs error but continues."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    call_count = [0]

    class FakeResponse500:
        status_code = 500

        def json(self):
            return {"detail": "error"}

        @property
        def text(self):
            return "error"

    class FakeClient:
        def post(self, path, data):
            call_count[0] += 1
            if path == "/api/law_books/":
                return {}
            exc = requests.HTTPError(response=FakeResponse500())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_laws(FakeArgs())
    assert call_count[0] == 2  # book + law


def test_cmd_laws_limit(monkeypatch):
    """--limit caps the number of books processed."""
    from oldp_ingestor.cli import cmd_laws

    # Create fixture with 2 books
    fixtures = [
        {
            "model": "laws.lawbook",
            "pk": i,
            "fields": {
                "code": f"B{i}",
                "title": f"Book {i}",
                "revision_date": "2024-01-01",
                "order": i,
                "changelog": "[]",
                "footnotes": "[]",
                "sections": "{}",
            },
        }
        for i in range(1, 3)
    ]
    fixture_path = _make_fixture_file(fixtures)

    post_calls = []

    class FakeClient:
        def post(self, path, data):
            post_calls.append(data)
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = 1
        verbose = False

    cmd_laws(FakeArgs())
    assert len(post_calls) == 1  # only 1 book


def test_cmd_laws_empty_title_fallback(monkeypatch):
    """Law with empty title gets title from section."""
    from oldp_ingestor.cli import cmd_laws

    fixtures = [
        {
            "model": "laws.lawbook",
            "pk": 1,
            "fields": {
                "code": "X",
                "title": "X Book",
                "revision_date": "2024-01-01",
                "order": 0,
                "changelog": "[]",
                "footnotes": "[]",
                "sections": "{}",
            },
        },
        {
            "model": "laws.law",
            "pk": 1,
            "fields": {
                "book": 1,
                "content": "<p>C</p>",
                "title": "",
                "section": "§ 1",
                "slug": "s",
                "order": 1,
            },
        },
    ]
    fixture_path = _make_fixture_file(fixtures)

    law_data = []

    class FakeClient:
        def post(self, path, data):
            if path == "/api/laws/":
                law_data.append(data)
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_laws(FakeArgs())
    assert law_data[0]["title"] == "§ 1"


# --- cmd_cases integration ---


def test_cmd_cases_dummy_creates(monkeypatch):
    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    post_calls = []

    class FakeClient:
        def post(self, path, data):
            post_calls.append((path, data))
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_cases(FakeArgs())
    assert len(post_calls) == 1
    assert post_calls[0][0] == "/api/cases/"
    assert post_calls[0][1]["court_name"] == "Bundesgerichtshof"


def test_cmd_cases_409_skipped(monkeypatch):
    import requests

    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    class FakeResponse409:
        status_code = 409

        def json(self):
            return {}

    class FakeClient:
        def post(self, path, data):
            raise requests.HTTPError(response=FakeResponse409())

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_cases(FakeArgs())  # should not raise


def test_cmd_cases_other_error(monkeypatch):
    import requests

    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    class FakeResponse500:
        status_code = 500

        def json(self):
            raise ValueError("no json")

        @property
        def text(self):
            return "server error"

    class FakeClient:
        def post(self, path, data):
            raise requests.HTTPError(response=FakeResponse500())

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_cases(FakeArgs())  # should not raise, just logs


def test_cmd_cases_limit(monkeypatch):
    from oldp_ingestor.cli import cmd_cases

    fixtures = [
        {
            "model": "courts.court",
            "pk": 1,
            "fields": {"name": "BGH", "code": "BGH", "slug": "bgh"},
        },
    ] + [
        {
            "model": "cases.case",
            "pk": i,
            "fields": {
                "court": 1,
                "file_number": f"ZR {i}/21",
                "date": "2024-01-01",
                "content": "<p>C</p>",
            },
        }
        for i in range(1, 4)
    ]
    fixture_path = _make_fixture_file(fixtures)

    post_calls = []

    class FakeClient:
        def post(self, path, data):
            post_calls.append(data)
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = 2
        verbose = False

    cmd_cases(FakeArgs())
    assert len(post_calls) == 2


def test_cmd_cases_truncates_long_fields(monkeypatch):
    from oldp_ingestor.cli import cmd_cases

    fixtures = [
        {
            "model": "courts.court",
            "pk": 1,
            "fields": {"name": "X" * 300, "code": "X", "slug": "x"},
        },
        {
            "model": "cases.case",
            "pk": 1,
            "fields": {
                "court": 1,
                "file_number": "Y" * 200,
                "date": "2024-01-01",
                "content": "<p>C</p>",
                "title": "Z" * 300,
            },
        },
    ]
    fixture_path = _make_fixture_file(fixtures)

    posted = []

    class FakeClient:
        def post(self, path, data):
            posted.append(data)
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_cases(FakeArgs())
    case = posted[0]
    assert len(case["file_number"]) == 100
    assert len(case["title"]) == 255
    assert len(case["court_name"]) == 255


# --- cmd_info ---


def test_cmd_info(monkeypatch, capsys):
    from oldp_ingestor.cli import cmd_info

    class FakeClient:
        def get(self, path, **kwargs):
            return {"laws": "/api/laws/", "cases": "/api/cases/"}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        pass

    cmd_info(FakeArgs())
    captured = capsys.readouterr()
    assert '"laws"' in captured.out


# --- main() ---


def test_main_no_command(monkeypatch):
    monkeypatch.setattr("sys.argv", ["oldp-ingestor"])
    from oldp_ingestor.cli import main

    with pytest.raises(SystemExit):
        main()


# --- New provider CLI tests ---


def test_cli_cases_help_includes_new_providers():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "cases", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    for provider in [
        "rii",
        "by",
        "nrw",
        "juris-bb",
        "juris-hh",
        "juris-he",
        "juris-th",
    ]:
        assert provider in result.stdout, f"Provider '{provider}' not in help output"


def test_make_case_provider_rii(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.rii import RiiCaseProvider

    class FakeArgs:
        provider = "rii"
        court = "bgh"
        date_from = None
        date_to = None
        limit = 5
        request_delay = 0.1

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, RiiCaseProvider)
    assert provider.court == "bgh"
    assert provider.limit == 5


def test_make_case_provider_by(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.by import ByCaseProvider

    class FakeArgs:
        provider = "by"
        court = None
        date_from = None
        date_to = None
        limit = 10
        request_delay = 0.1

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, ByCaseProvider)


def test_make_case_provider_nrw(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.nrw import NrwCaseProvider

    class FakeArgs:
        provider = "nrw"
        court = None
        date_from = "2024-01-01"
        date_to = "2024-12-31"
        limit = 10
        request_delay = 0.1

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, NrwCaseProvider)


def test_make_case_provider_juris_bb(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.juris import BbBeCaseProvider

    class FakeArgs:
        provider = "juris-bb"
        court = None
        date_from = None
        date_to = None
        limit = 5
        request_delay = 0.5

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, BbBeCaseProvider)


def test_make_case_provider_juris_he(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.juris import HeCaseProvider

    class FakeArgs:
        provider = "juris-he"
        court = None
        date_from = None
        date_to = None
        limit = 5
        request_delay = 0.5

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, HeCaseProvider)


def test_make_case_provider_juris_th(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    from oldp_ingestor.cli import _make_case_provider
    from oldp_ingestor.providers.de.juris import ThCaseProvider

    class FakeArgs:
        provider = "juris-th"
        court = None
        date_from = None
        date_to = None
        limit = 5
        request_delay = 0.5

    provider = _make_case_provider(FakeArgs())
    assert isinstance(provider, ThCaseProvider)


# --- Results dir and error counting ---


def test_cmd_cases_writes_result_file(monkeypatch, tmp_path):
    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    class FakeClient:
        def post(self, path, data):
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = rdir

    exit_code = cmd_cases(FakeArgs())
    assert exit_code == 0

    result_file = os.path.join(rdir, "cases_dummy.json")
    assert os.path.exists(result_file)

    with open(result_file) as f:
        data = json.load(f)
    assert data["provider"] == "dummy"
    assert data["command"] == "cases"
    assert data["status"] == "ok"
    assert data["created"] == 1
    assert data["errors"] == 0


def test_cmd_cases_error_counting(monkeypatch, tmp_path):
    import requests

    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    class FakeResponse500:
        status_code = 500

        def json(self):
            raise ValueError("no json")

        @property
        def text(self):
            return "server error"

    class FakeClient:
        def post(self, path, data):
            raise requests.HTTPError(response=FakeResponse500())

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = rdir

    exit_code = cmd_cases(FakeArgs())
    assert exit_code == 1  # partial failure

    with open(os.path.join(rdir, "cases_dummy.json")) as f:
        data = json.load(f)
    assert data["status"] == "partial"
    assert data["errors"] == 1


def test_cmd_cases_no_results_dir(monkeypatch):
    """When results_dir is empty, no file is written."""
    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    class FakeClient:
        def post(self, path, data):
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = ""

    exit_code = cmd_cases(FakeArgs())
    assert exit_code == 0


def test_cmd_laws_writes_result_file(monkeypatch, tmp_path):
    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    class FakeClient:
        def post(self, path, data):
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = rdir

    exit_code = cmd_laws(FakeArgs())
    assert exit_code == 0

    result_file = os.path.join(rdir, "laws_dummy.json")
    assert os.path.exists(result_file)

    with open(result_file) as f:
        data = json.load(f)
    assert data["provider"] == "dummy"
    assert data["command"] == "laws"
    assert data["status"] == "ok"
    assert data["created"] == 2  # 1 book + 1 law
    assert data["errors"] == 0


def test_cmd_laws_error_counting(monkeypatch, tmp_path):
    """Non-409 errors are counted in results."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    call_count = [0]

    class FakeResponse500:
        status_code = 500

        def json(self):
            return {"detail": "error"}

        @property
        def text(self):
            return "error"

    class FakeClient:
        def post(self, path, data):
            call_count[0] += 1
            if path == "/api/law_books/":
                return {}
            exc = requests.HTTPError(response=FakeResponse500())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = rdir

    exit_code = cmd_laws(FakeArgs())
    assert exit_code == 1  # partial failure

    with open(os.path.join(rdir, "laws_dummy.json")) as f:
        data = json.load(f)
    assert data["status"] == "partial"
    assert data["errors"] == 1


def test_cmd_laws_book_error_counted(monkeypatch, tmp_path):
    """Non-409 error on book is counted as books_errors."""
    import requests

    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)

    class FakeResponse500:
        status_code = 500

        def json(self):
            return {"detail": "error"}

        @property
        def text(self):
            return "error"

    class FakeClient:
        def post(self, path, data):
            exc = requests.HTTPError(response=FakeResponse500())
            raise exc

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    rdir = str(tmp_path / "results")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        results_dir = rdir

    exit_code = cmd_laws(FakeArgs())
    assert exit_code == 1

    with open(os.path.join(rdir, "laws_dummy.json")) as f:
        data = json.load(f)
    assert data["errors"] == 1


# --- Status subcommand ---


def test_cli_status_help():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "status", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--stale-hours" in result.stdout
    assert "--json" in result.stdout


def test_cmd_status_empty_dir(tmp_path):
    from oldp_ingestor.cli import cmd_status

    class FakeArgs:
        results_dir = str(tmp_path)
        stale_hours = 168
        json = False

    exit_code = cmd_status(FakeArgs())
    assert exit_code == 1  # unhealthy — all providers missing


def test_cmd_status_json_output(tmp_path, capsys):
    from datetime import datetime, timezone

    from oldp_ingestor.cli import cmd_status
    from oldp_ingestor.results import write_result

    started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 2, 9, 3, 10, 0, tzinfo=timezone.utc)
    write_result(
        str(tmp_path),
        "cases",
        "rii",
        started,
        finished,
        created=50,
        skipped=10,
        errors=0,
    )

    class FakeArgs:
        results_dir = str(tmp_path)
        stale_hours = 168
        json = True

    cmd_status(FakeArgs())
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["provider"] == "rii"


def test_cmd_status_table_output(tmp_path, capsys):
    from datetime import datetime, timezone

    from oldp_ingestor.cli import cmd_status
    from oldp_ingestor.results import write_result

    started = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 2, 9, 3, 10, 0, tzinfo=timezone.utc)
    write_result(
        str(tmp_path),
        "cases",
        "rii",
        started,
        finished,
        created=50,
        skipped=10,
        errors=0,
    )

    class FakeArgs:
        results_dir = str(tmp_path)
        stale_hours = 168
        json = False

    cmd_status(FakeArgs())
    captured = capsys.readouterr()
    assert "Provider" in captured.out
    assert "rii" in captured.out
    assert "(never)" in captured.out  # other providers never ran


# --- Exit code from main ---


def test_main_results_dir_from_env(monkeypatch, tmp_path):
    """--results-dir defaults to OLDP_RESULTS_DIR env var."""
    monkeypatch.setenv("OLDP_RESULTS_DIR", str(tmp_path))
    monkeypatch.setattr("sys.argv", ["oldp-ingestor", "status", "--help"])
    from oldp_ingestor.cli import main

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


# --- Source injection ---


def test_cmd_cases_includes_source(monkeypatch):
    """cmd_cases should inject provider.SOURCE into each case POST data."""
    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)

    post_calls = []

    class FakeClient:
        def post(self, path, data):
            post_calls.append((path, data))
            return {}

    monkeypatch.setattr(
        "oldp_ingestor.cli.OLDPClient.from_settings", lambda: FakeClient()
    )

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False

    cmd_cases(FakeArgs())
    assert len(post_calls) == 1
    assert "source" in post_calls[0][1]
    assert post_calls[0][1]["source"]["name"] == "Dummy"


# --- Sink CLI tests ---


def test_cmd_laws_json_file_sink(tmp_path):
    from oldp_ingestor.cli import cmd_laws

    fixture_path = _make_fixture_file(LAW_FIXTURES)
    _output_dir = str(tmp_path / "export")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        sink = "json-file"
        output_dir = _output_dir

    exit_code = cmd_laws(FakeArgs())
    assert exit_code == 0

    # Check law book file
    book_path = os.path.join(_output_dir, "law_books", "TST.json")
    assert os.path.exists(book_path)
    with open(book_path) as f:
        data = json.load(f)
    assert data["code"] == "TST"

    # Check law file
    law_path = os.path.join(_output_dir, "laws", "TST", "s-1.json")
    assert os.path.exists(law_path)
    with open(law_path) as f:
        data = json.load(f)
    assert data["section"] == "§ 1"


def test_cmd_cases_json_file_sink(tmp_path):
    from oldp_ingestor.cli import cmd_cases

    fixture_path = _make_fixture_file(CASE_FIXTURES)
    _output_dir = str(tmp_path / "export")

    class FakeArgs:
        provider = "dummy"
        path = fixture_path
        limit = None
        verbose = False
        sink = "json-file"
        output_dir = _output_dir

    exit_code = cmd_cases(FakeArgs())
    assert exit_code == 0

    case_path = os.path.join(_output_dir, "cases", "I_ZR_1_21.json")
    assert os.path.exists(case_path)
    with open(case_path) as f:
        data = json.load(f)
    assert data["file_number"] == "I ZR 1/21"
    assert data["court_name"] == "Bundesgerichtshof"


def test_make_sink_json_file_requires_output_dir():
    from oldp_ingestor.cli import _make_sink

    class FakeArgs:
        sink = "json-file"
        output_dir = None

    with pytest.raises(SystemExit):
        _make_sink(FakeArgs())


def test_make_sink_default_is_api(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    monkeypatch.setenv("OLDP_API_TOKEN", "test-token")

    from oldp_ingestor.cli import _make_sink
    from oldp_ingestor.sinks.api import ApiSink

    class FakeArgs:
        sink = "api"

    sink = _make_sink(FakeArgs())
    assert isinstance(sink, ApiSink)


def test_make_sink_no_attr_defaults_to_api(monkeypatch):
    """FakeArgs without sink attribute defaults to api sink."""
    monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")
    monkeypatch.setenv("OLDP_API_TOKEN", "test-token")

    from oldp_ingestor.cli import _make_sink
    from oldp_ingestor.sinks.api import ApiSink

    class FakeArgs:
        pass

    sink = _make_sink(FakeArgs())
    assert isinstance(sink, ApiSink)
