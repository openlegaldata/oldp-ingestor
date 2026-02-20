import pytest

from oldp_ingestor.client import OLDPClient


def test_client_init_basic():
    client = OLDPClient(api_url="http://localhost:8000/")
    assert client.api_url == "http://localhost:8000"
    assert "Authorization" not in client.session.headers
    assert client.session.auth is None


def test_client_init_with_token():
    client = OLDPClient(api_url="http://localhost:8000", api_token="my-token")
    assert client.session.headers["Authorization"] == "Token my-token"


def test_client_init_with_http_auth():
    client = OLDPClient(api_url="http://localhost:8000", http_auth="user:pass")
    assert client.session.auth == ("user", "pass")


def test_client_init_http_auth_no_colon():
    """http_auth without ':' is ignored."""
    client = OLDPClient(api_url="http://localhost:8000", http_auth="nocolon")
    assert client.session.auth is None


def test_client_get(monkeypatch):
    class FakeResp:
        status_code = 200

        def json(self):
            return {"key": "value"}

        def raise_for_status(self):
            pass

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "get", lambda url, **kw: FakeResp())

    result = client.get("/api/test/")
    assert result == {"key": "value"}


def test_client_post(monkeypatch):
    class FakeResp:
        status_code = 201

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "post", lambda url, **kw: FakeResp())

    result = client.post("/api/test/", data={"name": "test"})
    assert result == {"id": 1}


def test_client_from_settings(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://test:9000")
    monkeypatch.setenv("OLDP_API_TOKEN", "tok123")
    monkeypatch.setenv("OLDP_API_HTTP_AUTH", "u:p")

    # Force reload settings
    import importlib

    import oldp_ingestor.settings

    importlib.reload(oldp_ingestor.settings)

    client = OLDPClient.from_settings()
    assert client.api_url == "http://test:9000"
    assert client.session.headers["Authorization"] == "Token tok123"
    assert client.session.auth == ("u", "p")


def test_client_from_settings_missing_url(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "")
    monkeypatch.delenv("OLDP_API_TOKEN", raising=False)

    import importlib

    import oldp_ingestor.settings

    importlib.reload(oldp_ingestor.settings)

    with pytest.raises(ValueError, match="OLDP_API_URL"):
        OLDPClient.from_settings()
