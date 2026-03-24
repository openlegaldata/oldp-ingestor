import pytest
import requests

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


def test_client_init_write_delay():
    client = OLDPClient(api_url="http://localhost:8000", write_delay=0.5)
    assert client.write_delay == 0.5


def test_client_init_write_delay_default():
    client = OLDPClient(api_url="http://localhost:8000")
    assert client.write_delay == 0.0


def test_client_get(monkeypatch):
    class FakeResp:
        status_code = 200

        def json(self):
            return {"key": "value"}

        def raise_for_status(self):
            pass

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", lambda method, url, **kw: FakeResp())

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
    monkeypatch.setattr(client.session, "request", lambda method, url, **kw: FakeResp())

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


def test_client_from_settings_write_delay(monkeypatch):
    monkeypatch.setenv("OLDP_API_URL", "http://test:9000")
    monkeypatch.setenv("OLDP_API_TOKEN", "tok123")
    monkeypatch.setenv("OLDP_API_HTTP_AUTH", "")

    import importlib

    import oldp_ingestor.settings

    importlib.reload(oldp_ingestor.settings)

    client = OLDPClient.from_settings(write_delay=0.3)
    assert client.write_delay == 0.3


# --- Retry tests ---


def test_client_post_retries_429(monkeypatch):
    """POST retries on 429 then succeeds."""
    call_count = [0]

    class Resp429:
        status_code = 429
        headers = {}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class Resp201:
        status_code = 201
        headers = {}

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return Resp429()
        return Resp201()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    result = client.post("/api/test/", data={})
    assert result == {"id": 1}
    assert call_count[0] == 2


def test_client_post_retries_503(monkeypatch):
    """POST retries on 503 then succeeds."""
    call_count = [0]

    class Resp503:
        status_code = 503
        headers = {}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class Resp201:
        status_code = 201
        headers = {}

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return Resp503()
        return Resp201()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    result = client.post("/api/test/", data={})
    assert result == {"id": 1}
    assert call_count[0] == 2


def test_client_post_retries_connection_error(monkeypatch):
    """POST retries on ConnectionError then succeeds."""
    call_count = [0]

    class Resp201:
        status_code = 201
        headers = {}

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise requests.ConnectionError("connection refused")
        return Resp201()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    result = client.post("/api/test/", data={})
    assert result == {"id": 1}
    assert call_count[0] == 2


def test_client_post_respects_retry_after(monkeypatch):
    """POST uses Retry-After header value for wait time."""
    sleep_values = []

    class Resp429:
        status_code = 429
        headers = {"Retry-After": "5"}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class Resp201:
        status_code = 201
        headers = {}

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    call_count = [0]

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return Resp429()
        return Resp201()

    def mock_sleep(s):
        sleep_values.append(s)

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", mock_sleep)

    client.post("/api/test/", data={})
    assert 5.0 in sleep_values


def test_client_post_raises_after_max_retries(monkeypatch):
    """POST raises after exhausting all retries."""

    class Resp429:
        status_code = 429
        headers = {}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    def mock_request(method, url, **kwargs):
        return Resp429()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    with pytest.raises(requests.HTTPError):
        client.post("/api/test/", data={})


def test_client_post_no_retry_on_other_errors(monkeypatch):
    """POST does not retry on non-retryable status codes (400, 500)."""
    call_count = [0]

    class Resp400:
        status_code = 400
        headers = {}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        return Resp400()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    with pytest.raises(requests.HTTPError):
        client.post("/api/test/", data={})
    assert call_count[0] == 1  # no retry


def test_client_post_pacing(monkeypatch):
    """POST sleeps for write_delay before each request."""
    sleep_values = []

    class FakeResp:
        status_code = 201

        def json(self):
            return {"id": 1}

        def raise_for_status(self):
            pass

    def mock_sleep(s):
        sleep_values.append(s)

    client = OLDPClient(api_url="http://localhost:8000", write_delay=0.5)
    monkeypatch.setattr(client.session, "request", lambda m, u, **kw: FakeResp())
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", mock_sleep)

    client.post("/api/test/", data={})
    assert 0.5 in sleep_values


def test_client_get_retries_429(monkeypatch):
    """GET also retries on 429."""
    call_count = [0]

    class Resp429:
        status_code = 429
        headers = {}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class Resp200:
        status_code = 200
        headers = {}

        def json(self):
            return {"key": "value"}

        def raise_for_status(self):
            pass

    def mock_request(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return Resp429()
        return Resp200()

    client = OLDPClient(api_url="http://localhost:8000")
    monkeypatch.setattr(client.session, "request", mock_request)
    monkeypatch.setattr("oldp_ingestor.client.time.sleep", lambda s: None)

    result = client.get("/api/test/")
    assert result == {"key": "value"}
    assert call_count[0] == 2
