import logging

import requests

logger = logging.getLogger(__name__)


class OLDPClient:
    """Client for communicating with an OLDP API instance."""

    def __init__(self, api_url: str, api_token: str = "", http_auth: str = ""):
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()

        if api_token:
            self.session.headers["Authorization"] = f"Token {api_token}"

        if http_auth and ":" in http_auth:
            user, password = http_auth.split(":", 1)
            self.session.auth = (user, password)

        logger.debug("Initialized OLDP client for %s", self.api_url)

    def get(self, path: str, **kwargs) -> dict:
        url = f"{self.api_url}{path}"
        logger.debug("GET %s", url)
        resp = self.session.get(url, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, data: dict, **kwargs) -> dict:
        url = f"{self.api_url}{path}"
        logger.debug("POST %s", url)
        resp = self.session.post(url, json=data, **kwargs)
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def from_settings(cls) -> "OLDPClient":
        from oldp_ingestor import settings

        if not settings.OLDP_API_URL:
            raise ValueError("OLDP_API_URL is not set")

        return cls(
            api_url=settings.OLDP_API_URL,
            api_token=settings.OLDP_API_TOKEN,
            http_auth=settings.OLDP_API_HTTP_AUTH,
        )
