from oldp_ingestor.client import OLDPClient
from oldp_ingestor.sinks.base import Sink


class ApiSink(Sink):
    """Sink that writes data to an OLDP instance via its REST API."""

    def __init__(self, client: OLDPClient):
        self.client = client

    def write_law_book(self, book: dict) -> None:
        self.client.post("/api/law_books/", data=book)

    def write_law(self, law: dict) -> None:
        self.client.post("/api/laws/", data=law)

    def write_case(self, case: dict) -> None:
        self.client.post("/api/cases/", data=case)
