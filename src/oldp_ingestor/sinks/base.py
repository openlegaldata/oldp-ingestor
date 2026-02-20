from abc import ABC, abstractmethod


class Sink(ABC):
    """Abstract base class for all data sinks."""

    @abstractmethod
    def write_law_book(self, book: dict) -> None:
        """Write a law book record."""

    @abstractmethod
    def write_law(self, law: dict) -> None:
        """Write a law record."""

    @abstractmethod
    def write_case(self, case: dict) -> None:
        """Write a case record."""
