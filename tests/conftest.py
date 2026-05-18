"""Shared pytest configuration and fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _default_user_agent():
    """Configure a deterministic UA so HttpBaseClient/OLDPClient construction
    works in tests without each test setting one explicitly. Tests that
    exercise UA configuration directly are free to overwrite or reset it.
    """
    from oldp_ingestor.providers import http_client

    http_client.configure_user_agent(
        "oldp-ingestor-test", "https://github.com/openlegaldata/oldp-ingestor"
    )
    yield
    http_client._reset_user_agent_for_tests()


def pytest_addoption(parser):
    parser.addoption(
        "--run-real",
        action="store_true",
        default=False,
        help="Run tests that make real network requests",
    )
    parser.addoption(
        "--run-playwright",
        action="store_true",
        default=False,
        help="Run tests that require Playwright browsers",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-real"):
        skip_real = pytest.mark.skip(reason="needs --run-real option to run")
        for item in items:
            if "real" in item.keywords:
                item.add_marker(skip_real)

    if not config.getoption("--run-playwright"):
        skip_pw = pytest.mark.skip(reason="needs --run-playwright option to run")
        for item in items:
            if "playwright" in item.keywords:
                item.add_marker(skip_pw)
