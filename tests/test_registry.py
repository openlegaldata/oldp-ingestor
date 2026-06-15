"""Tests for the provider capability registry and the `providers` CLI command."""

import json
import subprocess
import sys

from oldp_ingestor.providers import registry


def test_capabilities_shape():
    caps = registry.capabilities()
    assert set(caps) == {"cases", "laws"}
    # Every entry exposes the two derived capabilities (or an import error).
    for group in caps.values():
        for entry in group.values():
            assert "error" in entry or {"kind", "date_from"} <= set(entry)


def test_kind_is_derived_from_class_hierarchy():
    caps = registry.capabilities("cases")["cases"]
    # Pure HTTP providers
    assert caps["ris"]["kind"] == "http"
    assert caps["by"]["kind"] == "http"
    assert caps["sn-ovg"]["kind"] == "http"
    # Playwright (browser) providers
    assert caps["sn"]["kind"] == "playwright"
    assert caps["juris-bb"]["kind"] == "playwright"
    # rii is a ScraperBaseClient + PlaywrightBaseClient hybrid → playwright
    assert caps["rii"]["kind"] == "playwright"


def test_date_from_is_derived_from_constructor():
    caps = registry.capabilities()
    # All case providers accept a date window.
    assert all(e.get("date_from") is True for e in caps["cases"].values()), caps[
        "cases"
    ]
    # Law providers: only ris is incremental; gii/eurlex are not.
    assert caps["laws"]["ris"]["date_from"] is True
    assert caps["laws"]["gii"]["date_from"] is False
    assert caps["laws"]["eurlex"]["date_from"] is False


def test_command_filter():
    assert set(registry.capabilities("laws")) == {"laws"}
    assert set(registry.capabilities("cases")) == {"cases"}


def test_juris_classes_subset():
    juris = registry.juris_case_classes()
    assert juris["juris-bb"] == "BbBeCaseProvider"
    assert all(name.startswith("juris-") for name in juris)
    assert len(juris) == 10


def test_cli_providers_command_emits_json():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "providers"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["cases"]["juris-bb"]["kind"] == "playwright"
    assert payload["laws"]["gii"]["date_from"] is False


def test_cli_providers_command_filtered():
    result = subprocess.run(
        [sys.executable, "-m", "oldp_ingestor.cli", "providers", "--command", "laws"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert set(payload) == {"laws"}
