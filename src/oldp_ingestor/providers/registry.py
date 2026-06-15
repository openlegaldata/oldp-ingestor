"""Central registry of ingest providers and their operational capabilities.

This is the single source of truth for *which* providers exist and *what
they can do*, used by both the ``oldp-ingestor providers`` CLI command and
the cron orchestration scripts (``scripts/ingest.sh``,
``scripts/anomaly-detect.py``). Those scripts used to hard-code parallel
rosters (which providers are Playwright-based, which support incremental
``--date-from`` fetching); they now read this instead, so adding a provider
in one place keeps the schedule wrappers in sync automatically.

Capabilities are *derived by introspecting the provider classes*, not
hand-maintained:

* ``kind`` — ``"playwright"`` if the class is a
  :class:`~oldp_ingestor.providers.playwright_client.PlaywrightBaseClient`
  (headless browser), else ``"http"``.
* ``date_from`` — whether the class constructor accepts a ``date_from``
  argument, i.e. supports an incremental date window.

The name → class maps below are the only thing maintained by hand; the
construction logic in ``cli.py`` (which passes per-provider kwargs) imports
the juris subset from here so the provider rosters live in exactly one place.
"""

from __future__ import annotations

import importlib
import inspect
from typing import Any

# Provider id -> (module path, class name). Order mirrors the argparse
# ``choices`` so ``--help`` output is unchanged. ``dummy`` is intentionally
# excluded: it is a test fixture requiring ``--path`` and is never scheduled.
CASE_PROVIDERS: dict[str, tuple[str, str]] = {
    "ris": ("oldp_ingestor.providers.de.ris_cases", "RISCaseProvider"),
    "rii": ("oldp_ingestor.providers.de.rii", "RiiCaseProvider"),
    "by": ("oldp_ingestor.providers.de.by", "ByCaseProvider"),
    "nrw": ("oldp_ingestor.providers.de.nrw", "NrwCaseProvider"),
    "ns": ("oldp_ingestor.providers.de.ns", "NsCaseProvider"),
    "eu": ("oldp_ingestor.providers.de.eu", "EuCaseProvider"),
    "hb": ("oldp_ingestor.providers.de.hb", "BremenCaseProvider"),
    "sn-ovg": ("oldp_ingestor.providers.de.sn_ovg", "SnOvgCaseProvider"),
    "sn": ("oldp_ingestor.providers.de.sn", "SnCaseProvider"),
    "sn-verfgh": ("oldp_ingestor.providers.de.sn_verfgh", "SnVerfghCaseProvider"),
    "juris-bb": ("oldp_ingestor.providers.de.juris", "BbBeCaseProvider"),
    "juris-hh": ("oldp_ingestor.providers.de.juris", "HhCaseProvider"),
    "juris-mv": ("oldp_ingestor.providers.de.juris", "MvCaseProvider"),
    "juris-rlp": ("oldp_ingestor.providers.de.juris", "RlpCaseProvider"),
    "juris-sa": ("oldp_ingestor.providers.de.juris", "SaCaseProvider"),
    "juris-sh": ("oldp_ingestor.providers.de.juris", "ShCaseProvider"),
    "juris-bw": ("oldp_ingestor.providers.de.juris", "BwCaseProvider"),
    "juris-sl": ("oldp_ingestor.providers.de.juris", "SlCaseProvider"),
    "juris-he": ("oldp_ingestor.providers.de.juris", "HeCaseProvider"),
    "juris-th": ("oldp_ingestor.providers.de.juris", "ThCaseProvider"),
}

LAW_PROVIDERS: dict[str, tuple[str, str]] = {
    "ris": ("oldp_ingestor.providers.de.ris", "RISProvider"),
    "gii": ("oldp_ingestor.providers.de.gii", "GiiLawProvider"),
    "eurlex": ("oldp_ingestor.providers.de.eurlex_laws", "EurLexLawProvider"),
}

_GROUPS: dict[str, dict[str, tuple[str, str]]] = {
    "cases": CASE_PROVIDERS,
    "laws": LAW_PROVIDERS,
}


def case_provider_names() -> list[str]:
    """Schedulable case provider ids (excludes the ``dummy`` fixture)."""
    return list(CASE_PROVIDERS)


def law_provider_names() -> list[str]:
    """Schedulable law provider ids (excludes the ``dummy`` fixture)."""
    return list(LAW_PROVIDERS)


def juris_case_classes() -> dict[str, str]:
    """``{provider_id: class_name}`` for the juris portals (for cli.py)."""
    return {
        name: cls
        for name, (_mod, cls) in CASE_PROVIDERS.items()
        if name.startswith("juris-")
    }


def _load(module_path: str, class_name: str) -> type:
    return getattr(importlib.import_module(module_path), class_name)


def _kind(cls: type) -> str:
    from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

    return "playwright" if issubclass(cls, PlaywrightBaseClient) else "http"


def _supports_date_from(cls: type) -> bool:
    try:
        return "date_from" in inspect.signature(cls.__init__).parameters
    except (TypeError, ValueError):
        return False


def capabilities(command: str | None = None) -> dict[str, dict[str, Any]]:
    """Introspect provider classes and return their capabilities.

    Shape::

        {
          "cases": {"ris": {"kind": "http", "date_from": true}, ...},
          "laws":  {"ris": {"kind": "http", "date_from": true},
                    "gii": {"kind": "http", "date_from": false}, ...}
        }

    *command* (``"cases"`` / ``"laws"``) limits the output to one group.
    A provider whose class cannot be imported is reported with an
    ``"error"`` field instead of capabilities, so one broken provider
    never blocks the others (the cron wrappers fall back to safe defaults).
    """
    out: dict[str, dict[str, Any]] = {}
    for group, table in _GROUPS.items():
        if command and command != group:
            continue
        out[group] = {}
        for name, (module_path, class_name) in table.items():
            try:
                cls = _load(module_path, class_name)
                out[group][name] = {
                    "kind": _kind(cls),
                    "date_from": _supports_date_from(cls),
                }
            except Exception as exc:  # noqa: BLE001 — surfaced in payload
                out[group][name] = {"error": f"{type(exc).__name__}: {exc}"}
    return out
