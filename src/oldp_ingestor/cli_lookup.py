"""CLI handlers for the ``lookup`` subcommand group.

The lookup commands expose targeted citation-based search as a small set
of typed tools designed for use by an AI agent (e.g. Claude Code) — the
agent does the citation parsing and provider routing, the ingestor just
executes one upstream query per call.

Output contract — every command prints a JSON object to stdout with one
of three top-level statuses::

    {"status": "ok",        "result": ...}      # success with data
    {"status": "not_found", "reason": "..."}    # ran fine, no hits
    {"status": "error",     "reason": "..."}    # upstream / validation

Errors *during execution* are caught and converted to ``error`` so the
calling agent never has to parse Python tracebacks.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from oldp_ingestor.providers.lookup import (
    LookupMixin,
    filter_courts,
    summarise_court,
)

logger = logging.getLogger("oldp_ingestor.lookup")


# Provider name → (module path, class name). Kept explicit so the help
# text and the ``providers`` listing stay in lockstep, and so we can
# import each class lazily without paying Playwright import cost when
# the agent only wants the RIS or NRW slice.
_LOOKUP_PROVIDERS: dict[str, tuple[str, str]] = {
    "ris": ("oldp_ingestor.providers.de.ris_cases", "RISCaseProvider"),
    "nrw": ("oldp_ingestor.providers.de.nrw", "NrwCaseProvider"),
    "ns": ("oldp_ingestor.providers.de.ns", "NsCaseProvider"),
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


def lookup_provider_names() -> list[str]:
    return list(_LOOKUP_PROVIDERS.keys())


def _load_provider_cls(name: str):
    if name not in _LOOKUP_PROVIDERS:
        raise KeyError(name)
    module_path, class_name = _LOOKUP_PROVIDERS[name]
    import importlib

    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)


def _instantiate(name: str, args):
    """Build a provider instance with the args the CLI exposes."""
    cls = _load_provider_cls(name)
    kwargs = {
        "request_delay": getattr(args, "request_delay", 0.2),
        "proxy": getattr(args, "proxy", None),
    }
    return cls(**kwargs)


def _emit(payload: dict[str, Any]) -> int:
    """Print ``payload`` as compact JSON; return exit code from status."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    sys.stdout.write("\n")
    sys.stdout.flush()
    if payload.get("status") == "error":
        return 2
    if payload.get("status") == "not_found":
        return 1
    return 0


def _fetch_all_courts() -> list[dict]:
    """Paginate the public OLDP courts list. No auth header is sent.

    Targets :data:`oldp_ingestor.settings.OLDP_PROD_API_URL` (defaults to
    ``https://de.openlegaldata.io``). The endpoint is publicly readable;
    sending the dev/local ``OLDP_API_TOKEN`` from settings against prod
    returns 401, which is why we deliberately bypass
    :class:`OLDPClient` here. Errors propagate to the caller and surface
    in the JSON response as ``courts_error``.
    """
    import requests

    from oldp_ingestor import settings
    from oldp_ingestor.providers.http_client import get_user_agent

    base = settings.OLDP_PROD_API_URL.rstrip("/")
    courts: list[dict] = []
    url = f"{base}/api/courts/?format=json&limit=500"
    headers = {"User-Agent": get_user_agent()}
    while url:
        resp = requests.get(url, headers=headers, timeout=(10, 60))
        resp.raise_for_status()
        data = resp.json()
        courts.extend(data.get("results", []))
        url = data.get("next") or ""
    return courts


def cmd_lookup_providers(args) -> int:
    """List lookup-capable providers + their declared capabilities.

    With ``--resolve-courts`` (default on; pass ``--no-resolve-courts`` to
    skip), the OLDP courts API is queried once and each provider's
    ``court_filter`` is applied to project the matching subset. This is
    the runtime-resolved coverage table the agent uses to route a
    citation by court.
    """
    resolve = getattr(args, "resolve_courts", True)
    all_courts: list[dict] = []
    courts_error: str | None = None
    if resolve:
        try:
            all_courts = _fetch_all_courts()
        except Exception as exc:  # noqa: BLE001 — surfaces in payload
            courts_error = f"{type(exc).__name__}: {exc}"

    result: dict[str, Any] = {}
    for name, (module_path, class_name) in _LOOKUP_PROVIDERS.items():
        try:
            cls = _load_provider_cls(name)
        except Exception as exc:  # noqa: BLE001
            result[name] = {"error": f"import: {type(exc).__name__}: {exc}"}
            continue
        if not issubclass(cls, LookupMixin):
            continue
        cap = cls.LOOKUP_CAPABILITY
        entry: dict[str, Any] = {
            "lookup_keys": list(cap.keys),
            "cost": cap.cost,
            "court_filter": cap.court_filter,
            "source": getattr(cls, "SOURCE", {}),
        }
        if resolve and all_courts:
            matched = filter_courts(all_courts, cap)
            entry["courts"] = [summarise_court(c) for c in matched]
            entry["courts_total"] = len(matched)
        result[name] = entry

    payload: dict[str, Any] = {"status": "ok", "result": result}
    if courts_error:
        payload["courts_error"] = courts_error
    return _emit(payload)


def cmd_lookup_search(args) -> int:
    provider_name = args.provider
    if provider_name not in _LOOKUP_PROVIDERS:
        return _emit(
            {
                "status": "error",
                "reason": f"Unknown lookup provider {provider_name!r}",
            }
        )
    if not (args.file_number or args.ecli):
        return _emit(
            {
                "status": "error",
                "reason": "lookup search requires --file-number or --ecli",
            }
        )

    provider = _instantiate(provider_name, args)
    try:
        candidates = provider.lookup_search(
            file_number=args.file_number,
            ecli=args.ecli,
            court_hint=args.court_hint,
            date=args.date,
            limit=args.limit,
        )
    except NotImplementedError:
        return _emit(
            {
                "status": "error",
                "reason": f"{provider_name} does not support lookup_search",
            }
        )
    except ValueError as exc:
        return _emit({"status": "error", "reason": str(exc)})
    except Exception as exc:  # noqa: BLE001
        return _emit(
            {
                "status": "error",
                "reason": f"{type(exc).__name__}: {exc}",
            }
        )
    finally:
        _try_close(provider)

    if not candidates:
        return _emit(
            {
                "status": "not_found",
                "reason": "no candidates matched the search criteria",
                "provider": provider_name,
            }
        )
    return _emit(
        {
            "status": "ok",
            "provider": provider_name,
            "candidates": candidates,
        }
    )


def cmd_lookup_fetch(args) -> int:
    provider_name = args.provider
    if provider_name not in _LOOKUP_PROVIDERS:
        return _emit(
            {"status": "error", "reason": f"Unknown lookup provider {provider_name!r}"}
        )

    provider = _instantiate(provider_name, args)
    try:
        case = provider.lookup_fetch(args.doc_id)
    except Exception as exc:  # noqa: BLE001
        return _emit({"status": "error", "reason": f"{type(exc).__name__}: {exc}"})
    finally:
        _try_close(provider)

    if case is None:
        return _emit(
            {
                "status": "not_found",
                "reason": "document not retrievable / parseable",
                "provider": provider_name,
                "doc_id": args.doc_id,
            }
        )
    return _emit({"status": "ok", "provider": provider_name, "case": case})


def cmd_lookup_ingest(args) -> int:
    """Fetch a case and POST it to the OLDP API in one step.

    409 ``conflict`` from OLDP is reported as ``status: ok`` with
    ``already_exists: true`` so an agent retrying citations idempotently
    can treat it as a success.
    """
    provider_name = args.provider
    if provider_name not in _LOOKUP_PROVIDERS:
        return _emit(
            {"status": "error", "reason": f"Unknown lookup provider {provider_name!r}"}
        )

    provider = _instantiate(provider_name, args)
    try:
        case = provider.lookup_fetch(args.doc_id)
    except Exception as exc:  # noqa: BLE001
        return _emit({"status": "error", "reason": f"{type(exc).__name__}: {exc}"})
    finally:
        _try_close(provider)

    if case is None:
        return _emit(
            {
                "status": "not_found",
                "reason": "document not retrievable / parseable",
                "provider": provider_name,
                "doc_id": args.doc_id,
            }
        )

    # Inject provider SOURCE metadata exactly like the streaming flow
    # so the OLDP backend records who supplied the document.
    if provider.SOURCE.get("name"):
        case["source"] = provider.SOURCE

    from oldp_ingestor.client import OLDPClient
    from oldp_ingestor.sinks.api import ApiSink
    import requests

    sink = ApiSink(OLDPClient.from_settings())
    try:
        sink.write_case(case)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 409:
            return _emit(
                {
                    "status": "ok",
                    "provider": provider_name,
                    "already_exists": True,
                    "case": _slim_case_for_response(case),
                }
            )
        detail = ""
        if exc.response is not None:
            try:
                detail = exc.response.json()
            except ValueError:
                detail = exc.response.text[:500]
        return _emit(
            {
                "status": "error",
                "reason": f"OLDP API rejected POST: HTTP {status}",
                "details": detail,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return _emit({"status": "error", "reason": f"{type(exc).__name__}: {exc}"})

    return _emit(
        {
            "status": "ok",
            "provider": provider_name,
            "already_exists": False,
            "case": _slim_case_for_response(case),
        }
    )


def _slim_case_for_response(case: dict) -> dict:
    """Drop ``content`` from a case dict before echoing it back.

    The full content can be hundreds of KB; the agent already has it from
    the fetch step and an ingest response only needs to identify what
    landed in OLDP.
    """
    return {k: v for k, v in case.items() if k != "content"}


def _try_close(provider) -> None:
    """Close a provider when the method exists (e.g. Playwright juris)."""
    close = getattr(provider, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass
