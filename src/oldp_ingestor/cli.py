import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests

from oldp_ingestor.client import OLDPClient
from oldp_ingestor.court_analysis import (
    analyze_missing_courts,
    format_table,
    format_tsv,
    parse_missing_courts,
)
from oldp_ingestor.providers.base import CaseProvider, LawProvider
from oldp_ingestor.validation import validate_case
from oldp_ingestor.results import (
    check_health,
    format_status_table,
    read_all_results,
    write_result,
)

logger = logging.getLogger("oldp_ingestor")


def _write_result_and_return(
    args,
    command,
    provider_name,
    started_at,
    created,
    skipped,
    errors,
    status,
    invalid=0,
):
    """Write result file (if configured) and return the appropriate exit code."""
    finished_at = datetime.now(timezone.utc)
    results_dir = getattr(args, "results_dir", None)
    if results_dir:
        write_result(
            results_dir,
            command,
            provider_name,
            started_at,
            finished_at,
            created=created,
            skipped=skipped,
            invalid=invalid,
            errors=errors,
            status=status,
        )
    if status == "error":
        return 2
    return 1 if errors > 0 else 0


def _save_failed_cases(results_dir, provider, failed_cases):
    """Save failed cases to a JSON file for later replay."""
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, f"failed_{provider}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(failed_cases, f, indent=2, ensure_ascii=False)
        f.write("\n")
    logger.info(
        "Saved %d failed case(s) to %s — fix courts/aliases then replay with: "
        "oldp-ingestor replay --input %s",
        len(failed_cases),
        path,
        path,
    )


def _load_failed_cases(path):
    """Load failed cases from a JSON file."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cmd_info(args):
    client = OLDPClient.from_settings()
    data = client.get("/api/?format=json")
    print(json.dumps(data, indent=2))


def _make_sink(args):
    sink_type = getattr(args, "sink", "api")
    if sink_type == "json-file":
        from oldp_ingestor.sinks.json_file import JSONFileSink

        output_dir = getattr(args, "output_dir", None)
        if not output_dir:
            logger.error("--output-dir is required when using --sink json-file")
            sys.exit(1)
        return JSONFileSink(output_dir)

    from oldp_ingestor.sinks.api import ApiSink

    write_delay = getattr(args, "write_delay", 0.0) or 0.0
    client = OLDPClient.from_settings(write_delay=write_delay)
    return ApiSink(client)


def cmd_laws(args):
    started_at = datetime.now(timezone.utc)
    status = "ok"
    books_errors = 0
    laws_errors = 0

    try:
        sink = _make_sink(args)
        provider = _make_law_provider(args)

        logger.info("Fetching law books from provider '%s'...", args.provider)
        books = provider.get_law_books()
        logger.info("Found %d law book(s).", len(books))

        if args.limit and len(books) > args.limit:
            books = books[: args.limit]
            logger.info("Limiting to %d law book(s).", args.limit)

        books_created = 0
        books_skipped = 0
        laws_created = 0
        laws_skipped = 0

        for book in books:
            book_label = f"{book['code']} ({book.get('revision_date', '?')})"
            # Inject source from provider
            if provider.SOURCE.get("name"):
                book["source"] = provider.SOURCE
            try:
                sink.write_law_book(book)
                books_created += 1
                logger.info("Created book: %s", book_label)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 409:
                    books_skipped += 1
                    logger.info("Skipped book (already exists): %s", book_label)
                    continue
                else:
                    books_errors += 1
                    logger.error("Error creating book %s: %s", book_label, e)
                    continue

            laws = provider.get_laws(book["code"], book.get("revision_date", ""))
            logger.info("Ingesting %d law(s) for %s...", len(laws), book_label)

            for law in laws:
                law_label = law.get("section", law.get("slug", "?"))
                # Ensure title is not blank (API requires it)
                if not law.get("title"):
                    law["title"] = law.get("section", "Untitled")
                # Truncate section/title to 200 chars (API max_length)
                for field in ("section", "title"):
                    if field in law and len(law[field]) > 200:
                        law[field] = law[field][:200]
                # Remove None values (API rejects null for optional fields)
                law = {k: v for k, v in law.items() if v is not None}
                # Inject source from provider
                if provider.SOURCE.get("name"):
                    law["source"] = provider.SOURCE
                try:
                    sink.write_law(law)
                    laws_created += 1
                    logger.debug("Created law: %s", law_label)
                except requests.HTTPError as e:
                    if e.response is not None and e.response.status_code == 409:
                        laws_skipped += 1
                        logger.debug("Skipped law (already exists): %s", law_label)
                    else:
                        laws_errors += 1
                        detail = ""
                        if e.response is not None:
                            try:
                                detail = f" - {e.response.json()}"
                            except (ValueError, AttributeError):
                                detail = f" - {e.response.text[:200]}"
                        logger.error(
                            "Error creating law %s: %s%s", law_label, e, detail
                        )

        total_errors = books_errors + laws_errors
        logger.info(
            "Summary: books created=%d skipped=%d errors=%d | laws created=%d skipped=%d errors=%d",
            books_created,
            books_skipped,
            books_errors,
            laws_created,
            laws_skipped,
            laws_errors,
        )

        if total_errors > 0:
            status = "partial"

        return _write_result_and_return(
            args,
            "laws",
            args.provider,
            started_at,
            created=books_created + laws_created,
            skipped=books_skipped + laws_skipped,
            errors=total_errors,
            status=status,
        )

    except Exception:
        logger.exception("Fatal error in laws command")
        return _write_result_and_return(
            args,
            "laws",
            args.provider,
            started_at,
            created=0,
            skipped=0,
            errors=1,
            status="error",
        )


def _validate_date(value: str, label: str) -> None:
    """Validate that *value* is a valid ISO 8601 date (YYYY-MM-DD)."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date for %s: %r (expected YYYY-MM-DD)", label, value)
        sys.exit(1)


def _make_law_provider(args) -> LawProvider:
    if args.provider == "dummy":
        from oldp_ingestor.providers.dummy.dummy_laws import DummyLawProvider

        if not args.path:
            logger.error("--path is required for the dummy provider")
            sys.exit(1)
        if not os.path.isfile(args.path):
            logger.error("File not found: %s", args.path)
            sys.exit(1)
        return DummyLawProvider(path=args.path)

    if args.provider == "ris":
        from oldp_ingestor.providers.de.ris import RISProvider

        if args.date_from:
            _validate_date(args.date_from, "--date-from")
        if args.date_to:
            _validate_date(args.date_to, "--date-to")
        return RISProvider(
            search_term=args.search_term,
            limit=args.limit,
            date_from=args.date_from,
            date_to=args.date_to,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "gii":
        from oldp_ingestor.providers.de.gii import GII_TOC_URL, GiiLawProvider

        cache_dir = getattr(args, "cache_dir", None)
        if not cache_dir:
            logger.error("--cache-dir is required for the gii provider")
            sys.exit(1)
        oldp_client = OLDPClient.from_settings()
        return GiiLawProvider(
            oldp_client=oldp_client,
            cache_dir=cache_dir,
            toc_url=getattr(args, "toc_url", None) or GII_TOC_URL,
            force_full=getattr(args, "full", False),
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    logger.error("Unknown provider '%s'", args.provider)
    sys.exit(1)


def cmd_cases(args):
    started_at = datetime.now(timezone.utc)
    status = "ok"
    cases_errors = 0

    try:
        sink = _make_sink(args)
        provider = _make_case_provider(args)
        batch_size = max(1, getattr(args, "batch_size", 100) or 100)

        logger.info(
            "Streaming cases from provider '%s' (batch_size=%d)...",
            args.provider,
            batch_size,
        )

        cases_found = 0
        cases_created = 0
        cases_skipped = 0
        cases_invalid = 0
        failed_cases: list[dict] = []

        def _flush_progress():
            logger.info(
                "Progress: found=%d created=%d skipped=%d invalid=%d errors=%d",
                cases_found,
                cases_created,
                cases_skipped,
                cases_invalid,
                cases_errors,
            )

        for case in provider.iter_cases():
            cases_found += 1
            if args.limit and cases_found > args.limit:
                logger.info("Limit reached at %d case(s).", args.limit)
                break

            case_label = case.get("file_number", "?")
            # Truncate fields to API max lengths
            for field, max_len in (
                ("file_number", 100),
                ("title", 255),
                ("court_name", 255),
            ):
                if (
                    field in case
                    and isinstance(case[field], str)
                    and len(case[field]) > max_len
                ):
                    case[field] = case[field][:max_len]
            # Remove None values (API rejects null for optional fields)
            case = {k: v for k, v in case.items() if v is not None}

            # Validate before sending to API
            validation_error = validate_case(case)
            if validation_error:
                cases_invalid += 1
                logger.warning(
                    "Skipped invalid case %s: %s", case_label, validation_error
                )
                if cases_found % batch_size == 0:
                    _flush_progress()
                continue

            # Inject source from provider
            if provider.SOURCE.get("name"):
                case["source"] = provider.SOURCE
            try:
                sink.write_case(case)
                cases_created += 1
                logger.debug("Created case: %s", case_label)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 409:
                    cases_skipped += 1
                    logger.debug("Skipped case (already exists): %s", case_label)
                else:
                    cases_errors += 1
                    detail = ""
                    if e.response is not None:
                        try:
                            detail = f" - {e.response.json()}"
                        except (ValueError, AttributeError):
                            detail = f" - {e.response.text[:200]}"
                    logger.error("Error creating case %s: %s%s", case_label, e, detail)
                    failed_cases.append({"case": case, "error": str(e) + detail})

            if cases_found % batch_size == 0:
                _flush_progress()

        logger.info("Found %d case(s).", cases_found)

        # Save failed cases to file for later replay
        if failed_cases and args.results_dir:
            _save_failed_cases(args.results_dir, args.provider, failed_cases)

        logger.info(
            "Summary: cases created=%d skipped=%d invalid=%d errors=%d",
            cases_created,
            cases_skipped,
            cases_invalid,
            cases_errors,
        )

        if cases_errors > 0:
            status = "partial"

        return _write_result_and_return(
            args,
            "cases",
            args.provider,
            started_at,
            created=cases_created,
            skipped=cases_skipped,
            invalid=cases_invalid,
            errors=cases_errors,
            status=status,
        )

    except Exception:
        logger.exception("Fatal error in cases command")
        return _write_result_and_return(
            args,
            "cases",
            args.provider,
            started_at,
            created=0,
            skipped=0,
            errors=1,
            status="error",
        )


def cmd_replay(args):
    """Replay failed cases from a saved JSON file.

    After fixing court aliases or other issues, re-submit previously
    failed cases without re-scraping the source websites.
    """
    started_at = datetime.now(timezone.utc)
    status = "ok"

    try:
        sink = _make_sink(args)
        failed_entries = _load_failed_cases(args.input)
        logger.info("Loaded %d failed case(s) from %s", len(failed_entries), args.input)

        created = 0
        skipped = 0
        errors = 0
        still_failed: list[dict] = []

        for entry in failed_entries:
            case = entry["case"]
            case_label = case.get("file_number", "?")
            try:
                sink.write_case(case)
                created += 1
                logger.info("Replayed case: %s", case_label)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 409:
                    skipped += 1
                    logger.debug("Skipped (already exists): %s", case_label)
                else:
                    errors += 1
                    detail = ""
                    if e.response is not None:
                        try:
                            detail = f" - {e.response.json()}"
                        except (ValueError, AttributeError):
                            detail = f" - {e.response.text[:200]}"
                    logger.error("Still failing: %s: %s%s", case_label, e, detail)
                    still_failed.append({"case": case, "error": str(e) + detail})

        # Overwrite the failed file with remaining failures (or delete if empty)
        if still_failed:
            with open(args.input, "w", encoding="utf-8") as f:
                json.dump(still_failed, f, indent=2, ensure_ascii=False)
                f.write("\n")
            logger.info(
                "%d case(s) still failing — saved to %s", len(still_failed), args.input
            )
        else:
            os.remove(args.input)
            logger.info("All cases replayed successfully — removed %s", args.input)

        logger.info(
            "Replay summary: created=%d skipped=%d errors=%d", created, skipped, errors
        )

        if errors > 0:
            status = "partial"

        results_dir = getattr(args, "results_dir", None)
        if results_dir:
            finished_at = datetime.now(timezone.utc)
            write_result(
                results_dir,
                "replay",
                os.path.basename(args.input),
                started_at,
                finished_at,
                created=created,
                skipped=skipped,
                errors=errors,
                status=status,
            )

        return 1 if errors > 0 else 0

    except Exception:
        logger.exception("Fatal error in replay command")
        return 2


_JURIS_PROVIDERS = {
    "juris-bb": "BbBeCaseProvider",
    "juris-hh": "HhCaseProvider",
    "juris-mv": "MvCaseProvider",
    "juris-rlp": "RlpCaseProvider",
    "juris-sa": "SaCaseProvider",
    "juris-sh": "ShCaseProvider",
    "juris-bw": "BwCaseProvider",
    "juris-sl": "SlCaseProvider",
    "juris-he": "HeCaseProvider",
    "juris-th": "ThCaseProvider",
}


def _make_case_provider(args) -> CaseProvider:
    # Validate dates up-front (applies to all providers that accept them)
    if getattr(args, "date_from", None):
        _validate_date(args.date_from, "--date-from")
    if getattr(args, "date_to", None):
        _validate_date(args.date_to, "--date-to")

    if args.provider == "dummy":
        from oldp_ingestor.providers.dummy.dummy_cases import DummyCaseProvider

        if not args.path:
            logger.error("--path is required for the dummy provider")
            sys.exit(1)
        if not os.path.isfile(args.path):
            logger.error("File not found: %s", args.path)
            sys.exit(1)
        return DummyCaseProvider(path=args.path)

    if args.provider == "ris":
        from oldp_ingestor.providers.de.ris_cases import RISCaseProvider

        return RISCaseProvider(
            court=args.court,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "rii":
        from oldp_ingestor.providers.de.rii import RiiCaseProvider

        return RiiCaseProvider(
            court=args.court,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
            cache_dir=getattr(args, "cache_dir", None),
        )

    if args.provider == "by":
        from oldp_ingestor.providers.de.by import ByCaseProvider

        return ByCaseProvider(
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "nrw":
        from oldp_ingestor.providers.de.nrw import NrwCaseProvider

        return NrwCaseProvider(
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "ns":
        from oldp_ingestor.providers.de.ns import NsCaseProvider

        return NsCaseProvider(
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "eu":
        from oldp_ingestor.providers.de.eu import EuCaseProvider
        from oldp_ingestor import settings

        return EuCaseProvider(
            username=settings.EURLEX_USER,
            password=settings.EURLEX_PASSWORD,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "hb":
        from oldp_ingestor.providers.de.hb import BremenCaseProvider

        return BremenCaseProvider(
            court=args.court,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "sn-ovg":
        from oldp_ingestor.providers.de.sn_ovg import SnOvgCaseProvider

        return SnOvgCaseProvider(
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "sn":
        from oldp_ingestor.providers.de.sn import SnCaseProvider

        return SnCaseProvider(
            court=args.court,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider == "sn-verfgh":
        from oldp_ingestor.providers.de.sn_verfgh import SnVerfghCaseProvider

        return SnVerfghCaseProvider(
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
        )

    if args.provider in _JURIS_PROVIDERS:
        import oldp_ingestor.providers.de.juris as juris_mod

        cls = getattr(juris_mod, _JURIS_PROVIDERS[args.provider])
        return cls(
            court=args.court,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            request_delay=args.request_delay,
            proxy=args.proxy,
            cache_dir=getattr(args, "cache_dir", None),
        )

    logger.error("Unknown provider '%s'", args.provider)
    sys.exit(1)


def cmd_status(args):
    results = read_all_results(args.results_dir)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        table = format_status_table(results, stale_hours=args.stale_hours)
        print(table)

    healthy = check_health(results, stale_hours=args.stale_hours)
    return 0 if healthy else 1


def _fetch_all_pages(client, path):
    """Fetch all pages from a DRF paginated endpoint."""
    items = []
    url = path
    while url:
        data = client.get(url)
        items.extend(data.get("results", []))
        next_url = data.get("next")
        if next_url:
            # DRF returns absolute URLs; strip the base to get a relative path
            if next_url.startswith(("http://", "https://")):
                # Keep only path + query string
                from urllib.parse import urlparse

                parsed = urlparse(next_url)
                url = parsed.path
                if parsed.query:
                    url += "?" + parsed.query
            else:
                url = next_url
        else:
            url = None
    return items


def cmd_analyze_courts(args):
    if args.input == "-":
        lines = sys.stdin.read().splitlines()
    else:
        with open(args.input) as f:
            lines = f.read().splitlines()

    missing = parse_missing_courts(lines)
    if not missing:
        print("No 'court_not_found' errors found in input.")
        return 0

    client = OLDPClient.from_settings()
    logger.info("Fetching courts from OLDP API...")
    courts = _fetch_all_pages(client, "/api/courts/?format=json")
    logger.info("Fetched %d court(s).", len(courts))

    logger.info("Fetching cities from OLDP API...")
    cities = _fetch_all_pages(client, "/api/cities/?format=json")
    logger.info("Fetched %d city/cities.", len(cities))

    logger.info("Fetching states from OLDP API...")
    states = _fetch_all_pages(client, "/api/states/?format=json")
    logger.info("Fetched %d state(s).", len(states))

    analyses = analyze_missing_courts(missing, courts, cities, states)

    if args.format == "tsv":
        print(format_tsv(analyses))
    else:
        print(format_table(analyses))

    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="oldp-ingestor", description="OLDP data ingestor CLI"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--results-dir",
        default=os.environ.get("OLDP_RESULTS_DIR", ""),
        help="Directory for JSON result files (env: OLDP_RESULTS_DIR)",
    )
    parser.add_argument(
        "--sink",
        choices=["api", "json-file"],
        default="api",
        help="Output sink: api (default) or json-file",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for json-file sink",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="SOCKS5/HTTP proxy URL for provider requests (e.g. socks5h://localhost:1080). "
        "Not applied to the OLDP API sink.",
    )
    parser.add_argument(
        "--max-rpm",
        type=int,
        default=0,
        help="Hard cap on requests-per-minute per target host (0 = disabled, default). "
        "Applied on top of --request-delay. Use for backfills against rate-sensitive hosts.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=5,
        help="After this many consecutive fully-failed calls to the same host, "
        "abort the run with BlockedHostError (0 = disabled, default: 5).",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("info", help="Show API info from the OLDP instance")

    laws_parser = subparsers.add_parser("laws", help="Ingest laws into OLDP")
    laws_parser.add_argument(
        "--provider",
        required=True,
        choices=["dummy", "ris", "gii"],
        help="Data source provider",
    )
    laws_parser.add_argument(
        "--path",
        help="Path to JSON fixture file (required for dummy provider)",
    )
    laws_parser.add_argument(
        "--search-term",
        help="Filter legislation by keyword (omit to fetch all)",
    )
    laws_parser.add_argument(
        "--limit",
        type=int,
        help="Max number of law books to ingest",
    )
    laws_parser.add_argument(
        "--date-from",
        help="Only fetch legislation adopted on or after this date (ISO 8601, e.g. 2025-01-01)",
    )
    laws_parser.add_argument(
        "--date-to",
        help="Only fetch legislation adopted on or before this date (ISO 8601, e.g. 2025-12-31)",
    )
    laws_parser.add_argument(
        "--request-delay",
        type=float,
        default=0.2,
        help="Delay in seconds between API requests (default: 0.2, i.e. max ~300 req/min)",
    )
    laws_parser.add_argument(
        "--write-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between OLDP API write requests (default: 0.0)",
    )
    laws_parser.add_argument(
        "--cache-dir",
        help="Directory to cache downloaded zips and per-slug state "
        "(required for the gii provider; enables resume on interrupted runs).",
    )
    laws_parser.add_argument(
        "--toc-url",
        help="Override the TOC URL (gii provider only).",
    )
    laws_parser.add_argument(
        "--full",
        action="store_true",
        help="Skip If-Modified-Since and re-download every zip "
        "(gii provider only; forces a full re-sync).",
    )

    cases_parser = subparsers.add_parser("cases", help="Ingest cases into OLDP")
    _case_choices = [
        "dummy",
        "ris",
        "rii",
        "by",
        "nrw",
        "ns",
        "eu",
        "hb",
        "sn-ovg",
        "sn",
        "sn-verfgh",
        "juris-bb",
        "juris-hh",
        "juris-mv",
        "juris-rlp",
        "juris-sa",
        "juris-sh",
        "juris-bw",
        "juris-sl",
        "juris-he",
        "juris-th",
    ]
    cases_parser.add_argument(
        "--provider",
        required=True,
        choices=_case_choices,
        help="Data source provider",
    )
    cases_parser.add_argument(
        "--path",
        help="Path to JSON fixture file (required for dummy provider)",
    )
    cases_parser.add_argument(
        "--limit",
        type=int,
        help="Max number of cases to ingest",
    )
    cases_parser.add_argument(
        "--court",
        help="Filter by court code (e.g. BGH, BVerfG) — only for ris provider",
    )
    cases_parser.add_argument(
        "--date-from",
        help="Only fetch decisions on or after this date (ISO 8601, e.g. 2026-01-01)",
    )
    cases_parser.add_argument(
        "--date-to",
        help="Only fetch decisions on or before this date (ISO 8601, e.g. 2026-06-30)",
    )
    cases_parser.add_argument(
        "--request-delay",
        type=float,
        default=0.2,
        help="Delay in seconds between API requests (default: 0.2, i.e. max ~300 req/min)",
    )
    cases_parser.add_argument(
        "--write-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between OLDP API write requests (default: 0.0)",
    )
    cases_parser.add_argument(
        "--cache-dir",
        help="Directory to cache downloaded XMLs (currently RII only). "
        "Enables resume on interrupted runs.",
    )
    cases_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Max cases to buffer between provider and sink; also the "
        "progress-logging interval (default: 100). Providers should not "
        "accumulate more than this before streaming to the sink.",
    )

    status_parser = subparsers.add_parser(
        "status", help="Show status dashboard for all providers"
    )
    status_parser.add_argument(
        "--stale-hours",
        type=int,
        default=168,
        help="Hours after which a result is considered stale (default: 168 = 7 days)",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        help="Output status as JSON",
    )

    analyze_parser = subparsers.add_parser(
        "analyze-courts",
        help="Analyse missing courts from ingestor logs",
    )
    analyze_parser.add_argument(
        "--input",
        required=True,
        help="Log file path (use '-' for stdin)",
    )
    analyze_parser.add_argument(
        "--format",
        choices=["table", "tsv"],
        default="table",
        help="Output format (default: table)",
    )

    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay failed cases from a saved JSON file (no re-scraping)",
    )
    replay_parser.add_argument(
        "--input",
        required=True,
        help="Path to failed_*.json file from a previous run",
    )
    replay_parser.add_argument(
        "--write-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between OLDP API write requests (default: 0.0)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    from oldp_ingestor.providers import http_client

    http_client.configure_defaults(
        max_rpm=args.max_rpm,
        circuit_breaker_threshold=args.max_consecutive_failures,
    )

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "info": cmd_info,
        "laws": cmd_laws,
        "cases": cmd_cases,
        "replay": cmd_replay,
        "status": cmd_status,
        "analyze-courts": cmd_analyze_courts,
    }

    exit_code = commands[args.command](args)
    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
