"""Provider for EUR-Lex (European Court of Justice case law).

Fetches case law from EUR-Lex using the CELLAR SPARQL endpoint for ECLI search
and the EUR-Lex HTML endpoint for full-text content.

Two-step pipeline:
1. SPARQL query to CELLAR for ECLI identifiers + metadata (paginated)
2. HTML full text from EUR-Lex per CELEX number -> content

Content is fetched from EUR-Lex (eur-lex.europa.eu/legal-content/) as the
primary source, with CELLAR (publications.europa.eu/resource/celex/) as
fallback. The SPARQL endpoint is public and requires no authentication.
"""

import logging
import re
from urllib.parse import urljoin

import lxml.html
from lxml import etree

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

EURLEX_BASE_URL = "https://eur-lex.europa.eu"
CELLAR_BASE_URL = "https://publications.europa.eu"
CELLAR_SPARQL_URL = "https://publications.europa.eu/webapi/rdf/sparql"
EURLEX_SPARQL_PAGE_SIZE = 100
EURLEX_MIN_CONTENT_LEN = 10

# CELEX sector 6 type code -> German case type name
_CELEX_TYPE_NAMES = {
    "CJ": "Urteil",
    "CO": "Beschluss",
    "CC": "Schlussantrag des Generalanwalts",
    "CS": "Pfändung",
    "CT": "Drittwiderspruch",
    "CV": "Gutachten",
    "CX": "Beschluss",
    "CD": "Entscheidung",
    "CP": "Stellungnahme",
    "CN": "Mitteilung neue Rechtssache",
    "CA": "Mitteilung Urteil",
    "CB": "Mitteilung Beschluss",
    "CU": "Mitteilung Gutachtenantrag",
    "CG": "Mitteilung: Gutachten",
    "TJ": "Urteil",
    "TO": "Beschluss",
    "TC": "Schlussantrag des Generalanwalts",
    "TT": "Drittwiderspruch",
    "TN": "Mitteilung neue Rechtssache",
    "TA": "Mitteilung Urteil",
    "TB": "Mitteilung Beschluss",
    "FJ": "Urteil",
    "FO": "Beschluss",
    "FT": "Drittwiderspruch",
    "FN": "Mitteilung neue Rechtssache",
    "FA": "Mitteilung Urteil",
    "FB": "Mitteilung Beschluss",
}

# ECLI court code -> German court name
_ECLI_COURT_NAMES = {
    "C": "Europäischer Gerichtshof",
    "T": "Gericht der Europäischen Union",
    "F": "Gericht für den öffentlichen Dienst der Europäischen Union",
}

# Unicode non-breaking hyphen (U+2011) used in EUR-Lex file numbers
_UNICODE_DASH = chr(8209)


class EuCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from EUR-Lex (European Court of Justice).

    Uses the CELLAR SPARQL endpoint for ECLI search and EUR-Lex HTML
    endpoint for full-text content (with CELLAR as fallback).

    Supports server-side date filtering via SPARQL ``FILTER`` clauses on
    the ``?date`` variable. When date_from/date_to are set, the SPARQL
    query includes ``FILTER(?date >= "..."^^xsd:date)`` /
    ``FILTER(?date <= "..."^^xsd:date)`` so only matching ECLIs are
    returned from the CELLAR endpoint.

    Args:
        username: EUR-Lex web service username (unused, kept for CLI compat).
        password: EUR-Lex web service password (unused, kept for CLI compat).
        date_from: Optional start date filter (YYYY-MM-DD). Added as
            ``FILTER(?date >= ...)`` in the SPARQL query for server-side
            filtering.
        date_to: Optional end date filter (YYYY-MM-DD). Added as
            ``FILTER(?date <= ...)`` in the SPARQL query for server-side
            filtering.
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "EUR-Lex",
        "homepage": "https://eur-lex.europa.eu/",
    }

    def __init__(
        self,
        username: str = "",
        password: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(
            base_url=EURLEX_BASE_URL, request_delay=request_delay, proxy=proxy
        )
        self.username = username
        self.password = password
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _build_sparql_query(self, limit: int, offset: int) -> str:
        """Build SPARQL query for ECLI search with optional date filters."""
        filters = []
        if self.date_from:
            filters.append(f'FILTER(?date >= "{self.date_from}"^^xsd:date)')
        if self.date_to:
            filters.append(f'FILTER(?date <= "{self.date_to}"^^xsd:date)')
        filter_clause = "\n  ".join(filters)

        return f"""\
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?ecli ?date ?celex
WHERE {{
  ?work cdm:case-law_ecli ?ecli .
  ?work cdm:work_date_document ?date .
  ?work cdm:resource_legal_id_celex ?celex .
  {filter_clause}
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}"""

    def _search_eclis(self) -> list[dict]:
        """Search CELLAR SPARQL endpoint for ECLI identifiers and metadata.

        Returns list of dicts with keys: ecli, date, celex.
        """
        results: list[dict] = []
        offset = 0
        effective_limit = self.limit or 10000

        while len(results) < effective_limit:
            page_size = min(EURLEX_SPARQL_PAGE_SIZE, effective_limit - len(results))
            query = self._build_sparql_query(page_size, offset)

            resp = self._get(
                CELLAR_SPARQL_URL,
                params={"query": query, "format": "application/json"},
            )

            data = resp.json()
            bindings = data.get("results", {}).get("bindings", [])

            if not bindings:
                break

            page_results = [
                {
                    "ecli": b["ecli"]["value"],
                    "date": b.get("date", {}).get("value", ""),
                    "celex": b.get("celex", {}).get("value", ""),
                }
                for b in bindings
                if "ecli" in b
            ]
            results.extend(page_results)

            logger.info(
                "SPARQL page (offset %d): found %d ECLIs (total %d)",
                offset,
                len(page_results),
                len(results),
            )

            if len(page_results) < page_size:
                break

            offset += page_size

        if self.limit and len(results) > self.limit:
            results = results[: self.limit]

        # Deduplicate by ECLI — SPARQL can return duplicate ECLIs with
        # _RES suffix CELEX variants (e.g. 62024CJ0008 and 62024CJ0008_RES).
        # Prefer the CELEX without _RES suffix.
        seen_eclis: dict[str, dict] = {}
        for item in results:
            ecli = item["ecli"]
            if ecli not in seen_eclis:
                seen_eclis[ecli] = item
            elif "_RES" in seen_eclis[ecli].get("celex", "") and "_RES" not in item.get(
                "celex", ""
            ):
                seen_eclis[ecli] = item
        if len(seen_eclis) < len(results):
            logger.info("Deduplicated %d -> %d ECLIs", len(results), len(seen_eclis))
        results = list(seen_eclis.values())

        return results

    def _try_fetch_html(self, url: str, celex: str) -> str | None:
        """Try to fetch and parse HTML content from a URL.

        Returns extracted body HTML, or None on failure (HTTP error, WAF
        block, short content, parse error).
        """
        try:
            resp = self._get(
                url, headers={"Accept": "text/html", "Accept-Language": "de"}
            )
        except Exception as exc:
            logger.warning(
                "Failed to fetch HTML for CELEX %s from %s: %s", celex, url, exc
            )
            return None

        if resp.status_code != 200 or len(resp.text) < EURLEX_MIN_CONTENT_LEN:
            logger.warning(
                "Empty/missing content for CELEX %s (status %d, url %s)",
                celex,
                resp.status_code,
                url,
            )
            return None

        # Detect AWS WAF challenge page (returns 200 but with JS challenge)
        if "aws-waf-token" in resp.text:
            logger.warning("WAF challenge detected for CELEX %s at %s", celex, url)
            return None

        try:
            return _extract_html_content(resp.text, url)
        except Exception as exc:
            logger.warning("Failed to parse HTML for CELEX %s: %s", celex, exc)
            return None

    def _fetch_case_content(self, celex: str) -> tuple[str, str] | None:
        """Fetch HTML full text for a CELEX number.

        Tries EUR-Lex first (primary source), falls back to CELLAR if
        EUR-Lex fails (e.g. WAF returns). Strips ``_RES`` suffix from
        CELEX before constructing URLs (these variants always 404).

        Returns ``(content, source_url)`` for the URL that served the
        content, or ``None`` on failure.
        """
        # Strip _RES suffix (always 404s on both EUR-Lex and CELLAR)
        base_celex = re.sub(r"_RES$", "", celex)

        # Try EUR-Lex first (primary source)
        eurlex_url = (
            f"{EURLEX_BASE_URL}/legal-content/DE/TXT/HTML/?uri=CELEX:{base_celex}"
        )
        content = self._try_fetch_html(eurlex_url, base_celex)
        if content is not None:
            return content, eurlex_url

        # Fallback to CELLAR if EUR-Lex fails
        cellar_url = f"{CELLAR_BASE_URL}/resource/celex/{base_celex}"
        content = self._try_fetch_html(cellar_url, base_celex)
        if content is not None:
            return content, cellar_url
        return None

    def get_cases(self) -> list[dict]:
        """Fetch cases from EUR-Lex: SPARQL search -> EUR-Lex HTML content.

        Metadata (date, file_number, type) is extracted from SPARQL + CELEX.
        Content HTML is fetched from EUR-Lex (fallback: CELLAR) via CELEX.
        """
        cases: list[dict] = []

        search_results = self._search_eclis()
        logger.info("Found %d ECLI(s) to process.", len(search_results))

        for item in search_results:
            ecli = item["ecli"]
            celex = item["celex"]
            date = item["date"]

            if not celex:
                logger.warning("No CELEX for %s, skipping", ecli)
                continue

            fetched = self._fetch_case_content(celex)
            if fetched is None:
                continue
            content, source_url = fetched

            if len(content) < EURLEX_MIN_CONTENT_LEN:
                logger.warning(
                    "Skipping %s: content too short (%d chars)", ecli, len(content)
                )
                continue

            # Extract file number from ECLI or title
            file_number = _extract_file_number_from_ecli(ecli)
            case_type = _get_case_type_from_celex(celex)
            court_name = _get_court_name_from_ecli(ecli)

            case: dict = {
                "court_name": court_name,
                "file_number": file_number,
                "date": date,
                "content": content,
                "ecli": ecli,
                "source_url": source_url,
            }

            if case_type:
                case["type"] = case_type

            cases.append(case)

            if self.limit and len(cases) >= self.limit:
                break

        return cases


def _parse_eclis_from_sparql(json_data: dict) -> list[str]:
    """Parse ECLI values from a SPARQL JSON response."""
    bindings = json_data.get("results", {}).get("bindings", [])
    return [b["ecli"]["value"] for b in bindings if "ecli" in b]


def _parse_case_details_from_xml(xml_bytes: bytes, ecli: str) -> dict:
    """Parse case metadata from EUR-Lex XML detail page.

    Returns dict with keys: date, title, file_number, type, ecli.
    Raises on missing required fields.
    """
    tree = etree.fromstring(xml_bytes)

    # Date
    date_values = tree.xpath("//WORK_DATE_DOCUMENT/VALUE/text()")
    date = date_values[0] if date_values else ""

    # Title — find EXPRESSION_TITLE matching DEU language
    title = _extract_title(tree)

    # File number — from title regex or SAMEAS fallback
    file_number = _extract_file_number(tree, title)

    # Case type from CELEX number
    case_type = ""
    celex_values = tree.xpath("//RESOURCE_LEGAL_ID_CELEX/VALUE/text()")
    if celex_values:
        case_type = _get_case_type_from_celex(celex_values[0])

    return {
        "date": date,
        "title": title,
        "file_number": file_number,
        "type": case_type,
        "ecli": ecli,
    }


def _extract_title(tree) -> str:
    """Extract German title from XML tree, taking first part before '#'."""
    for expr_title in tree.xpath("//EXPRESSION_TITLE"):
        titles = expr_title.xpath("./VALUE/text()")
        langs = expr_title.getparent().xpath(
            "./EXPRESSION_USES_LANGUAGE/IDENTIFIER/text()"
        )
        if len(titles) == 1 and len(langs) == 1 and langs[0] == "DEU":
            # Title may contain '#'-separated parts; use only first
            return titles[0].split("#")[0].strip()
    return ""


def _extract_file_number(tree, title: str) -> str:
    """Extract file number from title regex or SAMEAS fallback."""
    # Pattern: letter + dash (ASCII or Unicode U+2011) + digits/digits
    dash_group = "(" + re.escape(_UNICODE_DASH) + "|-)"
    fn_pattern = re.compile("[A-Z]" + dash_group + r"([0-9]{1,5})/([0-9]{2})")

    # Try last '#'-part of full title for file number (legacy used last part)
    fn_matches = list(fn_pattern.finditer(title))

    if len(fn_matches) == 1:
        file_number = fn_matches[0].group(0)
    elif len(fn_matches) > 1:
        file_number = ",".join(m.group(0) for m in fn_matches)
    else:
        # Fallback: extract from SAMEAS/URI with type="case"
        file_number = ""
        for t in tree.xpath('//SAMEAS/URI/TYPE[text()="case"]'):
            ids = t.getparent().xpath("./IDENTIFIER/text()")
            if ids:
                file_number = ids[0]
                break

        if not file_number:
            logger.warning("Cannot extract file number from title: %s", title)
            file_number = ""

    # Replace Unicode non-breaking hyphen with ASCII dash
    return file_number.replace(_UNICODE_DASH, "-")


def _extract_file_number_from_ecli(ecli: str) -> str:
    """Extract file number from ECLI string.

    ECLI format: ECLI:EU:C:2026:180
    The numeric part at the end is the case number within the court's year.
    For a useful file_number, we use the format 'C-180/26' derived from the ECLI.
    """
    parts = ecli.split(":")
    if len(parts) >= 5:
        court = parts[2]  # C, T, F
        year = parts[3]  # 2026
        number = parts[4]  # 180
        return f"{court}-{number}/{year[2:]}"
    return ecli


def _get_court_name_from_ecli(ecli: str) -> str:
    """Map ECLI court code to German court name.

    ECLI format: ECLI:EU:{court}:{year}:{number}
    Court codes: C (EuGH), T (EuG), F (EuGöD).
    Falls back to 'Europäischer Gerichtshof' for unknown codes.
    """
    parts = ecli.split(":")
    if len(parts) >= 3:
        court_code = parts[2]
        name = _ECLI_COURT_NAMES.get(court_code)
        if name:
            return name
        logger.debug("Unknown ECLI court code: %s", court_code)
    return _ECLI_COURT_NAMES["C"]


def _get_case_type_from_celex(celex: str) -> str:
    """Map CELEX number to German case type name.

    CELEX format: sector(1 digit) + year(4 digits) + type(1-2 letters) + ...
    Only sector 6 (case law) is supported.
    """
    match = re.search(r"^([0-9])([0-9]{4})([A-Z]{1,2})", celex)
    if not match:
        return ""

    sector = match.group(1)
    type_code = match.group(3)

    if sector != "6":
        logger.debug("Unsupported CELEX sector: %s", sector)
        return ""

    return _CELEX_TYPE_NAMES.get(type_code, "")


def _extract_html_content(html_text: str, source_url: str) -> str | None:
    """Extract <body> content from EUR-Lex HTML page with link processing."""
    # Some EUR-Lex pages include <?xml encoding=...?> declarations that
    # lxml.html.fromstring() rejects on str input. Strip them.
    cleaned = re.sub(r"<\?xml[^?]*\?>", "", html_text)
    tree = lxml.html.fromstring(cleaned)

    for body in tree.xpath("//body"):
        # Process links: make relative absolute, remove non-http/non-anchor
        for link in body.xpath(".//a[@href]"):
            href = link.attrib["href"]
            if href.startswith("#"):
                pass  # keep anchor links
            elif href.startswith("http"):
                pass  # keep absolute links
            elif href.startswith("."):
                link.attrib["href"] = urljoin(source_url, href)
            else:
                del link.attrib["href"]

        # Strip JS event handler attributes (EUR-Lex uses onclick on footnotes)
        for el in body.xpath(".//*[@onclick or @onerror or @onload]"):
            for attr in ("onclick", "onerror", "onload"):
                if attr in el.attrib:
                    del el.attrib[attr]

        # Remove <script> elements
        for script in body.xpath(".//script"):
            script.getparent().remove(script)

        # Serialize body children as HTML
        return "".join(
            etree.tostring(child, encoding="unicode") for child in body.iterchildren()
        )

    return None
