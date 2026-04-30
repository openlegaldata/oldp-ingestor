"""Parser for gesetze-im-internet.de gii-norm XML zips.

DTD reference: https://www.gesetze-im-internet.de/dtd/1.01/gii-norm.dtd

Each zip contains exactly one XML file with a ``<dokumente>`` root holding
multiple ``<norm>`` children. The first norm carries the law book metadata;
subsequent norms are individual sections (paragraphs, articles, table of
contents entries, …).

The mapping below is lifted from oldp's legacy ``LawInputHandlerFS``
(``oldp/apps/laws/processing/law_processor.py``) but the code here is the
upload-side: we produce the dict shapes that ``ApiSink`` POSTs to the OLDP
``/api/law_books/`` and ``/api/laws/`` endpoints.

Code-vs-jurabk note
-------------------
The legacy parser preferred ``<amtabk>`` over ``<jurabk>`` for the book
``code``. OLDP production data uses ``<jurabk>`` verbatim (e.g.
``"SGB 5"``, ``"BImSchV 1 2010"``) — so this parser uses ``<jurabk>``.
``<amtabk>`` is exposed separately on each ``Law`` row as a metadata field.
"""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import date, datetime
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)


_REVISION_DATE_RE = re.compile(r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})")


class GiiParseError(ValueError):
    """Raised when a gii-norm zip cannot be parsed into book + laws."""


def extract_xml_from_zip(zip_bytes: bytes) -> bytes:
    """Return the inner XML bytes from a gii xml.zip.

    Each gii zip contains exactly one ``BJNR<...>.xml``. If multiple XMLs are
    present, the first one is returned (this hasn't been observed in the wild
    but is defensive).
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not xml_names:
                raise GiiParseError("No XML file inside gii zip")
            with zf.open(xml_names[0]) as fh:
                return fh.read()
    except zipfile.BadZipFile as exc:
        raise GiiParseError(f"Not a valid zip file: {exc}") from exc


def _node_text(node, xpath_str: str) -> str | None:
    """Return the first text() result of ``xpath_str`` under ``node`` or None."""
    matches = node.xpath(xpath_str)
    return matches[0] if matches else None


def _serialize_children(node, xpath_str: str) -> str:
    """Serialize all children matched by ``xpath_str`` into a single string."""
    parts = node.xpath(xpath_str)
    return "".join(etree.tostring(p).decode("utf-8") for p in parts)


def _parse_revision_date(text: str | None) -> str | None:
    """Extract a YYYY-MM-DD date from the standkommentar text, if present."""
    if not text:
        return None
    match = _REVISION_DATE_RE.search(text)
    if not match:
        return None
    try:
        return date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        ).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _builddate_to_iso(builddate: str | None) -> str | None:
    """Convert YYYYMMDDHHMMSS root attribute to YYYY-MM-DD."""
    if not builddate or len(builddate) < 8:
        return None
    try:
        return datetime.strptime(builddate[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _book_revision_date(header_norm, root_builddate: str | None) -> str | None:
    """Pick the most informative revision date for the book.

    Prefers the latest ``<standangabe standtyp="Stand">`` ``<standkommentar>``
    text containing a parseable D.M.YYYY. Falls back to the root
    ``builddate``.
    """
    candidates: list[str] = []
    types = header_norm.xpath("metadaten/standangabe/standtyp/text()")
    comments = header_norm.xpath("metadaten/standangabe/standkommentar/text()")
    for typ, comment in zip(types, comments):
        if typ != "Stand":
            continue
        parsed = _parse_revision_date(comment)
        if parsed:
            candidates.append(parsed)
    if candidates:
        return max(candidates)
    return _builddate_to_iso(root_builddate)


def _strip(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def parse_book_metadata(xml_bytes: bytes) -> dict[str, Any]:
    """Parse only book-level metadata from a gii-norm XML.

    Cheap header-only extraction used during the conditional-GET sweep
    where most entries get skipped. Returns a dict with at least
    ``code`` (jurabk) and may include ``title``, ``revision_date``,
    ``doknr``, ``builddate``.

    Raises:
        GiiParseError: if the document has no parseable jurabk.
    """
    try:
        tree = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        raise GiiParseError(f"Malformed XML: {exc}") from exc
    if tree.tag != "dokumente":
        raise GiiParseError(f"Unexpected root element: {tree.tag!r}")

    root_builddate = tree.get("builddate")
    root_doknr = tree.get("doknr")

    header = tree.find("norm")
    if header is None:
        raise GiiParseError("No <norm> element in document")

    jurabk = _strip(_node_text(header, "metadaten/jurabk/text()"))
    if not jurabk:
        raise GiiParseError("Missing <jurabk> in header norm")

    title = _strip(_node_text(header, "metadaten/langue/text()"))
    if title:
        title = " ".join(title.split())  # collapse newlines/whitespace

    revision_date = _book_revision_date(header, root_builddate)

    book: dict[str, Any] = {
        "code": jurabk,
        "title": title or jurabk,
        "doknr": root_doknr,
        "builddate": _builddate_to_iso(root_builddate),
    }
    if revision_date:
        book["revision_date"] = revision_date
    return book


def _build_changelog(header_norm) -> list[dict[str, str]]:
    types = header_norm.xpath("metadaten/standangabe/standtyp/text()")
    comments = header_norm.xpath("metadaten/standangabe/standkommentar/text()")
    return [{"type": typ, "text": comment} for typ, comment in zip(types, comments)]


def _law_from_norm(norm, book_code: str, order: int) -> dict[str, Any] | None:
    """Build an OLDP Law dict from a single ``<norm>`` element.

    Returns ``None`` when the norm cannot become a valid Law row: either it
    has no section/title labels at all (header artifacts), or it has no
    body content (e.g. the ``Inhaltsübersicht`` placeholder). The OLDP
    ``Law.content`` field requires ``min_length=1`` so emitting an empty
    one would only earn a 400.
    """
    section = _strip(_node_text(norm, "metadaten/enbez/text()"))
    title = _strip(_node_text(norm, "metadaten/titel/text()"))

    if not section and not title:
        return None

    content = _serialize_children(norm, "textdaten/text/Content/*")
    if not content.strip():
        # gii structural placeholders (Inhaltsübersicht, repealed norms,
        # unbound section headings) carry no body. The API rejects empty
        # content with HTTP 400, so skip them at the parser.
        return None

    footnotes_xml = _serialize_children(norm, "textdaten/fussnoten/Content/*")

    law: dict[str, Any] = {
        "book_code": book_code,
        "section": section or title,
        "title": title or section,
        "content": content,
        "order": order,
    }

    doknr = norm.get("doknr")
    if doknr:
        law["doknr"] = doknr

    amtabk = _strip(_node_text(norm, "metadaten/amtabk/text()"))
    if amtabk:
        law["amtabk"] = amtabk

    kurzue = _strip(_node_text(norm, "metadaten/kurzue/text()"))
    if kurzue:
        law["kurzue"] = kurzue

    if footnotes_xml:
        law["footnotes"] = json.dumps(footnotes_xml, ensure_ascii=False)

    return law


def parse_gii_xml(xml_bytes: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse a gii-norm XML into ``(book, laws)`` dicts ready for OLDP upload.

    The first ``<norm>`` is treated as the book header; remaining ``<norm>``
    elements become individual Law rows in document order. Norms without
    section/title labels (the metadata-only header norm itself, plus the
    occasional placeholder) are skipped from the laws list.

    Raises:
        GiiParseError: on missing root, missing <norm>, or missing jurabk.
    """
    book = parse_book_metadata(xml_bytes)
    book_code = book["code"]

    try:
        tree = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:  # already covered by parse_book_metadata
        raise GiiParseError(f"Malformed XML: {exc}") from exc
    header = tree.find("norm")

    book["changelog"] = json.dumps(_build_changelog(header), ensure_ascii=False)
    header_footnotes = _serialize_children(header, "textdaten/fussnoten/Content/*")
    book["footnotes"] = json.dumps(header_footnotes, ensure_ascii=False)

    laws: list[dict[str, Any]] = []
    order = 0
    for idx, norm in enumerate(tree.findall("norm")):
        if idx == 0:
            continue  # header norm — already consumed for book metadata
        law = _law_from_norm(norm, book_code, order)
        if law is None:
            continue
        laws.append(law)
        order += 1

    return book, laws


def parse_gii_zip(zip_bytes: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Convenience wrapper: extract inner XML from zip then parse it."""
    return parse_gii_xml(extract_xml_from_zip(zip_bytes))
