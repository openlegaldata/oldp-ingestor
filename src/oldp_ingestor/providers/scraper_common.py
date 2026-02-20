"""Scraper base client with HTML/XML parsing utilities.

Provides common scraping helpers used by RII, BY, and NRW providers:
- ZIP/XML extraction
- HTML parsing with lxml
- German date parsing
- HTML tag stripping
- Content section building
"""

import io
import logging
import re
import zipfile
from html.parser import HTMLParser

import lxml.html
from lxml import etree

from oldp_ingestor.providers.http_client import HttpBaseClient

logger = logging.getLogger(__name__)


class _MLStripper(HTMLParser):
    """Simple HTML tag stripper."""

    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self._fed: list[str] = []

    def handle_data(self, d: str) -> None:
        self._fed.append(d)

    def get_data(self) -> str:
        return "".join(self._fed)

    def error(self, message: str) -> None:
        pass


class ScraperBaseClient(HttpBaseClient):
    """HTTP client with HTML/XML scraping utilities."""

    def _get_html_tree(self, url_or_path: str) -> lxml.html.HtmlElement:
        """Fetch HTML and return parsed lxml tree."""
        text = self._get(url_or_path).text.replace("\r\n", "\n")
        return lxml.html.fromstring(text)

    def _get_xml_from_zip(
        self, url_or_path: str, encoding: str = "utf-8"
    ) -> str | None:
        """Download ZIP, extract first XML file, return as string.

        Returns None if no XML file found in ZIP.
        """
        resp = self._get(url_or_path, stream=True)
        resp.raw.decode_content = True
        zip_bytes = io.BytesIO(resp.raw.read())
        try:
            zip_file = zipfile.ZipFile(zip_bytes)
        except zipfile.BadZipFile:
            logger.warning("Bad ZIP file from %s", url_or_path)
            return None
        for fn in zip_file.namelist():
            if fn.endswith(".xml"):
                return zip_file.read(fn).decode(encoding)
        logger.warning("ZIP from %s contains no XML files", url_or_path)
        return None

    @staticmethod
    def parse_german_date(date_str: str) -> str:
        """Parse DD.MM.YYYY -> YYYY-MM-DD."""
        parts = date_str.strip().split(".")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return date_str

    @staticmethod
    def strip_tags(html: str) -> str:
        """Remove HTML tags, return plain text."""
        s = _MLStripper()
        s.feed(html)
        return s.get_data()

    @staticmethod
    def get_inner_html(element, encoding: str = "utf-8") -> str:
        """Serialize lxml element's children as HTML string."""
        return "".join(
            etree.tostring(child).decode(encoding) for child in element.iterchildren()
        )

    @staticmethod
    def extract_body(html: str) -> str:
        """Extract <body> content from full HTML page."""
        match = re.search(r"<body[^>]*>(.*)</body>", html, re.DOTALL)
        if match:
            return match.group(1).strip()
        return html

    def _extract_text_from_pdf(self, url: str) -> str:
        """Download PDF from *url* and return extracted text wrapped in HTML."""
        import pymupdf

        resp = self._get(url)
        resp.raise_for_status()
        doc = pymupdf.open(stream=resp.content, filetype="pdf")
        paragraphs = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                paragraphs.append(text)
        doc.close()
        if not paragraphs:
            return ""
        html_parts = [f"<p>{p}</p>" for p in paragraphs]
        return "\n".join(html_parts)

    def _css_text(self, tree, selector: str, default: str = "") -> str:
        """Get text_content() of first CSS match."""
        from lxml.cssselect import CSSSelector

        matches = CSSSelector(selector)(tree)
        if matches:
            return matches[0].text_content().strip()
        return default

    def _xpath_text(
        self,
        tree,
        xpath: str,
        default: str | None = None,
        join_multiple_with: str = "\n",
    ) -> str | None:
        """Get text of first XPath match."""
        matches = tree.xpath(xpath + "/text()")
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            return join_multiple_with.join(matches)
        return default

    def _build_content_html(
        self,
        tree,
        content_tags: list[tuple[str, str]],
        xpath_tpl: str = "//dokument/{tag}",
        with_headline: bool = True,
    ) -> str:
        """Build HTML content from tagged sections.

        Args:
            tree: lxml tree (XML or HTML)
            content_tags: list of (tag_name, headline) tuples
            xpath_tpl: XPath template with {tag} placeholder
            with_headline: whether to prepend <h2> headlines
        """
        content = ""
        for tag_name, headline in content_tags:
            xpath = xpath_tpl.format(tag=tag_name)
            matches = tree.xpath(xpath)
            for i, match in enumerate(matches):
                tag_content = self.get_inner_html(match)
                if tag_content.strip():
                    if content:
                        content += "\n"
                    if with_headline and i == 0 and headline:
                        content += f"<h2>{headline}</h2>"
                    content += "\n\n" + tag_content
        return content
