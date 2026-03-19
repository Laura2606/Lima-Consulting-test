import logging
import re

from bs4 import BeautifulSoup
from readability import Document

logger = logging.getLogger(__name__)


def cleanse_article(raw_html: str) -> str:
    """
    Extract clean article text from raw HTML.

    Steps:
        1. readability-lxml strips navigation, ads, sidebars, footers
        2. BeautifulSoup removes residual <script>, <style>, <figure>
        3. Whitespace normalisation produces clean paragraph text

    Args:
        raw_html: Full HTML content of the article page.

    Returns:
        Clean plain-text article body. Empty string if extraction fails.
    """
    if not raw_html:
        return ""

    try:
        # Step 1: Readability extracts the main content block
        doc = Document(raw_html)
        content_html = doc.summary()

        # Step 2: BeautifulSoup cleans residual markup
        soup = BeautifulSoup(content_html, "lxml")

        # Remove elements that readability sometimes leaves behind
        for tag in soup.find_all(["script", "style", "figure", "figcaption", "aside"]):
            tag.decompose()

        # Remove share buttons, related-article blocks, etc.
        for attr_value in ["share", "related", "sidebar", "newsletter", "ad-slot"]:
            for el in soup.find_all(attrs={"class": re.compile(attr_value, re.I)}):
                el.decompose()
            for el in soup.find_all(attrs={"id": re.compile(attr_value, re.I)}):
                el.decompose()

        # Step 3: Extract text and normalise whitespace
        text = soup.get_text(separator="\n")
        # Collapse multiple blank lines into one
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.splitlines()]
        text = "\n".join(lines).strip()

        return text

    except Exception:
        logger.exception("Failed to cleanse article content")
        return ""


def extract_summary(article_text: str, max_chars: int = 300) -> str:
    """
    Extract a short summary snippet from the article text.

    Takes the first paragraph that is long enough to be meaningful.

    Args:
        article_text: Clean article text.
        max_chars: Maximum characters for the snippet.

    Returns:
        A short text snippet suitable for search result previews.
    """
    if not article_text:
        return ""

    paragraphs = [p.strip() for p in article_text.split("\n\n") if len(p.strip()) > 40]

    if not paragraphs:
        return article_text[:max_chars]

    snippet = paragraphs[0]
    if len(snippet) > max_chars:
        # Cut at the last space before the limit
        snippet = snippet[:max_chars].rsplit(" ", 1)[0] + "…"

    return snippet
