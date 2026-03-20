import logging
import re

from bs4 import BeautifulSoup
from readability import Document

logger = logging.getLogger(__name__)


def cleanse_article(raw_html: str) -> str:
    """
    Extrai o texto limpo do artigo a partir do HTML bruto.

    Steps:
    1. readability-lxml remove navegação, anúncios, barras laterais e rodapés
    2. BeautifulSoup remove tags residuais como <script>, <style>, <figure>
    3. A normalização de espaços em branco produz um texto limpo em parágrafos

    Args:
        raw_html: Conteúdo HTML completo da página do artigo.

    Returns:
    Corpo do artigo em texto simples e limpo.
    Retorna string vazia se a extração falhar.
    """
    if not raw_html:
        return ""

    try:
        # Etapa 1: Readability extrai o bloco principal de conteúdo
        doc = Document(raw_html)
        content_html = doc.summary()

        # Etapa 2: BeautifulSoup limpa marcações residuais
        soup = BeautifulSoup(content_html, "lxml")

        # Remove elementos que o readability às vezes deixa para trás
        for tag in soup.find_all(
            ["script", "style", "figure", "figcaption", "aside"]
        ):
            tag.decompose()

        # Remove botões de compartilhamento, blocos de artigos relacionados
        # etc.
        for attr_value in [
            "share",
            "related",
            "sidebar",
            "newsletter",
            "ad-slot",
        ]:
            for el in soup.find_all(
                attrs={"class": re.compile(attr_value, re.I)}
            ):
                el.decompose()
            for el in soup.find_all(
                attrs={"id": re.compile(attr_value, re.I)}
            ):
                el.decompose()

        # Etapa 3: Extrai o texto e normaliza os espaços em branco
        text = soup.get_text(separator="\n")
        # Junta múltiplas linhas em branco em apenas uma separação
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Remove espaços em branco no início e no fim de cada linha
        lines = [line.strip() for line in text.splitlines()]
        text = "\n".join(lines).strip()

        return text

    except Exception:
        logger.exception("Failed to cleanse article content")
        return ""


def extract_summary(article_text: str, max_chars: int = 300) -> str:
    """
    Extrai um pequeno resumo do texto do artigo.

    Pega o primeiro parágrafo que seja longo o suficiente para ser
    significativo.

    Args:
        article_text: Texto limpo do artigo.
        max_chars: Número máximo de caracteres do resumo.

    Returns:
        Um pequeno trecho de texto adequado para prévias de resultados
        de busca.
    """
    if not article_text:
        return ""

    paragraphs = [
        p.strip() for p in article_text.split("\n\n") if len(p.strip()) > 40
    ]

    if not paragraphs:
        return article_text[:max_chars]

    snippet = paragraphs[0]
    if len(snippet) > max_chars:
        # Cut at the last space before the limit
        snippet = snippet[:max_chars].rsplit(" ", 1)[0] + "…"

    return snippet
