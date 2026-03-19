"""
# Esta parte do projeto coleta os dados principais dos artigos

Spider Scrapy para o The Guardian (edição da Austrália).

Rastreia a página inicial e segue links de artigos, extraindo o HTML bruto
para a etapa posterior de limpeza. Respeita o robots.txt e limita a taxa
de requisições.

Uso:
    scrapy crawl guardian
    scrapy crawl guardian -a max_pages=100
    scrapy crawl guardian -a start_url=https://www.theguardian.com/uk
"""

import logging
from datetime import datetime, timezone

import scrapy

from news_crawler.items import ArticleItem

logger = logging.getLogger(__name__)


class GuardianSpider(scrapy.Spider):
    """Rastreia o The Guardian em busca de artigos de notícias."""

    name = "guardian"
    allowed_domains = ["theguardian.com"]

    # Configurável por argumentos de linha de comando (-a) ou settings.yaml
    start_url = "https://www.theguardian.com/au"
    max_pages = 50

    def __init__(self, start_url=None, max_pages=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if start_url:
            self.start_url = start_url
        if max_pages:
            self.max_pages = int(max_pages)
        self._pages_crawled = 0

    def start_requests(self):
        """Inicia o rastreamento a partir da URL inicial configurada."""
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response):
        """
        Interpreta a página inicial ou de seção.

        Identifica links de artigos e os segue. As URLs de artigos do Guardian
        geralmente contêm um padrão de data como /2026/mar/19/.
        """
        # Extrai links de artigos da página
        article_links = response.css('a[href*="/202"]::attr(href)').getall()

        # Remove duplicados e filtra URLs parecidas com artigos
        seen = set()
        for link in article_links:
            url = response.urljoin(link)

            # Pula páginas que não são artigos (ex: tags, perfis)
            if any(skip in url for skip in ["/profile/", "/tone/", "/info/"]):
                continue

            if url not in seen and self._pages_crawled < self.max_pages:
                seen.add(url)
                self._pages_crawled += 1
                yield scrapy.Request(url=url, callback=self.parse_article)

        # Segue links de seções para ampliar a cobertura (um nível de profundidade)
        section_links = response.css(
            'a[data-link-name="nav3 item"]::attr(href)'
        ).getall()
        for link in section_links:
            if self._pages_crawled < self.max_pages:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.parse,
                )

    def parse_article(self, response):
        """
        Interpreta uma página individual de artigo.

        Extrai metadados estruturados (título, autor, data) a partir do
        HTML semântico da página e das meta tags. O HTML bruto do corpo
        também é capturado para a etapa de limpeza no pipeline.
        """
        item = ArticleItem()

        # --- Título ---
        item["headline"] = (
            response.css("h1::text").get("")
            or response.css('meta[property="og:title"]::attr(content)').get("")
        ).strip()

        # --- Autor ---
        # O Guardian usa <a rel="author"> e também meta tags estruturadas
        item["author"] = (
            response.css('a[rel="author"]::text').get("")
            or response.css('meta[name="author"]::attr(content)').get("")
        ).strip()

        # --- Data de publicação ---
        # Dá preferência ao elemento <time>, que é legível por máquina
        item["published_date"] = (
            response.css("time::attr(datetime)").get("")
            or response.css(
                'meta[property="article:published_time"]::attr(content)'
            ).get("")
        ).strip()

        # --- Seção ---
        item["section"] = response.css(
            'meta[property="article:section"]::attr(content)'
        ).get("").strip()

        # --- URLs e metadados ---
        item["article_url"] = response.url
        item["source"] = "theguardian.com"
        item["crawled_at"] = datetime.now(timezone.utc).isoformat()

        # --- HTML bruto para o pipeline de limpeza ---
        item["raw_html"] = response.text

        # Só retorna itens que realmente parecem artigos
        if item["headline"]:
            logger.info("Collected article: %s", item["headline"][:80])
            yield item
        else:
            logger.debug("Skipped non-article page: %s", response.url)
