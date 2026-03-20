"""
# Esta parte do projeto limpa os dados dos artigos e prepara para salvar
# no BigQuery

Pipelines de itens do Scrapy.

Etapas do pipeline:
    1. CleansingPipeline — Extrai texto limpo do HTML bruto
    2. BigQueryPipeline — Salva os artigos limpos no BigQuery

Os pipelines são configurados em settings.py e executados em ordem numérica.
"""

import logging
from pathlib import Path

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account

from news_crawler.cleanser import cleanse_article, extract_summary

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Funções auxiliares
# ──────────────────────────────────────────────


def _load_config() -> dict:
    """Carrega o arquivo settings.yaml da pasta config."""
    config_path = (
        Path(__file__).resolve().parent.parent / "config" / "settings.yaml"
    )
    if not config_path.exists():
        logger.warning(
            "Arquivo de configuração não encontrado em %s — usando padrões",
            config_path,
        )
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


# ──────────────────────────────────────────────
# Pipeline 1: Limpeza do conteúdo
# ──────────────────────────────────────────────


class CleansingPipeline:
    """
    Limpa o HTML bruto do artigo e transforma em texto simples.

    Substitui o campo `raw_html` por um campo `article_text`
    e também gera um `snippet` para prévia nos resultados de busca.
    """

    def process_item(self, item, spider):
        raw_html = item.get("raw_html", "")

        # Executa a limpeza do conteúdo
        clean_text = cleanse_article(raw_html)
        item["article_text"] = clean_text
        item["snippet"] = extract_summary(clean_text)

        # Remove raw_html — não é mais necessário e aumentaria o armazenamento
        item.pop("raw_html", None)

        if not clean_text:
            logger.warning(
                "Texto do artigo ficou vazio após a limpeza: %s",
                item.get("article_url"),
            )

        return item


# ──────────────────────────────────────────────
# Pipeline 2: Armazenamento no BigQuery
# ──────────────────────────────────────────────


class BigQueryPipeline:
    """
    Salva os artigos limpos em uma tabela do BigQuery.

    Usa inserções em streaming para disponibilizar os dados quase em tempo
    real.
    Evita duplicidade usando a article_url para não salvar o mesmo artigo duas
    vezes.
    """

    def __init__(self):
        self.client = None
        self.table_ref = None
        self._seen_urls = set()

    def open_spider(self, spider):
        """Inicializa o cliente do BigQuery quando o spider começa."""
        config = _load_config().get("gcp", {})

        project_id = config.get("project_id")
        dataset = config.get("dataset", "news_articles")
        table = config.get("table", "articles")
        creds_path = config.get("credentials_path")

        if not project_id:
            logger.error(
                "GCP project_id não definido em config/settings.yaml — "
                "o salvamento no BigQuery será ignorado."
            )
            return

        # Faz a autenticação
        if creds_path:
            credentials = (
                service_account.Credentials.from_service_account_file(
                    creds_path
                )
            )
            self.client = bigquery.Client(
                project=project_id, credentials=credentials
            )
        else:
            # Usa Application Default Credentials como alternativa
            self.client = bigquery.Client(project=project_id)

        self.table_ref = f"{project_id}.{dataset}.{table}"
        logger.info(
            "Pipeline do BigQuery pronto — tabela de destino: %s",
            self.table_ref,
        )

    def process_item(self, item, spider):
        """Insere uma linha de artigo no BigQuery."""
        if not self.client:
            return item

        url = item.get("article_url", "")
        if url in self._seen_urls:
            logger.debug("URL duplicada ignorada: %s", url)
            return item
        self._seen_urls.add(url)

        row = {
            "headline": item.get("headline", ""),
            "author": item.get("author", ""),
            "published_date": item.get("published_date", ""),
            "article_text": item.get("article_text", ""),
            "snippet": item.get("snippet", ""),
            "article_url": url,
            "section": item.get("section", ""),
            "source": item.get("source", ""),
            "crawled_at": item.get("crawled_at", ""),
        }

        errors = self.client.insert_rows_json(self.table_ref, [row])
        if errors:
            logger.error(
                "Erro ao inserir no BigQuery para %s: %s", url, errors
            )
        else:
            logger.info("Salvo no BigQuery: %s", item.get("headline", "")[:60])

        return item

    def close_spider(self, spider):
        """Mostra um resumo quando o spider termina."""
        if self.client:
            logger.info(
                "Pipeline do BigQuery finalizado — %d artigos únicos"
                "processados",
                len(self._seen_urls),
            )
