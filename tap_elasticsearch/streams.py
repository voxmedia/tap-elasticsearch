"""Stream type classes for tap-elasticsearch."""

from pathlib import Path
from typing import Iterable, Optional

import requests
from elasticsearch.helpers import scan

from tap_elasticsearch.client import ElasticsearchStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class ArticlesStream(ElasticsearchStream):
    """Define custom stream."""

    name = "articles"
    primary_keys = ["_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "article.json"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        resp = scan(
            self.es,
            index="published-articles",
            doc_type="_doc",
            preserve_order=True,
            query={"query": {"match_all": {}}},
        )
        resp = requests.get(
            "http://bastion.nymetro.com/es/sites-prd/published-articles/_search?size=100",
        ).json()
        yield from resp["hits"]["hits"]


class ProductsStream(ElasticsearchStream):
    """Define custom stream."""

    name = "products"
    primary_keys = ["_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "product.json"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        resp = scan(
            self.es,
            index="published-products",
            doc_type="_doc",
            preserve_order=True,
            query={"query": {"match_all": {}}},
        )
        resp = requests.get(
            "http://bastion.nymetro.com/es/sites-prd/published-products/_search?size=100",
        ).json()
        yield from resp["hits"]["hits"]
