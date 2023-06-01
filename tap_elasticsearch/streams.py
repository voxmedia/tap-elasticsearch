"""Stream type classes for tap-elasticsearch."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pendulum

from tap_elasticsearch.client import TapelasticsearchStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ArticlesStream(TapelasticsearchStream):
    """Define custom stream."""

    name = "articles"
    path = "/published-articles/_search"
    primary_keys = ["_id"]
    schema_filepath = SCHEMAS_DIR / "article.json"
    # replication_method = "INCREMENTAL"  # noqa: ERA001
    # replication_key = "_source.date"  # noqa: ERA001

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        params: dict = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "date": {"gte": self.config.get("start_date")},
                            },
                        },
                    ],
                },
            },
            "sort": [{"date": "desc"}],
            "size": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["search_after"] = next_page_token
        return params


class ProductsStream(TapelasticsearchStream):
    """Define custom stream."""

    name = "products"
    path = "/published-products/_search"
    primary_keys = ["_id"]
    schema_filepath = SCHEMAS_DIR / "product.json"

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        start_date = self.config.get("start_date")
        start_timestamp = pendulum.parse(start_date).int_timestamp
        params: dict = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "creationDate": {"gte": start_timestamp},
                            },
                        },
                    ],
                },
            },
            "sort": [{"creationDate": "desc"}],
            "size": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["search_after"] = next_page_token
        return params
