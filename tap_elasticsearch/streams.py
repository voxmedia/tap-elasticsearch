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
    replication_method = "INCREMENTAL"
    replication_key = "date"
    is_sorted = True

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
        starting_replication_value = self.get_starting_replication_key_value(context)
        date_filter = (
            starting_replication_value
            if starting_replication_value
            else self.config.get("start_date")
        )
        params: dict = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "date": {"gte": date_filter},
                            },
                        },
                    ],
                },
            },
            "sort": [{"date": "asc"}],
            "size": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["search_after"] = next_page_token
        return params

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row["date"] = row["_source"].pop("date")
        return row


class ContentStream(ArticlesStream):
    """Define custom stream."""

    name = "content"
    path = "/published-content/_search"


class ProductsStream(TapelasticsearchStream):
    """Define custom stream."""

    name = "products"
    path = "/published-products/_search"
    primary_keys = ["_id"]
    schema_filepath = SCHEMAS_DIR / "product.json"
    replication_method = "INCREMENTAL"
    replication_key = "creationDate"
    is_sorted = True

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
        starting_replication_value = self.get_starting_replication_key_value(context)
        date_filter = (
            starting_replication_value
            if starting_replication_value
            else pendulum.parse(self.config.get("start_date")).int_timestamp
        )
        params: dict = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "creationDate": {"gte": date_filter},
                            },
                        },
                    ],
                },
            },
            "sort": [{"creationDate": "asc"}],
            "size": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["search_after"] = next_page_token
        return params
