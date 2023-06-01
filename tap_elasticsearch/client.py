"""REST client handling, including tap-elasticsearchStream base class."""

from __future__ import annotations

from math import ceil
from pathlib import Path
from typing import Callable, Iterable

import requests
from requests import Response
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CustomPaginator(BaseAPIPaginator):
    """custom paginator."""

    def get_next(self, response: requests.Response) -> str | None:
        data = response.json()["hits"]["hits"]
        length = len(data)
        return data[length - 1]["sort"]

    def has_more(self, response: Response) -> bool:
        """Return True if there are more pages to process."""
        page_count = self.count
        total_results = response.json()["hits"]["total"]
        total_pages = ceil(total_results / 1000)
        return page_count < total_pages


class TapelasticsearchStream(RESTStream):
    """tap-elasticsearch stream class."""

    date_column = "date"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("url_base")

    records_jsonpath = "$.hits.hits[*]"  # Or override `parse_response`.

    # # Set this value or override `get_new_paginator`.

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> CustomPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return CustomPaginator(None)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

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
        # TODO: Delete this method if not needed.
        return row
