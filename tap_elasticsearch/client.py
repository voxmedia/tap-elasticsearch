"""REST client handling, including tap-elasticsearchStream base class."""

from __future__ import annotations

import re
import typing as t
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from requests import Response
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def sanitize_keys(value):
    if isinstance(value, dict):
        return {
            replace_special_chars(key): sanitize_keys(val) for key, val in value.items()
        }
    if isinstance(value, list):
        return [sanitize_keys(val) for val in value]
    return value


def replace_special_chars(key):
    return re.sub("[ \\-&\\/]", "_", key)


class CustomPaginator(BaseAPIPaginator):
    """custom paginator."""

    def __init__(self, start_value: t.TPageToken, page_size: int) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
        """
        self._value: t.TPageToken = start_value
        self._page_size = page_size
        self._page_count = 0
        self._finished = False
        self._last_seen_record: dict | None = None

    def get_next(self, response: requests.Response) -> str | None:
        data = response.json()["hits"]["hits"]
        length = len(data)
        try:
            return data[length - 1]["sort"]
        except KeyError:
            # print(data[length - 1])  # noqa: ERA001
            # print(data[length - 2])  # noqa: ERA001
            return None

    def has_more(self, response: Response) -> bool:
        """Return True if there are more pages to process."""
        len_page_results = len(response.json()["hits"]["hits"])
        return self._page_size == len_page_results


class TapelasticsearchStream(RESTStream):
    """tap-elasticsearch stream class."""

    primary_keys = ["_id"]

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
        return CustomPaginator(None, self.config.get("page_size"))

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
        params: dict = {"size": self.config.get("page_size", 1000)}
        if self.replication_method == "FULL_TABLE":
            params["query"] = {
                "match_all": {},
            }
            params["sort"] = [{self.primary_keys[0]: "asc"}]
        elif self.replication_method == "INCREMENTAL":
            starting_replication_value = self.get_starting_replication_key_value(
                context,
            )

            params["query"] = {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                self.replication_key: {
                                    "gte": starting_replication_value,
                                },
                            },
                        },
                    ],
                },
            }
            params["sort"] = [{self.replication_key: "asc"}]
        if next_page_token:
            params["search_after"] = next_page_token
        return params

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
        if self.replication_method == "INCREMENTAL":
            row[self.replication_key] = row["_source"].pop(self.replication_key)
        row["_source"] = sanitize_keys(row["_source"])
        return row
