"""REST client handling, including ElasticsearchStream base class."""

from pathlib import Path

from elasticsearch import Elasticsearch
from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ElasticsearchStream(Stream):
    """Elasticsearch stream class."""

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.es = Elasticsearch(
            [
                {
                    "host": self.config.get("host"),
                    "port": 443,
                    "use_ssl": True,
                },
            ],
        )


# """REST client handling, including elasticsearchStream base class."""


# class elasticsearchStream(RESTStream):
#     """elasticsearch stream class."""

#     @property
#     def url_base(self) -> str:
#         """Return the API URL root, configurable via tap settings."""
#         # TODO: hardcode a value here, or retrieve it from self.config


#     # Set this value or override `get_new_paginator`.

#     @property
#     def http_headers(self) -> dict:
#         """Return the http headers needed.

#         Returns:
#             A dictionary of HTTP headers.
#         """
#         if "user_agent" in self.config:
#         # If not using an authenticator, you may also provide inline auth headers:
#         # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001

#     def get_new_paginator(self) -> BaseAPIPaginator:
#         """Create a new pagination helper instance.

#         If the source API can make use of the `next_page_token_jsonpath`
#         attribute, or it contains a `X-Next-Page` header in the response
#         then you can remove this method.

#         If you need custom pagination that uses page numbers, "next" links, or
#         other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

#         Returns:
#             A pagination helper instance.
#         """

#     def get_url_params(
#         self,
#         context: dict | None,
#         next_page_token: Any | None,
#     ) -> dict[str, Any]:
#         """Return a dictionary of values to be used in URL parameterization.

#         Args:
#             context: The stream context.
#             next_page_token: The next page index or value.

#         Returns:
#             A dictionary of URL query parameters.
#         """
#         if next_page_token:
#         if self.replication_key:

#     def prepare_request_payload(
#         self,
#         context: dict | None,
#         next_page_token: Any | None,
#     ) -> dict | None:
#         """Prepare the data payload for the REST API request.

#         By default, no payload will be sent (return None).

#         Args:
#             context: The stream context.
#             next_page_token: The next page index or value.

#         Returns:
#             A dictionary with the JSON body for a POST requests.
#         """
#         # TODO: Delete this method if no payload is required. (Most REST APIs.)

#     def parse_response(self, response: requests.Response) -> Iterable[dict]:
#         """Parse the response and return an iterator of result records.

#         Args:
#             response: The HTTP ``requests.Response`` object.

#         Yields:
#             Each record from the source.
#         """
#         # TODO: Parse response body and return a set of records.

#     def post_process(
#         self,
#         row: dict,
#     ) -> dict | None:
#         """As needed, append or transform raw data to match expected structure.

#         Args:
#             row: An individual record from the stream.
#             context: The stream context.

#         Returns:
#             The updated record dictionary, or ``None`` to skip the record.
#         """
#         # TODO: Delete this method if not needed.
