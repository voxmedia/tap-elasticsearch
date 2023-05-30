"""Elasticsearch tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_elasticsearch.streams import (
    ArticlesStream,
    ProductsStream,
)

STREAM_TYPES = [
    ArticlesStream,
    ProductsStream,
]


class TapElasticsearch(Tap):
    """Elasticsearch tap class."""

    name = "tap-elasticsearch"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="Your Elasticsearch host address.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
