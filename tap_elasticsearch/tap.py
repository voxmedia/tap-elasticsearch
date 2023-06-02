"""tap-elasticsearch tap class."""

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_elasticsearch.streams import ArticlesStream, ContentStream, ProductsStream

STREAM_TYPES = [
    ArticlesStream,
    ContentStream,
    ProductsStream,
]


class Tapelasticsearch(Tap):
    """tap-elasticsearch tap class."""

    name = "tap-elasticsearch"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "page_size",
            th.IntegerType,
            description="The page size",
        ),
        th.Property(
            "url_base",
            th.StringType,
            description="The base url of the elasticsearch instance",
        ),
        th.Property(
            "start_date",
            th.StringType,
            description="The start date",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapelasticsearch.cli()
