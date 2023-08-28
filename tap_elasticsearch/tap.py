"""tap-elasticsearch tap class."""

from __future__ import annotations

import requests
from requests.exceptions import ConnectionError
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_elasticsearch.client import TapelasticsearchStream


class Tapelasticsearch(Tap):
    """tap-elasticsearch tap class."""

    name = "tap-elasticsearch"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "page_size",
            th.IntegerType,
            description="The page size",
            default=1000,
        ),
        th.Property(
            "url_base",
            th.StringType,
            description="The base url of the elasticsearch instance",
            required=True,
        ),
        th.Property(
            "start_date",
            th.StringType,
            description="The start date",
        ),
        th.Property(
            "request_interval",
            th.NumberType,
            description="The interval between requests",
            default=0,
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        url_base = self.config.get("url_base", "")
        try:
            aliases = requests.get(url_base + "/_aliases", timeout=60).json()
        except ConnectionError as e:
            msg = "Could not connect to Elasticsearch instance."
            raise RuntimeError(msg) from e

        if "error" in aliases:
            raise RuntimeError(aliases)

        alias_names = []
        for v in aliases.values():
            if v["aliases"]:
                alias_names.extend(v["aliases"])
        # included_indices = self.config.get("included_indices", [])  # noqa: ERA001
        catalog_dict = {
            s["stream"]: s for s in self.input_catalog.to_dict().get("streams", {})
        }
        for alias in alias_names:
            try:
                if not catalog_dict[alias]:
                    continue
            except KeyError:
                continue
            stream = TapelasticsearchStream(
                tap=self,
                name=alias,
                schema=catalog_dict[alias]["schema"],
                path=f"/{alias}/_search",
            )
            stream.apply_catalog(self.catalog)
            yield stream


if __name__ == "__main__":
    Tapelasticsearch.cli()
