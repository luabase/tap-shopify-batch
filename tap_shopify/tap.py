"""Shopify tap class."""

from __future__ import annotations
import logging

from singer_sdk import Tap
from singer_sdk import typing as th
from functools import cached_property
from tap_shopify.gql_queries import schema_query, queries_query
from typing import Any, Iterable

import requests
import inflection

from tap_shopify.client_bulk import shopifyBulkStream
from tap_shopify.client_gql import shopifyGqlStream


class ShopifyStream(shopifyGqlStream, shopifyBulkStream):
    """Define base based on the type GraphQL or Bulk."""

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.config.get("bulk"):
            return shopifyBulkStream.parse_response(self, response)
        else:
            return shopifyGqlStream.parse_response(self, response)

    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        if self.config.get("bulk"):
            return shopifyBulkStream.query(self)
        else:
            return shopifyGqlStream.query(self)


class TapShopify(Tap):
    """Shopify tap class."""

    name = "tap-shopify"

    gql_types_in_schema = []

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service.",
        ),
        th.Property(
            "store",
            th.StringType,
            required=True,
            description="The shopify shop name.",
        ),
        th.Property(
            "api_version",
            th.StringType,
            default="2023-04",
            description="The version of the API to use.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync.",
        ),
        th.Property(
            "bulk",
            th.BooleanType,
            default=False,
            description="To use the bulk API or not.",
        ),
        th.Property(
            "ignore_access_denied",
            th.BooleanType,
            default=True,
            description="Ignores streams where access is denied, and prune subfields when access is denied.",
        ),
        th.Property(
            "ignore_deprecated",
            th.BooleanType,
            default=True,
            description="To ignore deprecated fields or not.",
        ),
    ).to_dict()

    def request_gql(self, query: str) -> requests.Response:
        """Make a request to the GraphQL endpoint and return the response."""
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.config["access_token"],
        }
        store = self.config["store"]
        api_version = self.config["api_version"]
        url = f"https://{store}.myshopify.com/admin/api/{api_version}/graphql.json"

        request_data = {"query": query}

        resp = requests.post(
            url=url,
            headers=headers,
            json=request_data,
        )

        resp.raise_for_status()

        return resp

    @cached_property
    def schema_gql(self) -> dict:
        """Return the schema for the stream."""
        resp = self.request_gql(schema_query)
        return resp.json()["data"]["__schema"]["types"]

    def filter_queries(self, query):
        args = [a["name"] for a in query["args"]]
        return "first" in args and "query" in args

    @cached_property
    def queries_gql(self) -> dict:
        """Return the schema for the stream."""

        resp = self.request_gql(queries_query)
        jresp = resp.json()
        queries = jresp["data"]["__schema"]["queryType"]["fields"]
        return [q for q in queries if self.filter_queries(q)]

    def extract_gql_node(self, query: dict) -> dict:
        query_fields = query["type"]["ofType"]["fields"]
        return next((f for f in query_fields if f["name"] == "nodes"), None)

    def get_gql_query_type(self, node: dict) -> str:
        return node["type"]["ofType"]["ofType"]["ofType"]["name"]

    def get_type_fields(self, gql_type: str) -> list[dict]:
        type_def = next(s for s in self.schema_gql if s["name"] == gql_type)

        filtered_fields = [
            f
            for f in type_def["fields"]
            if f["type"]["kind"] == "NON_NULL"
            and f["type"]["ofType"]["kind"] == "SCALAR"
        ]

        return {f["name"]: f["type"]["ofType"] for f in filtered_fields}

    def discover_streams(self) -> list[ShopifyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        queries = self.queries_gql

        # Ordered by priority.
        incremental_fields = [
            "updatedAt",
            "editedAt",
            "lastEditDate",
            "occurredAt",
            "createdAt",
            "startedAt",
            "processedAt",
        ]

        streams = []

        for query in queries:
            node = self.extract_gql_node(query)
            if not node:
                continue

            gql_type = self.get_gql_query_type(node)
            fields_def = self.get_type_fields(gql_type)
            # logging.info(f"Fields for {gql_type}: {fields_def}")

            # Get the primary key
            pk = [k for k, v in fields_def.items() if v["name"] == "ID"]
            if not pk:
                continue

            # Get the replication key
            date_fields = [k for k, v in fields_def.items() if v["name"] == "DateTime"]
            rk = next((i for i in incremental_fields if i in date_fields), None)

            self.gql_types_in_schema.append(gql_type)

            type_def = dict(
                name=inflection.underscore(query["name"]),
                query_name=query["name"],
                gql_type=gql_type,
                primary_keys=pk,
                replication_key=rk,
            )
            streams.append(type_def)

        for type_def in streams:
            yield type(type_def["name"], (ShopifyStream,), type_def)(tap=self)


if __name__ == "__main__":
    TapShopify.cli()
