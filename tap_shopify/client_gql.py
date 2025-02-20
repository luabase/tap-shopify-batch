"""GraphQL client handling, including shopify-betaStream base class."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import requests  # noqa: TCH002
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_shopify.client import ShopifyStream
from tap_shopify.gql_queries import query_incremental


class shopifyGqlStream(ShopifyStream):
    """shopify stream class."""

    # @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        # This is for supporting the single object like shop endpoint
        # if not self.replication_key and not self.single_object_params:
        #     base_query = simple_query
        # elif self.single_object_params:
        #     base_query = simple_query_incremental
        # else:
        #     base_query = query_incremental

        base_query = query_incremental

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)
        additional_args = ", " + ", ".join(self.additional_arguments)
        query = query.replace("__additional_args__", additional_args)

        return query

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}

        if next_page_token:
            params.update(next_page_token)
        else:
            params["first"] = 1
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                params["filter"] = f"updated_at:>{date}"
        if self.single_object_params:
            params = self.single_object_params
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the GraphQL API request."""
        params = self.get_url_params(context, next_page_token)
        query = self.query.lstrip()
        request_data = {
            "query": query,
            "variables": params,
        }
        self.saved_context = context
        self.logger.debug(f"Attempting query:\n{query}, with params {params}, with context {context}, and next_page_token {next_page_token}")
        return request_data
    
    def ignore_path(self, path):
        self.schema = delete_schema_item(self.schema, path[-1])
        # Delete the attribute so we regen the query.
        del self.query

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # if self.replication_key:
        #     json_path = f"$.data.{self.query_name}.edges[*].node"
        # else:
        #     json_path = f"$.data.{self.query_name}"
        
        # Not sure why replication_key matters here. The data is still stored under node
        # See locations stream.
        json_path = f"$.data.{self.query_name}.edges[*].node"
        response = response.json()

        errors = response.get("errors")
        if errors:
            # Don't handle and crash.
            if not self.config.get("ignore_access_denied"):
                raise Exception(response["errors"])

            # Handle errors!
            for error in errors:
                code = error.get("extensions", {}).get('code', None)
                if code == 'ACCESS_DENIED':
                    # Try again!
                    field = error['path'][-1]
                    
                    # This failed on the top level, which means we don't have access to the endpoint.
                    if len(error['path']) == 1:
                        self.logger.error(f"Skipping stream {self.name} due to access denied error: {error['message']}")
                        return None

                    # Otherwise it's a subfield we can ignore.
                    self.logger.error(f"Ignoring access denied field: {field}. Error: {error['message']}. Updating schema...")
                    self.ignore_path(error['path'])
                    yield from self.request_records(self.saved_context)
                elif code == 'missingRequiredArguments':
                    self.logger.error(f"Missing required arguments for stream: {self.name}, {error['message']}")
                    return None
                elif error.get('path', None):
                    self.logger.error(f"Not an access denied error. But there is a fauly path: {error['path']}. Error: {error['message']}. Ignoring path and updating schema...")
                    if len(error['path']) == 1:
                        self.logger.error("Ignoring the entire stream since the path specifies the top level request.")
                        return None
                    self.ignore_path(error['path'])
                    yield from self.request_records(self.saved_context)
                else:
                    raise Exception(response["errors"])

        yield from extract_jsonpath(json_path, input=response)


def delete_schema_item(d, target_key):
    if not isinstance(d, dict):
        return d
    if target_key in d:
        del d[target_key]
    for key in d:
        if key == 'required' and target_key in d[key]:
            d[key].remove(target_key)
        else:
            d[key] = delete_schema_item(d[key], target_key)
    return d
