"""GraphQL client handling, including shopify-betaStream base class."""

from datetime import datetime
from time import sleep
from typing import Any, Iterable, Optional, cast

import requests
import simplejson
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import SinglePagePaginator

from tap_shopify.client import ShopifyStream
from tap_shopify.exceptions import InvalidOperation, OperationFailed
from tap_shopify.gql_queries import bulk_query, bulk_query_status, simple_query


class shopifyBulkStream(ShopifyStream):
    """shopify stream class."""

    # @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        if self.name == "shop":
            base_query = simple_query
        else:
            base_query = bulk_query

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)

        filters = self.filters

        # No filters should also exclude the parens
        if filters:
            query = query.replace("__filters__", "(" + filters + ")")
        else:
            query = query.replace("__filters__", "")

        return query

    @property
    def filters(self):
        """Return a dictionary of values to be used in URL parameterization."""
        filters = []
        if self.additional_arguments:
            filters.extend(self.additional_arguments)
        if self.replication_key:
            start_date = self.get_starting_timestamp({})
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                filters.append(f'query: "updated_at:>{date}"')
        return ",".join(filters)

    def get_operation_status(self):
        headers = self.http_headers
        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=self.rest_method,
                    url=self.get_url({}),
                    headers=headers,
                    json=dict(query=bulk_query_status, variables={}),
                ),
            ),
        )

        decorated_request = self.request_decorator(self._request)
        response = decorated_request(request, {})

        return response

    def check_status(self, operation_id, sleep_time=10, timeout=1800):
        status_jsonpath = "$.data.currentBulkOperation"
        start = datetime.now().timestamp()

        while datetime.now().timestamp() < (start + timeout):
            status_response = self.get_operation_status()
            status = next(
                extract_jsonpath(status_jsonpath, input=status_response.json())
            )
            self.logger.info("Poll status...")
            self.logger.info(status)
            if status["id"] != operation_id:
                raise InvalidOperation(
                    "The current job was not triggered by the process, "
                    "check if other service is using the Bulk API"
                )
            if status["url"]:
                return status["url"]
            if status["status"] == "COMPLETED":
                if status["objectCount"] == '0':
                    self.logger.info("No data found for stream: %s", self.name)
                    return None
                raise InvalidOperation(f"Objects for stream {self.name} is not empty, but no download url was provided.")
            elif status["status"] == "FAILED":
                # Can't access, skip.
                if status['errorCode'] == "ACCESS_DENIED":
                    return None
                if status['errorCode'] == "INTERNAL_SERVER_ERROR":
                    return None
                raise InvalidOperation(f"Job failed: {status['errorCode']}, {status}")
            sleep(sleep_time)
        raise OperationFailed("Job Timeout")

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        operation_id_jsonpath = "$.data.bulkOperationRunQuery.bulkOperation.id"
        request_response = response.json()

        self.logger.info(f"Request response: {request_response}")

        operation_id = next(
            extract_jsonpath(operation_id_jsonpath, input=request_response)
        )

        url = self.check_status(operation_id)
        if not url:
            return []

        output = requests.get(url, stream=True)

        for line in output.iter_lines():
            yield simplejson.loads(line)
