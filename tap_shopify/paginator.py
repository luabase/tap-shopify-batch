import math
from functools import cached_property
import sys
from time import sleep

import requests
from requests import Response
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator


class ShopifyPaginator(BaseAPIPaginator):
    """shopify paginator class."""

    def __init__(self, logger, *args, **kwargs) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
        """
        self._logger = logger
        self._page_count = 0
        self._page_size = 0
        self._finished = False
        self._query_cost = None
        self._available_points = None
        self._restore_rate = None
        self._max_points = 10000
        self._single_query_max_cost_limt = 1000
        super().__init__(None, *args, **kwargs)

    @property
    def page_size(self) -> int:
        """Return the page size for the stream."""
        if not self._available_points or not self._query_cost:
            self._page_size = 1
            return 1

        # We want to leave at least 1/4 of the max points, or 2 single queries worth of points.
        # 1/4 points so we leave room for others, it's really all about the restore rate for big jobs.
        # 2 single queries worth of points so we can send spend at least 1 in the next cycle
        min_leftover_points = max(self._max_points // 4, (2 * self._single_query_max_cost_limt))
        if self._available_points < min_leftover_points:
            # Wait until we hit min.
            sleep_time = math.ceil((min_leftover_points - self._available_points) / self._restore_rate)
            self._logger.info(f"Sleeping for {sleep_time} seconds to restore points. {self._available_points} points left, restoring to {min_leftover_points}")
            sleep(sleep_time)

        # Send one max query.
        est_per_query_cost = self._query_cost / self._page_size
        new_page_size = self._single_query_max_cost_limt / est_per_query_cost
        self._page_size = 250 if new_page_size > 250 else int(new_page_size)
        self._logger.debug(f"Next page size {self._page_size}")
        return self._page_size

    def query_name(self, response_json) -> str:
        """Set or return the GraphQL query name."""
        return list(response_json.get("data"))[0]

    # Use get_next to handle finished.
    def has_more(self, _: requests.Response) -> bool:
        return True

    def get_next(self, response: requests.Response):
        """Get the next pagination value."""
        response_json = response.json()

        # Request failed. No next page.
        # Some requests may fail, like for an bad endpoint / schema.
        if response_json.get('errors', None):
            return None

        query_name = self.query_name(response_json)

        cost = response_json["extensions"].get("cost")
        self._logger.debug(f"Query cost profile {cost}")

        self._query_cost = cost.get("requestedQueryCost")
        self._available_points = cost["throttleStatus"].get("currentlyAvailable")
        self._restore_rate = cost["throttleStatus"].get("restoreRate")
        self._max_points = cost["throttleStatus"].get("maximumAvailable")

        has_next_json_path = f"$.data.{query_name}.pageInfo.hasNextPage"
        has_next = next(extract_jsonpath(has_next_json_path, response_json))

        if has_next:
            cursor_json_path = f"$.data.{query_name}.pageInfo.endCursor"
            all_matches = extract_jsonpath(cursor_json_path, response_json)
            return next(all_matches, None)

        return None

    @property
    def current_value(self):
        """Get the current pagination value.

        Returns:
            Current page value.
        """
        return dict(first=self.page_size, after=self._value)
