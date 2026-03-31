"""
Integration tests for tap-clubspeed pagination with mocked HTTP.

Patches tap_clubspeed.clubspeed.requests.get to simulate multi-page responses.
Verifies the tap keeps requesting pages until an empty page is received.
"""
import unittest
from unittest.mock import patch

try:
    from .base import ClubspeedBaseTest, MockResponse
except ImportError:
    from base import ClubspeedBaseTest, MockResponse

from tap_clubspeed.clubspeed import Clubspeed


class ClubspeedPaginationTest(ClubspeedBaseTest, unittest.TestCase):
    """Verify tap-clubspeed handles multi-page API responses correctly."""

    # Patch target: the requests.get used inside clubspeed.py
    PATCH_TARGET = "tap_clubspeed.clubspeed.requests.get"

    def _make_pages(self, stream_name, key=None, page_sizes=None):
        """
        Build a list of MockResponse objects for the given page sizes.
        The final response is always empty to terminate pagination.
        """
        if page_sizes is None:
            page_sizes = [100, 50]
        pages = []
        for size in page_sizes:
            records = [
                self._generate_stream_record(stream_name)
                for _ in range(size)
            ]
            pages.append(self.make_get_response(records, key))
        # Terminal empty page
        pages.append(self.make_get_response([] if key is None else [], key))
        return pages

    # ── _get_response pagination loop ────────────────────────────────────

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_two_pages_then_empty_fetches_all_records(self, mock_get):
        """_get_response fetches page 1, page 2, then stops on empty page."""
        page1 = [{"checkId": i, "closedDate": "2024-01-01"} for i in range(100)]
        page2 = [{"checkId": i, "closedDate": "2024-01-02"} for i in range(100, 150)]

        mock_get.side_effect = [
            MockResponse(page1),
            MockResponse(page2),
            MockResponse([]),  # terminal
        ]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("checks")
        results = list(client._get_response(endpoint))

        self.assertEqual(150, len(results))
        self.assertEqual(3, mock_get.call_count)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_single_page_stops_after_one_empty(self, mock_get):
        """_get_response stops immediately when first page is empty."""
        mock_get.side_effect = [MockResponse([])]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("checks")
        results = list(client._get_response(endpoint))

        self.assertEqual([], results)
        self.assertEqual(1, mock_get.call_count)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_keyed_response_pagination(self, mock_get):
        """Paginated response with an envelope key yields records from the key."""
        page1 = [{"onlineBookingsId": 1}, {"onlineBookingsId": 2}]
        page2 = [{"onlineBookingsId": 3}]

        mock_get.side_effect = [
            MockResponse({"bookings": page1}),
            MockResponse({"bookings": page2}),
            MockResponse({"bookings": []}),
        ]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("booking")
        results = list(client._get_response(endpoint, key="bookings"))

        self.assertEqual(3, len(results))
        self.assertEqual(3, mock_get.call_count)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_page_numbers_increment_correctly(self, mock_get):
        """The URL page parameter increments by 1 on each call."""
        mock_get.side_effect = [
            MockResponse([{"checkId": 1}]),
            MockResponse([{"checkId": 2}]),
            MockResponse([]),
        ]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("checks")
        list(client._get_response(endpoint))

        urls = [call[0][0] for call in mock_get.call_args_list]
        self.assertIn("page=0", urls[0])
        self.assertIn("page=1", urls[1])
        self.assertIn("page=2", urls[2])

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_limit_is_included_in_url(self, mock_get):
        """The URL always includes the limit parameter."""
        mock_get.side_effect = [MockResponse([])]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("checks")
        list(client._get_response(endpoint))

        called_url = mock_get.call_args[0][0]
        self.assertIn(f"limit={client._limit}", called_url)

    # ── Stream-level pagination (via Clubspeed methods) ──────────────────

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_checks_paginates_across_multiple_pages(self, mock_get):
        """client.checks() returns records across multiple pages."""
        page1 = [{"checkId": i} for i in range(100)]
        page2 = [{"checkId": i} for i in range(100, 120)]

        # checks() calls _get_response(endpoint, key='checks'), so wrap in dict
        mock_get.side_effect = [
            MockResponse({"checks": page1}),
            MockResponse({"checks": page2}),
            MockResponse({"checks": []}),
        ]

        client = Clubspeed("myclub", "secret")
        results = list(client.checks())

        self.assertEqual(120, len(results))

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_customers_paginates_and_yields_all(self, mock_get):
        """client.customers() yields all records across pages."""
        page1 = [{"customerId": i} for i in range(100)]
        page2 = [{"customerId": i} for i in range(100, 115)]

        mock_get.side_effect = [
            MockResponse(page1),
            MockResponse(page2),
            MockResponse([]),
        ]

        client = Clubspeed("myclub", "secret")
        results = list(client.customers())

        self.assertEqual(115, len(results))

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_500_error_during_pagination_is_ignored_and_continues(self, mock_get):
        """IgnoreHttpException on a page is swallowed and pagination continues."""
        from tap_clubspeed.clubspeed import IgnoreHttpException

        # Page 1 returns 500, page 2 returns data, page 3 is empty
        mock_resp_500 = MockResponse([], 500)
        mock_resp_500.raise_for_status = lambda: None  # _get checks status == 500 directly

        mock_get.side_effect = [
            mock_resp_500,  # 500 → IgnoreHttpException
            MockResponse([{"checkId": 42}]),
            MockResponse([]),
        ]

        client = Clubspeed("myclub", "secret")
        endpoint = client._construct_endpoint("checks")
        # _get raises IgnoreHttpException on 500 before raise_for_status
        # _get_response catches it and continues, so next page should still be fetched
        results = list(client._get_response(endpoint))

        self.assertIn({"checkId": 42}, results)


if __name__ == "__main__":
    unittest.main()
