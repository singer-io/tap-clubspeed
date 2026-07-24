"""
Unit tests for tap_clubspeed.clubspeed.Clubspeed HTTP client.
"""
import unittest
from unittest.mock import patch, MagicMock

import requests

from tap_clubspeed.clubspeed import Clubspeed, IgnoreHttpException


def _make_response(status_code=200, json_body=None):
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    if status_code >= 400:
        http_err = requests.exceptions.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_err
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestConstructEndpoint(unittest.TestCase):
    """_construct_endpoint builds the correct URL."""

    def setUp(self):
        self.client = Clubspeed("myclub", "secret123")

    def test_correct_url_structure(self):
        """URL includes protocol, subdomain, domain, api_prefix, path and key."""
        expected = ("https://myclub.clubspeedtiming.com/"
                    "api/index.php/payments.json?key=secret123")
        self.assertEqual(expected, self.client._construct_endpoint("payments"))

    def test_different_paths(self):
        """Different path names produce different endpoints."""
        ep_a = self.client._construct_endpoint("customers")
        ep_b = self.client._construct_endpoint("checks")
        self.assertNotEqual(ep_a, ep_b)
        self.assertIn("customers", ep_a)
        self.assertIn("checks", ep_b)


class TestSetPageInEndpoint(unittest.TestCase):
    """_set_page_in_endpoint adds or updates pagination params."""

    def setUp(self):
        self.client = Clubspeed("myclub", "secret123")
        self.base = self.client._construct_endpoint("payments")

    def test_adds_page_and_limit_on_first_call(self):
        """First call appends page=0 and limit."""
        result = self.client._set_page_in_endpoint(self.base, 0)
        self.assertIn("&page=0", result)
        self.assertIn("&limit=100", result)

    def test_updates_page_on_subsequent_call(self):
        """Subsequent call replaces page number."""
        paged = self.client._set_page_in_endpoint(self.base, 0)
        result = self.client._set_page_in_endpoint(paged, 5)
        self.assertIn("&page=5", result)
        self.assertNotIn("&page=0", result)


class TestAddFilter(unittest.TestCase):
    """_add_filter appends the correct filter/order clause."""

    def setUp(self):
        self.client = Clubspeed("myclub", "secret123")
        self.base = self.client._construct_endpoint("checks")

    def test_no_column_name_returns_endpoint_unchanged(self):
        """When column_name is None, endpoint is returned as-is."""
        result = self.client._add_filter(self.base, "V2", None, "bm")
        self.assertEqual(self.base, result)

    def test_v2_with_bookmark(self):
        """V2 with bookmark appends $gt where clause."""
        result = self.client._add_filter(self.base, "V2", "closedDate", "2023-01-01")
        self.assertIn('"closedDate":{"$gt":"2023-01-01"}', result)
        self.assertIn("order=closedDate ASC", result)

    def test_v2_no_bookmark(self):
        """V2 without bookmark appends $isnot null clause."""
        result = self.client._add_filter(self.base, "V2", "closedDate", None)
        self.assertIn('"closedDate":{"$isnot":"null"}', result)

    def test_v1_with_bookmark(self):
        """V1 with bookmark appends filter > bookmark clause."""
        result = self.client._add_filter(self.base, "V1", "closedDate", "2023-01-01")
        self.assertIn("filter=closedDate > 2023-01-01", result)
        self.assertIn("order=closedDate ASC", result)

    def test_v1_no_bookmark(self):
        """V1 without bookmark appends IS NOT NULL filter."""
        result = self.client._add_filter(self.base, "V1", "closedDate", None)
        self.assertIn("filter=closedDate IS NOT NULL", result)


class TestGetMethod(unittest.TestCase):
    """_get() delegates to requests.get and handles errors."""

    def setUp(self):
        self.client = Clubspeed("myclub", "secret123")
        self.url = "https://myclub.clubspeedtiming.com/api/index.php/checks.json?key=secret123"

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_happy_path_returns_json(self, mock_get):
        """_get returns parsed JSON on 200."""
        mock_get.return_value = _make_response(200, {"data": [1, 2, 3]})
        result = self.client._get(self.url)
        self.assertEqual({"data": [1, 2, 3]}, result)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_500_raises_ignore_http_exception(self, mock_get):
        """_get raises IgnoreHttpException on 500."""
        mock_get.return_value = _make_response(500)
        with self.assertRaises(IgnoreHttpException):
            self.client._get(self.url)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_404_raises_http_error(self, mock_get):
        """_get raises HTTPError on 404."""
        mock_get.return_value = _make_response(404)
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._get(self.url)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_403_raises_forbidden_error(self, mock_get):
        """_get raises ClubspeedForbiddenError on 403."""
        from tap_clubspeed.clubspeed import ClubspeedForbiddenError
        resp = MagicMock()
        resp.status_code = 403
        resp.raise_for_status.return_value = None
        mock_get.return_value = resp
        with self.assertRaises(ClubspeedForbiddenError):
            self.client._get(self.url)

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_get_logs_url(self, mock_get):
        """_get logs the URL before making the request."""
        mock_get.return_value = _make_response(200, {})
        with self.assertLogs("root", level="INFO") as cm:
            self.client._get(self.url)
        self.assertTrue(any(self.url in line for line in cm.output))


class TestGetResponse(unittest.TestCase):
    """_get_response() paginates and yields records."""

    def setUp(self):
        self.client = Clubspeed("myclub", "secret123")
        self.base_url = self.client._construct_endpoint("checks")

    @patch.object(Clubspeed, "_get")
    def test_yields_all_records_single_page(self, mock_get):
        """_get_response yields all records from a single page."""
        mock_get.side_effect = [
            {"checks": [{"checkId": 1}, {"checkId": 2}]},
            {"checks": []},  # empty page terminates loop
        ]
        results = list(self.client._get_response(self.base_url, key="checks"))
        self.assertEqual([{"checkId": 1}, {"checkId": 2}], results)

    @patch.object(Clubspeed, "_get")
    def test_stops_on_empty_page(self, mock_get):
        """_get_response stops paginating when page returns no items."""
        mock_get.side_effect = [
            [{"id": 1}],
            [],
        ]
        results = list(self.client._get_response(self.base_url))
        self.assertEqual([{"id": 1}], results)

    @patch.object(Clubspeed, "_get")
    def test_ignores_500_and_continues(self, mock_get):
        """_get_response swallows IgnoreHttpException; next page returns empty to finish."""
        # After the 500 error, the next paginated request returns empty → loop exits
        mock_get.side_effect = [IgnoreHttpException("500"), []]
        results = list(self.client._get_response(self.base_url))
        self.assertEqual([], results)

    @patch.object(Clubspeed, "_get")
    def test_heat_ids_collected_from_heat_main_endpoint(self, mock_get):
        """Records from heatMain endpoint have their heatId collected."""
        heat_url = self.client._construct_endpoint("heatMain")
        mock_get.side_effect = [
            [{"heatId": 42}, {"heatId": 99}],
            [],
        ]
        list(self.client._get_response(heat_url))
        self.assertIn(42, self.client._new_heats)
        self.assertIn(99, self.client._new_heats)


class TestIsAuthorized(unittest.TestCase):
    """is_authorized() calls the payments endpoint."""

    @patch.object(Clubspeed, "_get")
    def test_is_authorized_calls_payments_endpoint(self, mock_get):
        """is_authorized calls GET on the payments endpoint."""
        mock_get.return_value = {"payments": []}
        client = Clubspeed("myclub", "secret123")
        client.is_authorized()
        called_url = mock_get.call_args[0][0]
        self.assertIn("payments", called_url)

    @patch.object(Clubspeed, "_get")
    def test_is_authorized_raises_on_http_error(self, mock_get):
        """is_authorized propagates HTTPError on auth failure."""
        mock_get.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
        client = Clubspeed("myclub", "secret123")
        with self.assertRaises(requests.exceptions.HTTPError):
            client.is_authorized()


if __name__ == "__main__":
    unittest.main()
