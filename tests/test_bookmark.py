"""
Integration tests for tap-clubspeed bookmarking with mocked HTTP.

Patches tap_clubspeed.clubspeed.requests.get so no real API calls are made.
Verifies INCREMENTAL streams correctly read, set, and advance their bookmarks.
"""
import unittest
from unittest.mock import patch, MagicMock, call

import singer

try:
    from .base import ClubspeedBaseTest, MockResponse
except ImportError:
    from base import ClubspeedBaseTest, MockResponse

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.streams import Checks, Customers, Payments
from tap_clubspeed.sync import sync_stream


class ClubspeedBookmarkTest(ClubspeedBaseTest, unittest.TestCase):
    """Verify bookmark behaviour for INCREMENTAL streams."""

    # ── Bookmark read/set via Stream helpers ─────────────────────────────

    def test_bookmark_is_none_when_state_is_empty(self):
        """get_bookmark returns None when the state has no entry."""
        instance = Checks(MagicMock())
        self.assertIsNone(instance.get_bookmark({}))

    def test_bookmark_returns_stored_value(self):
        """get_bookmark returns the value stored in state."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        instance = Checks(MagicMock())
        self.assertEqual("2023-01-01 00:00:00", instance.get_bookmark(state))

    def test_update_bookmark_writes_newer_value(self):
        """update_bookmark advances the bookmark when the value is newer."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2024-06-01 00:00:00")
        stored = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2024-06-01 00:00:00", stored)

    def test_update_bookmark_does_not_go_backwards(self):
        """update_bookmark does NOT overwrite with an older value."""
        state = {"bookmarks": {"checks": {"closedDate": "2024-06-01 00:00:00"}}}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2023-01-01 00:00:00")
        stored = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2024-06-01 00:00:00", stored)

    def test_update_bookmark_writes_when_no_previous_bookmark(self):
        """update_bookmark writes the value when state has no existing bookmark."""
        state = {}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2024-01-01 00:00:00")
        stored = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2024-01-01 00:00:00", stored)

    # ── is_bookmark_old ──────────────────────────────────────────────────

    def test_is_bookmark_old_true_for_newer_record(self):
        """is_bookmark_old is True when the record value is newer."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        instance = Checks(MagicMock())
        self.assertTrue(instance.is_bookmark_old(state, "2024-01-01 00:00:00"))

    def test_is_bookmark_old_false_for_older_record(self):
        """is_bookmark_old is False when the record value is older."""
        state = {"bookmarks": {"checks": {"closedDate": "2024-06-01 00:00:00"}}}
        instance = Checks(MagicMock())
        self.assertFalse(instance.is_bookmark_old(state, "2023-01-01 00:00:00"))

    def test_is_bookmark_old_true_when_no_bookmark(self):
        """is_bookmark_old is True when there is no prior bookmark."""
        instance = Checks(MagicMock())
        self.assertTrue(instance.is_bookmark_old({}, "2023-01-01 00:00:00"))

    # ── Sync writes bookmark into state ─────────────────────────────────

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_bookmark_written_to_state_after_sync(self, mock_write_record, mock_write_state):
        """After syncing an INCREMENTAL stream, the bookmark is updated in state."""
        client = MagicMock()
        instance = Checks(client)
        client.checks.return_value = iter([
            {"checkId": 1, "closedDate": "2024-03-01 00:00:00"},
            {"checkId": 2, "closedDate": "2024-03-15 00:00:00"},
        ])
        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        instance.stream = self._make_catalog_stream("checks", schema)
        state = {}

        sync_stream(state, instance)

        stored = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2024-03-15 00:00:00", stored)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_bookmark_filters_old_records_on_second_sync(self, mock_write_record, mock_write_state):
        """Second sync with a bookmark excludes records before the bookmark."""
        client = MagicMock()
        instance = Checks(client)

        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        instance.stream = self._make_catalog_stream("checks", schema)

        # Records: one old (before bookmark), one new (after bookmark)
        records = [
            {"checkId": 1, "closedDate": "2022-01-01 00:00:00"},  # before
            {"checkId": 2, "closedDate": "2024-05-01 00:00:00"},  # after
        ]
        client.checks.return_value = iter(records)

        state = {"bookmarks": {"checks": {"closedDate": "2023-06-01 00:00:00"}}}
        sync_stream(state, instance)

        # Only checkId=2 should have been written
        written_records = [call[0][1] for call in mock_write_record.call_args_list]
        written_ids = [r["checkId"] for r in written_records]
        self.assertNotIn(1, written_ids)
        self.assertIn(2, written_ids)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_bookmark_advances_to_latest_record(self, mock_write_record, mock_write_state):
        """Bookmark is advanced to the most recent record's replication key."""
        client = MagicMock()
        instance = Payments(client)

        schema = {
            "properties": {
                "paymentId": {"type": ["null", "integer"]},
                "payDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        instance.stream = self._make_catalog_stream("payments", schema)
        client.payments.return_value = iter([
            {"paymentId": 1, "payDate": "2024-01-01 00:00:00"},
            {"paymentId": 2, "payDate": "2024-06-15 00:00:00"},
        ])

        state = {}
        sync_stream(state, instance)

        stored = singer.get_bookmark(state, "payments", "payDate")
        self.assertEqual("2024-06-15 00:00:00", stored)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_write_state_called_per_record_for_incremental(
            self, mock_write_record, mock_write_state):
        """write_state is called after each record for INCREMENTAL streams."""
        client = MagicMock()
        instance = Customers(client)

        schema = {
            "properties": {
                "customerId": {"type": ["null", "integer"]},
                "lastVisited": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        instance.stream = self._make_catalog_stream("customers", schema)
        client.customers.return_value = iter([
            {"customerId": 1, "lastVisited": "2024-01-01 00:00:00"},
            {"customerId": 2, "lastVisited": "2024-01-02 00:00:00"},
        ])

        state = {}
        sync_stream(state, instance)

        self.assertEqual(2, mock_write_state.call_count)

    # ── HTTP-level bookmark filtering (via _add_filter) ──────────────────

    @patch("tap_clubspeed.clubspeed.requests.get")
    def test_bookmark_passed_to_api_filter(self, mock_get):
        """The bookmark value is forwarded to _add_filter, which builds the URL filter."""
        # checks() calls _get_response(endpoint, key='checks'), so the response
        # must be a dict with a 'checks' key (even if empty) to avoid TypeError.
        mock_get.return_value = MockResponse({"checks": []})

        client = Clubspeed("myclub", "secret")
        # Call the checks method with a bookmark value
        list(client.checks(column_name="closedDate", bookmark="2023-01-01"))

        # The GET URL should contain the filter clause with the bookmark value
        called_url = mock_get.call_args[0][0]
        self.assertIn("2023-01-01", called_url)


if __name__ == "__main__":
    unittest.main()
