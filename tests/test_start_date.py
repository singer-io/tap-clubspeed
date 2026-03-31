"""
Integration tests for tap-clubspeed start-date filtering with mocked HTTP.

Verifies that INCREMENTAL streams only emit records whose replication key
is on or after the configured start_date (via bookmark semantics).
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import ClubspeedBaseTest
except ImportError:
    from base import ClubspeedBaseTest

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.streams import Checks, Customers
from tap_clubspeed.sync import sync_stream


class ClubspeedStartDateTest(ClubspeedBaseTest, unittest.TestCase):
    """
    Verify start-date / bookmark filtering behaviour for INCREMENTAL streams.

    tap-clubspeed does not natively filter by start_date at the API level.
    Instead, it relies on bookmark-based filtering in Stream.is_bookmark_old().
    These tests verify that the bookmark mechanism correctly filters old records.
    """

    START_DATE_1 = "2015-03-25T00:00:00Z"  # earlier — expect more records
    START_DATE_2 = "2017-01-25T00:00:00Z"  # later   — expect fewer or equal records

    def _sync_with_initial_bookmark(self, stream_class, client_method_name,
                                    records, bookmark_value, schema):
        """Run a sync starting from a given bookmark value; return written records."""
        client = MagicMock(spec=Clubspeed)
        getattr(client, client_method_name).return_value = iter(records)

        instance = stream_class(client)
        instance.stream = self._make_catalog_stream(instance.name, schema)

        rep_key = instance.replication_key
        state = (
            {"bookmarks": {instance.name: {rep_key: bookmark_value}}}
            if bookmark_value else {}
        )

        written = []
        with patch("tap_clubspeed.sync.singer.write_record",
                   side_effect=lambda s, r: written.append(r)):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream(state, instance)

        return written

    # ── Checks stream ────────────────────────────────────────────────────

    def test_checks_early_start_date_includes_all_records(self):
        """With an early start-date bookmark, all records are emitted."""
        records = [
            {"checkId": 1, "closedDate": "2016-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2018-06-01 00:00:00"},
            {"checkId": 3, "closedDate": "2020-01-01 00:00:00"},
        ]
        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        written = self._sync_with_initial_bookmark(
            Checks, "checks", records, self.START_DATE_1, schema
        )
        self.assertEqual(3, len(written))

    def test_checks_later_start_date_excludes_older_records(self):
        """With a later start-date bookmark, records before it are excluded."""
        records = [
            {"checkId": 1, "closedDate": "2016-01-01 00:00:00"},  # before bookmark
            {"checkId": 2, "closedDate": "2018-06-01 00:00:00"},  # after bookmark
        ]
        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        written = self._sync_with_initial_bookmark(
            Checks, "checks", records, self.START_DATE_2, schema
        )
        written_ids = [r["checkId"] for r in written]
        self.assertNotIn(1, written_ids)
        self.assertIn(2, written_ids)

    def test_later_start_date_returns_fewer_or_equal_records(self):
        """Later start-date always produces fewer or equal records than earlier one."""
        records = [
            {"checkId": 1, "closedDate": "2016-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2016-06-01 00:00:00"},
            {"checkId": 3, "closedDate": "2018-01-01 00:00:00"},
            {"checkId": 4, "closedDate": "2020-01-01 00:00:00"},
        ]
        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }

        written_early = self._sync_with_initial_bookmark(
            Checks, "checks", records, self.START_DATE_1, schema
        )
        # Must re-use the same records for second sync (iterators are exhausted)
        written_late = self._sync_with_initial_bookmark(
            Checks, "checks", records, self.START_DATE_2, schema
        )

        self.assertLessEqual(len(written_late), len(written_early))

    # ── Customers stream ─────────────────────────────────────────────────

    def test_customers_early_bookmark_returns_all_records(self):
        """With early bookmark, all customer records are emitted."""
        records = [
            {"customerId": 1, "lastVisited": "2016-03-01 00:00:00"},
            {"customerId": 2, "lastVisited": "2019-01-01 00:00:00"},
        ]
        schema = {
            "properties": {
                "customerId": {"type": ["null", "integer"]},
                "lastVisited": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        written = self._sync_with_initial_bookmark(
            Customers, "customers", records, self.START_DATE_1, schema
        )
        self.assertEqual(2, len(written))

    # ── No bookmark (no start_date filtering) ────────────────────────────

    def test_no_bookmark_emits_all_records(self):
        """With no bookmark, all records are emitted regardless of date."""
        records = [
            {"checkId": 1, "closedDate": "2010-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2024-01-01 00:00:00"},
        ]
        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        written = self._sync_with_initial_bookmark(
            Checks, "checks", records, None, schema
        )
        self.assertEqual(2, len(written))

    # ── FULL_TABLE streams ignore start_date ─────────────────────────────

    def test_full_table_stream_always_emits_all_records(self):
        """FULL_TABLE streams are not filtered by start_date or bookmark."""
        from tap_clubspeed.streams import Booking
        client = MagicMock(spec=Clubspeed)
        records = [{"onlineBookingsId": i} for i in range(5)]
        client.booking.return_value = iter(records)

        instance = Booking(client)
        instance.stream = self._make_catalog_stream(
            "booking",
            {"properties": {"onlineBookingsId": {"type": ["null", "integer"]}},
             "type": "object"},
        )

        # Even with a "late" start_date state, FULL_TABLE emits everything
        state = {"bookmarks": {"booking": {}}}
        written = []
        with patch("tap_clubspeed.sync.singer.write_record",
                   side_effect=lambda s, r: written.append(r)):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream(state, instance)

        self.assertEqual(5, len(written))


if __name__ == "__main__":
    unittest.main()
