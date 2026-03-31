"""
Integration tests for tap-clubspeed interrupted sync resumption (mock mode).

Verifies that when a sync is interrupted mid-stream and restarted with the
saved state, it resumes from the correct bookmark without duplicating records.
"""
import unittest
from unittest.mock import patch, MagicMock

import singer

try:
    from .base import ClubspeedBaseTest, MockResponse
except ImportError:
    from base import ClubspeedBaseTest, MockResponse

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.streams import Checks, Payments, Booking
from tap_clubspeed.sync import sync_stream


def _make_catalog_stream(stream_name, schema):
    cs = MagicMock()
    cs.tap_stream_id = stream_name
    cs.schema.to_dict.return_value = schema
    cs.metadata = []
    return cs


CHECKS_SCHEMA = {
    "properties": {
        "checkId": {"type": ["null", "integer"]},
        "closedDate": {"type": ["null", "string"]},
    },
    "type": "object",
}

PAYMENTS_SCHEMA = {
    "properties": {
        "paymentId": {"type": ["null", "integer"]},
        "payDate": {"type": ["null", "string"]},
    },
    "type": "object",
}


class ClubspeedInterruptedSyncTest(ClubspeedBaseTest, unittest.TestCase):
    """Verify sync correctly resumes from a mid-point bookmark after interruption."""

    # ── Bookmark resumption ───────────────────────────────────────────────

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_resumed_sync_skips_records_before_interrupted_bookmark(
            self, mock_write_record, mock_write_state):
        """After an interruption, records before the saved bookmark are skipped."""
        INTERRUPTED_BOOKMARK = "2023-06-15 00:00:00"

        # All records the API would return on a fresh sync
        all_records = [
            {"checkId": 1, "closedDate": "2023-01-01 00:00:00"},  # before bookmark
            {"checkId": 2, "closedDate": "2023-06-01 00:00:00"},  # before bookmark
            {"checkId": 3, "closedDate": "2023-07-01 00:00:00"},  # after bookmark
            {"checkId": 4, "closedDate": "2024-01-01 00:00:00"},  # after bookmark
        ]

        client = MagicMock(spec=Clubspeed)
        client.checks.return_value = iter(all_records)

        instance = Checks(client)
        instance.stream = _make_catalog_stream("checks", CHECKS_SCHEMA)

        interrupted_state = {
            "bookmarks": {"checks": {"closedDate": INTERRUPTED_BOOKMARK}}
        }

        sync_stream(interrupted_state, instance)

        written_records = [call[0][1] for call in mock_write_record.call_args_list]
        written_ids = [r["checkId"] for r in written_records]

        # Records 1 and 2 are before the interrupted bookmark
        self.assertNotIn(1, written_ids,
                         msg="checkId=1 was before interrupted bookmark, should be skipped")
        self.assertNotIn(2, written_ids,
                         msg="checkId=2 was before interrupted bookmark, should be skipped")
        # Records 3 and 4 are after the bookmark
        self.assertIn(3, written_ids)
        self.assertIn(4, written_ids)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_resumed_sync_does_not_duplicate_already_synced_records(
            self, mock_write_record, mock_write_state):
        """Records already processed before interruption are not re-emitted."""
        # Simulate: sync processed records up to 2023-09-01
        INTERRUPTED_BOOKMARK = "2023-09-01 00:00:00"

        records = [
            {"checkId": 10, "closedDate": "2023-08-01 00:00:00"},  # already synced
            {"checkId": 11, "closedDate": "2023-10-01 00:00:00"},  # new after interruption
        ]

        client = MagicMock(spec=Clubspeed)
        client.checks.return_value = iter(records)

        instance = Checks(client)
        instance.stream = _make_catalog_stream("checks", CHECKS_SCHEMA)

        interrupted_state = {
            "bookmarks": {"checks": {"closedDate": INTERRUPTED_BOOKMARK}}
        }

        sync_stream(interrupted_state, instance)

        written_records = [call[0][1] for call in mock_write_record.call_args_list]
        written_ids = [r["checkId"] for r in written_records]

        self.assertNotIn(10, written_ids,
                         msg="checkId=10 was already synced before interruption")
        self.assertIn(11, written_ids)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_bookmark_advanced_correctly_after_resumed_sync(
            self, mock_write_record, mock_write_state):
        """After a resumed sync, the bookmark is set to the latest synced record."""
        INTERRUPTED_BOOKMARK = "2023-06-01 00:00:00"

        records = [
            {"checkId": 1, "closedDate": "2023-07-01 00:00:00"},
            {"checkId": 2, "closedDate": "2024-01-01 00:00:00"},
        ]

        client = MagicMock(spec=Clubspeed)
        client.checks.return_value = iter(records)

        instance = Checks(client)
        instance.stream = _make_catalog_stream("checks", CHECKS_SCHEMA)

        state = {"bookmarks": {"checks": {"closedDate": INTERRUPTED_BOOKMARK}}}
        sync_stream(state, instance)

        final_bookmark = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2024-01-01 00:00:00", final_bookmark,
                         msg="Bookmark should advance to the latest record after resumed sync")

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_multiple_incremental_streams_resume_independently(
            self, mock_write_record, mock_write_state):
        """Each stream resumes from its own independent bookmark."""
        checks_records = [
            {"checkId": 1, "closedDate": "2024-03-01 00:00:00"},
        ]
        payments_records = [
            {"paymentId": 99, "payDate": "2024-03-01 00:00:00"},
        ]

        # Both streams have old bookmarks
        state = {
            "bookmarks": {
                "checks": {"closedDate": "2023-01-01 00:00:00"},
                "payments": {"payDate": "2024-06-01 00:00:00"},  # future bookmark!
            }
        }

        # Sync checks: record is newer than bookmark → should be emitted
        client_checks = MagicMock(spec=Clubspeed)
        client_checks.checks.return_value = iter(checks_records)
        instance_checks = Checks(client_checks)
        instance_checks.stream = _make_catalog_stream("checks", CHECKS_SCHEMA)

        checks_written = []
        with patch("tap_clubspeed.sync.singer.write_record",
                   side_effect=lambda s, r: checks_written.append(r)):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream(state, instance_checks)

        # Sync payments: record is older than bookmark → should be skipped
        client_payments = MagicMock(spec=Clubspeed)
        client_payments.payments.return_value = iter(payments_records)
        instance_payments = Payments(client_payments)
        instance_payments.stream = _make_catalog_stream("payments", PAYMENTS_SCHEMA)

        payments_written = []
        with patch("tap_clubspeed.sync.singer.write_record",
                   side_effect=lambda s, r: payments_written.append(r)):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream(state, instance_payments)

        self.assertEqual(1, len(checks_written),
                         "checks record newer than bookmark should be emitted")
        self.assertEqual(0, len(payments_written),
                         "payments record older than bookmark should be skipped")

    # ── FULL_TABLE streams always fully replicate ─────────────────────────

    @patch("tap_clubspeed.sync.singer.write_record")
    def test_full_table_stream_always_fully_replicated_after_interruption(
            self, mock_write_record):
        """FULL_TABLE streams ignore state and always emit all records."""
        stale_state = {"bookmarks": {}}

        client = MagicMock(spec=Clubspeed)
        all_records = [{"onlineBookingsId": i} for i in range(3)]
        client.booking.return_value = iter(all_records)

        instance = Booking(client)
        instance.stream = _make_catalog_stream(
            "booking",
            {"properties": {"onlineBookingsId": {"type": ["null", "integer"]}},
             "type": "object"},
        )

        sync_stream(stale_state, instance)

        self.assertEqual(3, mock_write_record.call_count,
                         "FULL_TABLE stream must replicate all records regardless of state")

    @patch("tap_clubspeed.sync.singer.write_record")
    def test_full_table_stream_with_nonempty_state_still_emits_all(
            self, mock_write_record):
        """Even with a non-empty state dict, FULL_TABLE streams run in full."""
        state = {"bookmarks": {"some_other_stream": {"id": "999"}}}

        client = MagicMock(spec=Clubspeed)
        client.booking.return_value = iter([{"onlineBookingsId": 1},
                                            {"onlineBookingsId": 2}])
        instance = Booking(client)
        instance.stream = _make_catalog_stream(
            "booking",
            {"properties": {"onlineBookingsId": {"type": ["null", "integer"]}},
             "type": "object"},
        )

        sync_stream(state, instance)

        self.assertEqual(2, mock_write_record.call_count)


if __name__ == "__main__":
    unittest.main()
