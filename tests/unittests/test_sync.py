"""
Unit tests for tap_clubspeed.sync and Stream.sync().
"""
import unittest
from unittest.mock import MagicMock, patch

import singer

from tap_clubspeed.sync import sync_stream
from tap_clubspeed.streams import (
    Booking, Checks, Customers
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stream_instance(stream_class, records, replication_key=None):
    """
    Build a stream instance whose underlying client method returns `records`.
    Also attaches a catalog stream mock with a minimal schema.
    """
    client = MagicMock()
    instance = stream_class(client)

    # Minimal schema – only fields that appear in test records
    schema = {
        "properties": {
            "id": {"type": ["null", "integer"]},
        },
        "type": "object",
    }

    if replication_key and replication_key not in schema["properties"]:
        schema["properties"][replication_key] = {"type": ["null", "string"]}

    catalog_stream = MagicMock()
    catalog_stream.tap_stream_id = instance.name
    catalog_stream.schema.to_dict.return_value = schema
    catalog_stream.metadata = []
    instance.stream = catalog_stream

    # Make the client method return records
    getattr(client, instance.name).return_value = iter(records)

    return instance


# ---------------------------------------------------------------------------
# sync_stream() – top-level function
# ---------------------------------------------------------------------------

class TestSyncStreamFullTable(unittest.TestCase):
    """sync_stream with a FULL_TABLE stream."""

    @patch("tap_clubspeed.sync.singer.write_record")
    def test_write_record_called_per_record(self, mock_write_record):
        """write_record is called once for each record."""
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        instance = _make_stream_instance(Booking, records)
        # Booking has key_properties = ['onlineBookingsId'] – add it to schema
        instance.stream.schema.to_dict.return_value = {
            "properties": {"id": {"type": ["null", "integer"]},
                           "onlineBookingsId": {"type": ["null", "integer"]}},
            "type": "object",
        }
        state = {}
        sync_stream(state, instance)
        self.assertEqual(3, mock_write_record.call_count)

    @patch("tap_clubspeed.sync.singer.write_record")
    def test_returns_record_count(self, mock_write_record):
        """sync_stream returns the number of records it processes (attempted writes)."""
        records = [{"id": 1}, {"id": 2}]
        instance = _make_stream_instance(Booking, records)
        instance.stream.schema.to_dict.return_value = {
            "properties": {"id": {"type": ["null", "integer"]},
                           "onlineBookingsId": {"type": ["null", "integer"]}},
            "type": "object",
        }
        state = {}
        count = sync_stream(state, instance)
        self.assertEqual(2, count)

    @patch("tap_clubspeed.sync.singer.write_record")
    def test_empty_records_returns_zero(self, mock_write_record):
        """sync_stream returns 0 when there are no records."""
        instance = _make_stream_instance(Booking, [])
        state = {}
        count = sync_stream(state, instance)
        self.assertEqual(0, count)
        mock_write_record.assert_not_called()

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_write_state_not_called_for_full_table(self, mock_write_record, mock_write_state):
        """write_state is NOT called per-record for FULL_TABLE streams."""
        records = [{"id": 1}]
        instance = _make_stream_instance(Booking, records)
        instance.stream.schema.to_dict.return_value = {
            "properties": {"id": {"type": ["null", "integer"]},
                           "onlineBookingsId": {"type": ["null", "integer"]}},
            "type": "object",
        }
        state = {}
        sync_stream(state, instance)
        mock_write_state.assert_not_called()


class TestSyncStreamIncremental(unittest.TestCase):
    """sync_stream with an INCREMENTAL stream."""

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_write_state_called_per_record_for_incremental(
            self, mock_write_record, mock_write_state):
        """write_state is called after every record for INCREMENTAL streams."""
        records = [
            {"checkId": 1, "closedDate": "2023-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2023-01-02 00:00:00"},
        ]
        instance = _make_stream_instance(Checks, records, replication_key="closedDate")
        instance.stream.schema.to_dict.return_value = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        state = {}
        sync_stream(state, instance)
        self.assertEqual(2, mock_write_state.call_count)

    @patch("tap_clubspeed.sync.singer.write_state")
    @patch("tap_clubspeed.sync.singer.write_record")
    def test_transform_exception_is_handled_without_crash(self, mock_write_record, mock_write_state):
        """A transform exception is caught and logged; sync continues."""
        records = [{"checkId": "bad-data"}, {"checkId": 2, "closedDate": "2023-01-01 00:00:00"}]
        instance = _make_stream_instance(Checks, records, replication_key="closedDate")
        # Schema that will cause transform to fail on first record (wrong type)
        instance.stream.schema.to_dict.return_value = {
            "properties": {
                "checkId": {"type": "integer"},        # strict integer, no null
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        state = {}
        # Should not raise
        sync_stream(state, instance)


# ---------------------------------------------------------------------------
# Stream.sync() – incremental bookmark filtering
# ---------------------------------------------------------------------------

class TestStreamSyncIncremental(unittest.TestCase):
    """Stream.sync() with INCREMENTAL replication method."""

    def _make_instance(self, records, state):
        client = MagicMock()
        instance = Checks(client)
        client.checks.return_value = iter(records)

        schema = {
            "properties": {
                "checkId": {"type": ["null", "integer"]},
                "closedDate": {"type": ["null", "string"]},
            },
            "type": "object",
        }
        catalog_stream = MagicMock()
        catalog_stream.tap_stream_id = "checks"
        catalog_stream.schema.to_dict.return_value = schema
        catalog_stream.metadata = []
        instance.stream = catalog_stream
        return instance

    def test_only_new_records_yielded_when_bookmark_exists(self):
        """Records whose replication key is after the bookmark are yielded."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        records = [
            {"checkId": 1, "closedDate": "2022-12-31 00:00:00"},  # before bookmark
            {"checkId": 2, "closedDate": "2023-01-02 00:00:00"},  # after bookmark
        ]
        instance = self._make_instance(records, state)
        results = list(instance.sync(state))
        ids = [r["checkId"] for (_, r) in results]
        self.assertNotIn(1, ids)
        self.assertIn(2, ids)

    def test_all_records_yielded_when_no_bookmark(self):
        """All records are yielded when state has no bookmark."""
        state = {}
        records = [
            {"checkId": 1, "closedDate": "2022-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2023-01-01 00:00:00"},
        ]
        instance = self._make_instance(records, state)
        results = list(instance.sync(state))
        self.assertEqual(2, len(results))

    def test_bookmark_is_updated_to_latest_value(self):
        """Bookmark is updated to the most recent replication key value synced."""
        state = {}
        records = [
            {"checkId": 1, "closedDate": "2023-01-01 00:00:00"},
            {"checkId": 2, "closedDate": "2023-06-01 00:00:00"},
        ]
        instance = self._make_instance(records, state)
        list(instance.sync(state))
        bookmark = singer.get_bookmark(state, "checks", "closedDate")
        self.assertEqual("2023-06-01 00:00:00", bookmark)

    def test_record_missing_replication_key_is_still_yielded(self):
        """A record without the replication key is yielded (KeyError handled)."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        records = [{"checkId": 99}]  # no closedDate
        instance = self._make_instance(records, state)
        results = list(instance.sync(state))
        self.assertEqual(1, len(results))


class TestStreamSyncFullTable(unittest.TestCase):
    """Stream.sync() with FULL_TABLE replication method."""

    def test_all_records_yielded(self):
        """All records are yielded for a FULL_TABLE stream."""
        client = MagicMock()
        instance = Booking(client)
        client.booking.return_value = iter([{"onlineBookingsId": 1}, {"onlineBookingsId": 2}])

        schema = {"properties": {"onlineBookingsId": {"type": ["null", "integer"]}}, "type": "object"}
        catalog_stream = MagicMock()
        catalog_stream.tap_stream_id = "booking"
        catalog_stream.schema.to_dict.return_value = schema
        catalog_stream.metadata = []
        instance.stream = catalog_stream

        results = list(instance.sync({}))
        self.assertEqual(2, len(results))

    def test_stream_yielded_with_record_tuple(self):
        """sync() yields (stream, record) tuples."""
        client = MagicMock()
        instance = Booking(client)
        client.booking.return_value = iter([{"onlineBookingsId": 5}])

        catalog_stream = MagicMock()
        catalog_stream.tap_stream_id = "booking"
        instance.stream = catalog_stream

        results = list(instance.sync({}))
        stream_obj, record = results[0]
        self.assertIs(catalog_stream, stream_obj)
        self.assertEqual({"onlineBookingsId": 5}, record)


# ---------------------------------------------------------------------------
# Stream bookmark helpers
# ---------------------------------------------------------------------------

class TestStreamBookmarkHelpers(unittest.TestCase):
    """get_bookmark / update_bookmark / is_bookmark_old on Stream."""

    def test_get_bookmark_returns_none_when_absent(self):
        """get_bookmark returns None when stream has no bookmark in state."""
        instance = Checks(MagicMock())
        self.assertIsNone(instance.get_bookmark({}))

    def test_get_bookmark_returns_stored_value(self):
        """get_bookmark returns the stored value from state."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01"}}}
        instance = Checks(MagicMock())
        self.assertEqual("2023-01-01", instance.get_bookmark(state))

    def test_update_bookmark_writes_newer_value(self):
        """update_bookmark writes the value when it is newer than the current bookmark."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2023-06-01 00:00:00")
        self.assertEqual("2023-06-01 00:00:00",
                         singer.get_bookmark(state, "checks", "closedDate"))

    def test_update_bookmark_does_not_write_older_value(self):
        """update_bookmark does NOT overwrite with an older value."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-06-01 00:00:00"}}}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2023-01-01 00:00:00")
        # Bookmark should remain at the newer date
        self.assertEqual("2023-06-01 00:00:00",
                         singer.get_bookmark(state, "checks", "closedDate"))

    def test_update_bookmark_writes_when_no_previous_bookmark(self):
        """update_bookmark always writes when there is no existing bookmark."""
        state = {}
        instance = Checks(MagicMock())
        instance.update_bookmark(state, "2023-01-01 00:00:00")
        self.assertEqual("2023-01-01 00:00:00",
                         singer.get_bookmark(state, "checks", "closedDate"))

    def test_is_bookmark_old_true_when_value_newer_than_bookmark(self):
        """is_bookmark_old is True when the record value is newer than the bookmark."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-01-01 00:00:00"}}}
        instance = Checks(MagicMock())
        self.assertTrue(instance.is_bookmark_old(state, "2023-06-01 00:00:00"))

    def test_is_bookmark_old_false_when_value_older_than_bookmark(self):
        """is_bookmark_old is False when the record value is older than the bookmark."""
        state = {"bookmarks": {"checks": {"closedDate": "2023-06-01 00:00:00"}}}
        instance = Checks(MagicMock())
        self.assertFalse(instance.is_bookmark_old(state, "2023-01-01 00:00:00"))

    def test_is_bookmark_old_true_when_no_bookmark(self):
        """is_bookmark_old is True when there is no existing bookmark."""
        instance = Checks(MagicMock())
        self.assertTrue(instance.is_bookmark_old({}, "2023-01-01 00:00:00"))

    def test_is_bookmark_old_with_integer_key(self):
        """is_bookmark_old compares integer replication keys correctly."""
        state = {"bookmarks": {"customers": {"lastVisited": 3}}}
        instance = Customers(MagicMock())
        # value 5 >= current bookmark 3 → True (is old enough to include)
        self.assertTrue(instance.is_bookmark_old(state, 5))
        # value 1 < current bookmark 3 → False
        self.assertFalse(instance.is_bookmark_old(state, 1))


if __name__ == "__main__":
    unittest.main()
