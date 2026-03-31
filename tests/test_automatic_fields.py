"""
Integration tests for tap-clubspeed — verify automatic (minimum) fields.

With no additional fields selected, the tap should still emit at least the
primary-key and replication-key fields for every stream.

Uses schema-driven mock records; patches requests.get for HTTP isolation.
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import ClubspeedBaseTest
except ImportError:
    from base import ClubspeedBaseTest

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.streams import STREAMS
from tap_clubspeed.sync import sync_stream


class ClubspeedAutomaticFieldsTest(ClubspeedBaseTest, unittest.TestCase):
    """
    Verify that automatic fields (primary keys + replication keys) are always
    emitted regardless of field selection.
    """

    STREAMS_TO_EXCLUDE = {
        "heat_main_details",  # depends on heat_main populating _new_heats
    }

    def _sync_with_minimum_record(self, stream_name):
        """
        Sync one record that contains only the automatic fields (PK + rep key).
        Returns the written records.
        """
        meta = self.expected_metadata()[stream_name]
        automatic_fields = meta[self.PRIMARY_KEYS] | meta[self.REPLICATION_KEYS]

        # Build a minimal record with only automatic fields
        minimal_record = {}
        schema = self._load_schema(stream_name)
        for field in automatic_fields:
            if field in schema.get("properties", {}):
                minimal_record[field] = self._generate_value(
                    schema["properties"][field], date_value="2024-01-15T00:00:00Z"
                )

        client = MagicMock(spec=Clubspeed)
        getattr_mock = MagicMock(return_value=iter([minimal_record]))
        setattr(client, stream_name, getattr_mock)

        stream_class = STREAMS[stream_name]
        instance = stream_class(client)
        instance.stream = self._make_catalog_stream(stream_name, instance.load_schema())

        written = []

        def capture_record(stream_id, record):
            written.append(record)

        with patch("tap_clubspeed.sync.singer.write_record", side_effect=capture_record):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream({}, instance)

        return written

    def test_primary_keys_always_emitted(self):
        """Primary key fields are present in every emitted record."""
        for stream_name, meta in self.expected_metadata().items():
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue
            with self.subTest(stream=stream_name):
                written = self._sync_with_minimum_record(stream_name)
                if not written:
                    continue
                for record in written:
                    for pk in meta[self.PRIMARY_KEYS]:
                        self.assertIn(
                            pk, record,
                            msg=f"PK '{pk}' missing from '{stream_name}' record",
                        )

    def test_replication_keys_emitted_for_incremental_streams(self):
        """Replication key fields are present in emitted records for INCREMENTAL streams."""
        for stream_name, meta in self.expected_metadata().items():
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue
            if meta[self.REPLICATION_METHOD] != self.INCREMENTAL:
                continue
            with self.subTest(stream=stream_name):
                written = self._sync_with_minimum_record(stream_name)
                if not written:
                    continue
                for record in written:
                    for rk in meta[self.REPLICATION_KEYS]:
                        self.assertIn(
                            rk, record,
                            msg=f"Rep key '{rk}' missing from '{stream_name}' record",
                        )

    def test_full_table_streams_emit_at_least_primary_keys(self):
        """FULL_TABLE streams always emit their primary key fields."""
        for stream_name, meta in self.expected_metadata().items():
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue
            if meta[self.REPLICATION_METHOD] != self.FULL_TABLE:
                continue
            with self.subTest(stream=stream_name):
                written = self._sync_with_minimum_record(stream_name)
                if not written:
                    continue
                for record in written:
                    for pk in meta[self.PRIMARY_KEYS]:
                        self.assertIn(pk, record)

    def test_no_records_lost_for_full_table_minimum_sync(self):
        """FULL_TABLE streams emit all records even with minimum fields selected."""
        stream_name = "booking"  # simple FULL_TABLE stream
        client = MagicMock(spec=Clubspeed)
        records = [{"onlineBookingsId": i} for i in range(5)]
        client.booking.return_value = iter(records)

        from tap_clubspeed.streams import Booking
        instance = Booking(client)
        instance.stream = self._make_catalog_stream(
            stream_name,
            {"properties": {"onlineBookingsId": {"type": ["null", "integer"]}},
             "type": "object"},
        )

        written = []
        with patch("tap_clubspeed.sync.singer.write_record",
                   side_effect=lambda s, r: written.append(r)):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream({}, instance)

        self.assertEqual(5, len(written))


if __name__ == "__main__":
    unittest.main()
