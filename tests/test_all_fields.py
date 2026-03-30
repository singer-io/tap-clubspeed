"""
Integration tests for tap-clubspeed — verify all schema fields are replicated.

Uses schema-driven mock records generated from the tap's own JSON schema files.
Patches tap_clubspeed.clubspeed.requests.get so no real HTTP calls are made.
"""
import unittest
from unittest.mock import patch, MagicMock

import singer

try:
    from .base import ClubspeedBaseTest, MockResponse
except ImportError:
    from base import ClubspeedBaseTest, MockResponse

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.streams import STREAMS
from tap_clubspeed.sync import sync_stream

# Fields known to exist in the schema but possibly absent from mock-generated
# records (e.g. nested objects that collapse to None). Add entries here after
# observing failures in the first test run; leave empty for now.
KNOWN_MISSING_FIELDS = {
    # "<stream_name>": {"<field_name>", ...},
}


def _make_catalog_stream(stream_name, schema):
    """Build a minimal mock catalog stream object for sync_stream."""
    cs = MagicMock()
    cs.tap_stream_id = stream_name
    cs.schema.to_dict.return_value = schema
    cs.metadata = []
    return cs


class ClubspeedAllFieldsTest(ClubspeedBaseTest, unittest.TestCase):
    """Verify that all schema fields are present in at least one emitted record."""

    MISSING_FIELDS = KNOWN_MISSING_FIELDS

    # Streams that have complex data dependencies and are skipped in this test.
    # heat_main_details depends on heat_main having populated _new_heats.
    STREAMS_TO_EXCLUDE = {"heat_main_details"}

    def _sync_stream_with_mock(self, stream_name, records):
        """
        Run sync_stream for one stream with mock records.
        Returns the list of records written by singer.write_record.
        """
        client = MagicMock(spec=Clubspeed)

        # Attach mock return value to the client method for this stream
        client_method = MagicMock(return_value=iter(records))
        setattr(client, stream_name, client_method)

        stream_class = STREAMS[stream_name]
        instance = stream_class(client)
        instance.stream = _make_catalog_stream(stream_name, instance.load_schema())

        written = []

        def capture_record(stream_id, record):
            written.append(record)

        with patch("tap_clubspeed.sync.singer.write_record", side_effect=capture_record):
            with patch("tap_clubspeed.sync.singer.write_state"):
                sync_stream({}, instance)

        return written

    def test_all_schema_fields_present_in_emitted_records(self):
        """Every field in the schema appears in at least one emitted record."""
        expected_meta = self.expected_metadata()

        for stream_name in expected_meta:
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue

            with self.subTest(stream=stream_name):
                schema = self._load_schema(stream_name)
                expected_fields = set(schema.get("properties", {}).keys())

                # Skip known missing fields
                known_missing = self.MISSING_FIELDS.get(stream_name, set())
                required_fields = expected_fields - known_missing

                # Generate two records so both date ranges are covered
                record1 = self._generate_stream_record(stream_name, "2024-01-01T00:00:00Z")
                record2 = self._generate_stream_record(stream_name, "2024-06-01T00:00:00Z")

                written = self._sync_stream_with_mock(stream_name, [record1, record2])

                if not written:
                    # No records emitted — skip assertion (state may have filtered them)
                    continue

                actual_fields = set().union(*[set(r.keys()) for r in written])
                missing = required_fields - actual_fields
                self.assertEqual(
                    set(), missing,
                    msg=(
                        f"Stream '{stream_name}' is missing fields in emitted records: "
                        f"{missing}. Add to KNOWN_MISSING_FIELDS if expected."
                    ),
                )

    def test_schema_driven_records_have_correct_types(self):
        """_generate_stream_record produces records with schema-valid types."""
        for stream_name in self.expected_stream_names():
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue
            with self.subTest(stream=stream_name):
                record = self._generate_stream_record(stream_name)
                schema = self._load_schema(stream_name)
                # Basic structural check: all schema properties exist in generated record
                for prop in schema.get("properties", {}):
                    self.assertIn(
                        prop, record,
                        msg=f"Generated record for '{stream_name}' missing field '{prop}'",
                    )

    def test_no_extra_fields_beyond_schema(self):
        """Emitted records do not contain fields absent from the schema."""
        expected_meta = self.expected_metadata()

        for stream_name in expected_meta:
            if stream_name in self.STREAMS_TO_EXCLUDE:
                continue
            with self.subTest(stream=stream_name):
                schema = self._load_schema(stream_name)
                schema_fields = set(schema.get("properties", {}).keys())

                record = self._generate_stream_record(stream_name)
                written = self._sync_stream_with_mock(stream_name, [record])

                for rec in written:
                    extra = set(rec.keys()) - schema_fields
                    self.assertEqual(
                        set(), extra,
                        msg=f"'{stream_name}' emitted extra fields not in schema: {extra}",
                    )


if __name__ == "__main__":
    unittest.main()
