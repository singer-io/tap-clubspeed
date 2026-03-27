"""
Unit tests for tap_clubspeed.discover.
"""
import json
import os
import unittest
from unittest.mock import MagicMock, patch

from tap_clubspeed.clubspeed import Clubspeed
from tap_clubspeed.discover import discover_streams
from tap_clubspeed.streams import STREAMS


class TestDiscoverStreams(unittest.TestCase):
    """discover_streams returns the expected catalog entries."""

    def setUp(self):
        self.client = MagicMock(spec=Clubspeed)

    def test_returns_list(self):
        """discover_streams returns a list."""
        result = discover_streams(self.client)
        self.assertIsInstance(result, list)

    def test_length_matches_streams_registry(self):
        """Number of catalog entries matches number of registered streams."""
        result = discover_streams(self.client)
        self.assertEqual(len(STREAMS), len(result))

    def test_each_entry_has_required_keys(self):
        """Every catalog entry has stream, tap_stream_id, schema, and metadata."""
        result = discover_streams(self.client)
        required_keys = {"stream", "tap_stream_id", "schema", "metadata"}
        for entry in result:
            self.assertTrue(required_keys.issubset(entry.keys()),
                            msg=f"Entry missing keys: {entry}")

    def test_stream_names_match_registry(self):
        """tap_stream_id values correspond to registered stream names."""
        result = discover_streams(self.client)
        discovered_names = {entry["tap_stream_id"] for entry in result}
        self.assertEqual(set(STREAMS.keys()), discovered_names)

    def test_schema_has_properties(self):
        """Every stream schema contains a 'properties' object."""
        result = discover_streams(self.client)
        for entry in result:
            self.assertIn("properties", entry["schema"],
                          msg=f"Schema missing 'properties' for {entry['tap_stream_id']}")

    def test_metadata_is_list(self):
        """Metadata for each stream is a list."""
        result = discover_streams(self.client)
        for entry in result:
            self.assertIsInstance(entry["metadata"], list,
                                  msg=f"Metadata not a list for {entry['tap_stream_id']}")

    def test_checks_stream_has_replication_metadata(self):
        """The 'checks' stream metadata includes valid-replication-keys."""
        result = discover_streams(self.client)
        checks = next(e for e in result if e["tap_stream_id"] == "checks")
        root_meta = next(
            m["metadata"] for m in checks["metadata"] if m["breadcrumb"] == ()
        )
        self.assertIn("valid-replication-keys", root_meta)

    def test_booking_stream_has_full_table_replication(self):
        """The 'booking' full-table stream has forced-replication-method=FULL_TABLE."""
        result = discover_streams(self.client)
        booking = next(e for e in result if e["tap_stream_id"] == "booking")
        root_meta = next(
            m["metadata"] for m in booking["metadata"] if m["breadcrumb"] == ()
        )
        self.assertEqual("FULL_TABLE", root_meta.get("forced-replication-method"))

    def test_field_inclusion_set_on_key_properties(self):
        """Key property fields have inclusion=automatic in metadata."""
        result = discover_streams(self.client)
        checks_entry = next(e for e in result if e["tap_stream_id"] == "checks")
        key_prop_meta = next(
            m["metadata"]
            for m in checks_entry["metadata"]
            if m["breadcrumb"] == ("properties", "checkId")
        )
        self.assertEqual("automatic", key_prop_meta.get("inclusion"))

    def test_non_key_field_inclusion_is_available(self):
        """Non-key, non-replication fields have inclusion=available."""
        result = discover_streams(self.client)
        checks_entry = next(e for e in result if e["tap_stream_id"] == "checks")
        # 'type' is neither a key nor replication key for checks
        type_meta = next(
            (m["metadata"]
             for m in checks_entry["metadata"]
             if m["breadcrumb"] == ("properties", "type")),
            None,
        )
        if type_meta is not None:
            self.assertEqual("available", type_meta.get("inclusion"))


class TestLoadSchema(unittest.TestCase):
    """Stream.load_schema() reads the JSON file from schemas/."""

    def test_checks_schema_loaded_correctly(self):
        """Checks stream loads schema with checkId property."""
        from tap_clubspeed.streams import Checks
        client = MagicMock()
        stream = Checks(client)
        schema = stream.load_schema()
        self.assertIn("checkId", schema["properties"])

    def test_missing_schema_raises(self):
        """load_schema raises FileNotFoundError for unknown stream name."""
        from tap_clubspeed.streams import Stream
        stream = Stream(MagicMock())
        stream.name = "nonexistent_stream_xyz"
        with self.assertRaises(FileNotFoundError):
            stream.load_schema()


if __name__ == "__main__":
    unittest.main()
