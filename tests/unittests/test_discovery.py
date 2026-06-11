"""
Unit tests for tap_clubspeed.discover.
"""
import unittest
from unittest.mock import MagicMock, patch

from tap_clubspeed.clubspeed import Clubspeed, ClubspeedForbiddenError
from tap_clubspeed.discover import discover_streams, _apply_access_checks
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


class TestStreamCheckAccess(unittest.TestCase):
    """Stream.check_access() handles accessible, forbidden, and no-client cases."""

    def test_returns_true_when_accessible(self):
        """check_access returns True when the client method yields records."""
        from tap_clubspeed.streams import Checks

        client = MagicMock()
        client.checks.return_value = iter([{"checkId": 1}])
        stream = Checks(client)
        self.assertTrue(stream.check_access())

    def test_returns_true_when_stream_empty(self):
        """check_access returns True when the stream yields no records (but no 403)."""
        from tap_clubspeed.streams import Checks

        client = MagicMock()
        client.checks.return_value = iter([])
        stream = Checks(client)
        self.assertTrue(stream.check_access())

    def test_returns_false_on_forbidden(self):
        """check_access returns False when the client raises ClubspeedForbiddenError."""
        from tap_clubspeed.streams import Checks

        client = MagicMock()
        client.checks.side_effect = ClubspeedForbiddenError("403 Forbidden")
        stream = Checks(client)
        self.assertFalse(stream.check_access())

    def test_returns_true_with_none_client(self):
        """check_access returns True (skips network) when no client is set."""
        from tap_clubspeed.streams import Checks

        stream = Checks(None)
        self.assertTrue(stream.check_access())

    def test_returns_true_when_client_method_missing(self):
        """check_access returns True when the client has no method for the stream."""
        from tap_clubspeed.streams import Stream

        client = MagicMock(spec=[])  # spec with no attributes
        stream = Stream(client)
        stream.name = "nonexistent_stream"
        self.assertTrue(stream.check_access())

    def test_limit_set_to_1_during_probe_and_restored(self):
        """check_access temporarily sets _limit=1 and restores the original value."""
        from tap_clubspeed.streams import Checks

        client = MagicMock()
        client._limit = 100
        observed_limits = []

        def capture_limit(column_name=None, bookmark=None):
            observed_limits.append(client._limit)
            return iter([])

        client.checks.side_effect = capture_limit
        stream = Checks(client)
        stream.check_access()

        self.assertEqual([1], observed_limits)
        self.assertEqual(100, client._limit)


class TestApplyAccessChecks(unittest.TestCase):
    """_apply_access_checks filters inaccessible streams from the catalog."""

    def setUp(self):
        self.client = MagicMock(spec=Clubspeed)
        self.entries = [
            {"stream": "checks", "tap_stream_id": "checks", "schema": {}, "metadata": []},
            {"stream": "booking", "tap_stream_id": "booking", "schema": {}, "metadata": []},
        ]

    def test_all_accessible_returns_all_entries(self):
        """All entries are returned when every stream is accessible."""
        from tap_clubspeed.streams import Stream

        with patch.object(Stream, "check_access", return_value=True):
            result = _apply_access_checks(self.client, self.entries)

        self.assertEqual(len(result), 2)

    def test_partial_access_excludes_forbidden_stream(self):
        """Streams returning 403 are excluded; accessible ones remain."""
        from tap_clubspeed.streams import Stream

        def selective_check(self_stream):
            return self_stream.name != "checks"

        with patch.object(Stream, "check_access", selective_check):
            result = _apply_access_checks(self.client, self.entries)

        names = [e["tap_stream_id"] for e in result]
        self.assertNotIn("checks", names)
        self.assertIn("booking", names)
        self.assertEqual(len(result), 1)

    def test_all_inaccessible_raises_exception(self):
        """Exception is raised when no streams are accessible at all."""
        from tap_clubspeed.streams import Stream

        with patch.object(Stream, "check_access", return_value=False):
            with self.assertRaisesRegex(Exception, "No streams are accessible"):
                _apply_access_checks(self.client, self.entries)

    def test_single_accessible_stream_succeeds(self):
        """Discovery succeeds if at least one stream is accessible."""
        from tap_clubspeed.streams import Stream

        def only_booking(self_stream):
            return self_stream.name == "booking"

        with patch.object(Stream, "check_access", only_booking):
            result = _apply_access_checks(self.client, self.entries)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tap_stream_id"], "booking")


class TestDiscoverStreamsWithAccessChecks(unittest.TestCase):
    """discover_streams integrates access checks end-to-end."""

    def setUp(self):
        self.client = MagicMock(spec=Clubspeed)

    def test_full_catalog_when_all_accessible(self):
        """discover_streams returns all streams when all are accessible."""
        from tap_clubspeed.streams import Stream

        with patch.object(Stream, "check_access", return_value=True):
            result = discover_streams(self.client)

        self.assertEqual(len(result), len(STREAMS))

    def test_excluded_stream_missing_from_catalog(self):
        """A forbidden stream is absent from the returned catalog."""
        from tap_clubspeed.streams import Stream

        def block_checks(self_stream):
            return self_stream.name != "checks"

        with patch.object(Stream, "check_access", block_checks):
            result = discover_streams(self.client)

        names = [e["tap_stream_id"] for e in result]
        self.assertNotIn("checks", names)
        self.assertEqual(len(result), len(STREAMS) - 1)

    def test_all_blocked_raises(self):
        """discover_streams raises when every stream is blocked."""
        from tap_clubspeed.streams import Stream

        with patch.object(Stream, "check_access", return_value=False):
            with self.assertRaises(Exception):
                discover_streams(self.client)


if __name__ == "__main__":
    unittest.main()
