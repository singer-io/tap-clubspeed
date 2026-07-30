"""
Integration tests for tap-clubspeed stream discovery with mocked data.

Calls tap_clubspeed.discover.discover_streams() directly — no HTTP calls needed
because discovery only reads JSON schema files and builds metadata in-memory.
"""
import unittest
from unittest.mock import MagicMock, patch

from singer import metadata

try:
    from .base import ClubspeedBaseTest
except ImportError:
    from base import ClubspeedBaseTest

from tap_clubspeed.clubspeed import Clubspeed, ClubspeedForbiddenError
from tap_clubspeed.discover import discover_streams
from tap_clubspeed.streams import STREAMS


def _get_catalog():
    """Run discover_streams with a mock client (no HTTP calls needed)."""
    client = MagicMock(spec=Clubspeed)
    return discover_streams(client)


class ClubspeedDiscoveryTest(ClubspeedBaseTest, unittest.TestCase):
    """Verify stream discovery returns the expected catalog structure."""

    def setUp(self):
        super().setUp()
        self.catalog = _get_catalog()
        self.catalog_by_name = {s["tap_stream_id"]: s for s in self.catalog}

    # ── Quantity checks ──────────────────────────────────────────────────

    def test_discovery_returns_all_expected_streams(self):
        """discover_streams returns exactly the set of expected stream names."""
        discovered = set(self.catalog_by_name.keys())
        self.assertEqual(discovered, self.expected_stream_names())

    def test_discovery_count_matches_streams_registry(self):
        """Number of catalog entries equals the STREAMS registry length."""
        self.assertEqual(len(STREAMS), len(self.catalog))

    # ── Schema structure ─────────────────────────────────────────────────

    def test_every_stream_has_properties_in_schema(self):
        """Every stream's schema contains a 'properties' object."""
        for entry in self.catalog:
            with self.subTest(stream=entry["tap_stream_id"]):
                self.assertIn("properties", entry["schema"])
                self.assertGreater(len(entry["schema"]["properties"]), 0)

    def test_every_stream_has_metadata_list(self):
        """Every stream has a non-empty metadata list."""
        for entry in self.catalog:
            with self.subTest(stream=entry["tap_stream_id"]):
                self.assertIsInstance(entry["metadata"], list)
                self.assertGreater(len(entry["metadata"]), 0)

    # ── Primary keys ─────────────────────────────────────────────────────

    def test_primary_keys_match_expected(self):
        """Primary keys in table-key-properties match expected_metadata."""
        expected = self.expected_metadata()
        for stream_name, exp_meta in expected.items():
            with self.subTest(stream=stream_name):
                entry = self.catalog_by_name[stream_name]
                mdata = metadata.to_map(entry["metadata"])
                actual_pks = set(
                    metadata.get(mdata, (), "table-key-properties") or []
                )
                self.assertEqual(exp_meta[self.PRIMARY_KEYS], actual_pks)

    # ── Replication method ───────────────────────────────────────────────

    def test_replication_method_matches_expected(self):
        """forced-replication-method matches expected_metadata for every stream."""
        expected = self.expected_metadata()
        for stream_name, exp_meta in expected.items():
            with self.subTest(stream=stream_name):
                entry = self.catalog_by_name[stream_name]
                mdata = metadata.to_map(entry["metadata"])
                actual_method = metadata.get(mdata, (), "forced-replication-method")
                self.assertEqual(exp_meta[self.REPLICATION_METHOD], actual_method)

    def test_full_table_streams_have_no_replication_key_in_metadata(self):
        """FULL_TABLE streams must NOT have valid-replication-keys set."""
        for stream_name in self.full_table_streams():
            with self.subTest(stream=stream_name):
                entry = self.catalog_by_name[stream_name]
                mdata = metadata.to_map(entry["metadata"])
                rep_keys = metadata.get(mdata, (), "valid-replication-keys")
                self.assertFalse(
                    rep_keys,
                    msg=f"{stream_name} (FULL_TABLE) should have no valid-replication-keys",
                )

    def test_incremental_streams_have_replication_key_in_metadata(self):
        """INCREMENTAL streams must have at least one valid-replication-key.

        heat_main_details uses the string key "None" by tap design.
        """
        for stream_name in self.incremental_streams():
            with self.subTest(stream=stream_name):
                entry = self.catalog_by_name[stream_name]
                mdata = metadata.to_map(entry["metadata"])
                rep_keys = metadata.get(mdata, (), "valid-replication-keys") or []
                self.assertGreater(
                    len(rep_keys), 0,
                    msg=f"{stream_name} (INCREMENTAL) must have valid-replication-keys",
                )

    # ── Field inclusion ──────────────────────────────────────────────────

    def test_key_property_fields_have_automatic_inclusion(self):
        """Primary-key fields must have inclusion=automatic in metadata."""
        expected = self.expected_metadata()
        for stream_name, exp_meta in expected.items():
            with self.subTest(stream=stream_name):
                entry = self.catalog_by_name[stream_name]
                mdata = metadata.to_map(entry["metadata"])
                for pk in exp_meta[self.PRIMARY_KEYS]:
                    inclusion = metadata.get(mdata, ("properties", pk), "inclusion")
                    self.assertEqual(
                        "automatic", inclusion,
                        msg=f"{stream_name}.{pk} should have inclusion=automatic",
                    )

    def test_non_key_fields_have_available_inclusion(self):
        """Non-PK, non-replication-key fields must have inclusion=available."""
        expected = self.expected_metadata()
        for stream_name, exp_meta in expected.items():
            entry = self.catalog_by_name[stream_name]
            mdata = metadata.to_map(entry["metadata"])
            automatic_fields = (
                exp_meta[self.PRIMARY_KEYS] | exp_meta[self.REPLICATION_KEYS]
            )
            schema_props = entry["schema"].get("properties", {})
            for field in schema_props:
                if field in automatic_fields:
                    continue
                with self.subTest(stream=stream_name, field=field):
                    inclusion = metadata.get(mdata, ("properties", field), "inclusion")
                    self.assertEqual(
                        "available", inclusion,
                        msg=f"{stream_name}.{field} should have inclusion=available",
                    )

    # ── Spot-check specific streams ──────────────────────────────────────

    def test_checks_stream_replication_key_is_closed_date(self):
        """The 'checks' stream uses closedDate as its replication key."""
        entry = self.catalog_by_name["checks"]
        mdata = metadata.to_map(entry["metadata"])
        rep_keys = metadata.get(mdata, (), "valid-replication-keys") or []
        self.assertIn("closedDate", rep_keys)

    def test_payments_stream_primary_key_is_payment_id(self):
        """The 'payments' stream has paymentId as its primary key."""
        entry = self.catalog_by_name["payments"]
        mdata = metadata.to_map(entry["metadata"])
        pks = metadata.get(mdata, (), "table-key-properties") or []
        self.assertIn("paymentId", pks)

    def test_booking_stream_is_full_table(self):
        """The 'booking' stream uses FULL_TABLE replication."""
        entry = self.catalog_by_name["booking"]
        mdata = metadata.to_map(entry["metadata"])
        method = metadata.get(mdata, (), "forced-replication-method")
        self.assertEqual("FULL_TABLE", method)


class ClubspeedDiscoveryExclusionTest(unittest.TestCase):
    """Integration tests: unauthorized (403) streams are excluded from the catalog."""

    def _catalog_with_one_blocked(self, blocked_stream: str):
        """Return a catalog where `blocked_stream` raises ClubspeedForbiddenError."""
        from tap_clubspeed.streams import Stream

        def selective_check_access(self_stream):
            return self_stream.name != blocked_stream

        client = MagicMock(spec=Clubspeed)
        with patch.object(Stream, "check_access", selective_check_access):
            return discover_streams(client)

    def test_forbidden_stream_not_in_catalog(self):
        """A stream returning 403 is absent from the discovered catalog."""
        catalog = self._catalog_with_one_blocked("checks")
        names = {e["tap_stream_id"] for e in catalog}
        self.assertNotIn("checks", names)

    def test_authorized_streams_still_in_catalog(self):
        """All streams other than the blocked one remain in the catalog."""
        blocked = "checks"
        catalog = self._catalog_with_one_blocked(blocked)
        names = {e["tap_stream_id"] for e in catalog}
        expected = set(STREAMS.keys()) - {blocked}
        self.assertEqual(expected, names)

    def test_warning_logged_for_excluded_stream(self):
        """A LOGGER.warning is emitted for the excluded stream."""
        from tap_clubspeed.streams import Stream

        client = MagicMock(spec=Clubspeed)

        def block_checks(self_stream):
            return self_stream.name != "checks"

        with patch.object(Stream, "check_access", block_checks):
            with self.assertLogs(level="WARNING") as cm:
                discover_streams(client)

        self.assertTrue(any("checks" in line for line in cm.output))

    def test_all_streams_blocked_raises_exception(self):
        """discover_streams raises when every stream returns 403."""
        from tap_clubspeed.streams import Stream

        client = MagicMock(spec=Clubspeed)
        with patch.object(Stream, "check_access", return_value=False):
            with self.assertRaisesRegex(Exception, "No streams are accessible"):
                discover_streams(client)

    def test_single_authorized_stream_succeeds(self):
        """Discovery returns a catalog as long as at least one stream is accessible."""
        from tap_clubspeed.streams import Stream

        client = MagicMock(spec=Clubspeed)
        only_accessible = list(STREAMS.keys())[0]

        def allow_one(self_stream):
            return self_stream.name == only_accessible

        with patch.object(Stream, "check_access", allow_one):
            catalog = discover_streams(client)

        self.assertEqual(1, len(catalog))
        self.assertEqual(only_accessible, catalog[0]["tap_stream_id"])


if __name__ == "__main__":
    unittest.main()
