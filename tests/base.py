"""
Base test case for tap-clubspeed integration tests with mocked data.

This is a plain mixin — not a TestCase itself. Mix with unittest.TestCase
in each test class. No tap-tester or Singer/Stitch infrastructure required.
"""
import json
import os


class MockResponse:
    """Minimal requests.Response stand-in."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class ClubspeedBaseTest:
    """Base test mixin for tap-clubspeed integration tests.

    All constants, metadata, and schema-driven mock-data helpers live here.
    Inherit from this class alongside unittest.TestCase in each test module.

    Tap          : tap-clubspeed
    Package      : tap_clubspeed
    Connection   : platform.clubspeed
    Credentials  : private_key  (env: TAP_CLUBSPEED_PRIVATE_KEY)
    Properties   : subdomain    (env: TAP_CLUBSPEED_SUBDOMAIN)
    """

    # ── Metadata constants ───────────────────────────────────────────────
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    API_LIMIT = "api_limit"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    default_start_date = "2020-01-01T00:00:00Z"

    # ── Stream metadata ──────────────────────────────────────────────────

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about each stream."""
        return {
            "booking": {
                cls.PRIMARY_KEYS: {"onlineBookingsId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "booking_availability": {
                cls.PRIMARY_KEYS: {"heatId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "check_details": {
                cls.PRIMARY_KEYS: {"checkDetailId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"createdDate"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "checks": {
                cls.PRIMARY_KEYS: {"checkId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"closedDate"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "customers": {
                cls.PRIMARY_KEYS: {"customerId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastVisited"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "discount_types": {
                cls.PRIMARY_KEYS: {"discountId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_heat_details": {
                cls.PRIMARY_KEYS: {"eventId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"added"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_heat_types": {
                cls.PRIMARY_KEYS: {"eventHeatTypeId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_reservation_links": {
                cls.PRIMARY_KEYS: {"eventReservationLinkId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_reservations": {
                cls.PRIMARY_KEYS: {"eventReservationId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"startTime"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_reservation_types": {
                cls.PRIMARY_KEYS: {"eventReservationTypeId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_rounds": {
                cls.PRIMARY_KEYS: {"eventRoundId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "events": {
                cls.PRIMARY_KEYS: {"eventId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"eventScheduledTime"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_statuses": {
                cls.PRIMARY_KEYS: {"eventStatusId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_tasks": {
                cls.PRIMARY_KEYS: {"eventTaskId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"completedAt"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_task_types": {
                cls.PRIMARY_KEYS: {"eventTaskId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "event_types": {
                cls.PRIMARY_KEYS: {"eventTypeId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "gift_card_history": {
                cls.PRIMARY_KEYS: {"giftCardHistoryId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"transactionDate"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "heat_main": {
                cls.PRIMARY_KEYS: {"heatId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"finish"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "heat_main_details": {
                # Special stream: uses heat_main's bookmark via _new_heats list.
                # replication_key is the string "None" by tap design.
                cls.PRIMARY_KEYS: {"heatId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"None"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "heat_types": {
                cls.PRIMARY_KEYS: {"heatTypesId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "memberships": {
                cls.PRIMARY_KEYS: {"membershipTypeId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"changed"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "membership_types": {
                cls.PRIMARY_KEYS: {"membershipTypeId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "payments": {
                cls.PRIMARY_KEYS: {"paymentId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"payDate"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "payments_voided": {
                cls.PRIMARY_KEYS: {"paymentId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"voidDate"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "product_classes": {
                cls.PRIMARY_KEYS: {"productClassId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "products": {
                cls.PRIMARY_KEYS: {"productId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "reservations": {
                cls.PRIMARY_KEYS: {"onlineBookingReservationsId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"createdAt"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "sources": {
                cls.PRIMARY_KEYS: {"sourceId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "taxes": {
                cls.PRIMARY_KEYS: {"taxId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "users": {
                cls.PRIMARY_KEYS: {"userId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
        }

    @classmethod
    def expected_stream_names(cls):
        return set(cls.expected_metadata().keys())

    @classmethod
    def incremental_streams(cls):
        return {
            name
            for name, meta in cls.expected_metadata().items()
            if meta[cls.REPLICATION_METHOD] == cls.INCREMENTAL
        }

    @classmethod
    def full_table_streams(cls):
        return {
            name
            for name, meta in cls.expected_metadata().items()
            if meta[cls.REPLICATION_METHOD] == cls.FULL_TABLE
        }

    # ── Fixture / config helpers ─────────────────────────────────────────

    def setUp(self):
        self.config = self.get_mock_config()
        self.state = {}

    def tearDown(self):
        pass

    @staticmethod
    def get_mock_config():
        """Dummy config — no real credentials. Safe for CI use."""
        return {
            "subdomain": "mock_subdomain",
            "private_key": "mock_private_key",
        }

    @staticmethod
    def get_mock_state():
        return {}

    # ── Schema-driven mock-data generation ───────────────────────────────

    @staticmethod
    def _schema_path(stream_name):
        base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return os.path.join(base_dir, "tap_clubspeed", "schemas",
                            f"{stream_name}.json")

    @classmethod
    def _load_schema(cls, stream_name):
        with open(cls._schema_path(stream_name), "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _schema_type(schema):
        """Return the concrete (non-null) type from a JSON-schema fragment."""
        t = schema.get("type", "object")
        if isinstance(t, list):
            non_null = [x for x in t if x != "null"]
            return non_null[0] if non_null else "null"
        return t

    @staticmethod
    def _generate_value(schema, date_value="2024-01-15T00:00:00Z"):
        """Recursively generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        schema_type = ClubspeedBaseTest._schema_type(schema)
        if schema_type == "object":
            properties = schema.get("properties", {})
            return {
                key: ClubspeedBaseTest._generate_value(val, date_value)
                for key, val in properties.items()
            }
        if schema_type == "array":
            item_schema = schema.get("items", {"type": "string"})
            return [ClubspeedBaseTest._generate_value(item_schema, date_value)]
        if schema_type == "string":
            fmt = schema.get("format")
            if fmt == "date-time":
                return date_value
            if fmt == "email":
                return "mock@example.com"
            return "mock"
        return {"integer": 1, "number": 1.0, "boolean": True}.get(schema_type)

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value="2024-01-15T00:00:00Z"):
        """Generate one schema-valid mock record for the given stream."""
        return cls._generate_value(cls._load_schema(stream_name),
                                   date_value=date_value)

    # ── HTTP mock helpers ─────────────────────────────────────────────────
    # The tap's HTTP function is tap_clubspeed.clubspeed.requests.get
    # Patch target: "tap_clubspeed.clubspeed.requests.get"

    @classmethod
    def make_get_response(cls, records, key=None):
        """
        Build a MockResponse that matches _get_response(endpoint, key=...) usage.

        When the stream uses a key (e.g. key='bookings'), the response body must be:
            {key: [records]}
        When no key is used, the body is the list directly.
        """
        if key is not None:
            payload = {key: records}
        else:
            payload = records
        return MockResponse(payload)

    @classmethod
    def make_paginated_side_effect(cls, pages, key=None):
        """
        Build a list of MockResponse objects representing consecutive pages.

        The last element is always an empty page to signal end-of-pagination.
        pages: list of record-lists, e.g. [[rec1, rec2], [rec3]]
        """
        responses = [cls.make_get_response(page, key) for page in pages]
        # Append the terminal empty page
        responses.append(cls.make_get_response([] if key is None else [], key))
        return responses
