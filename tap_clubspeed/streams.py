import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse


logger = singer.get_logger()
KEY_PROPERTIES = ['id']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try: 
            parse(string)
            return True
        except ValueError:
            return False
    return False


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES


    def __init__(self, client=None):
        self.client = client


    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)


    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and needs_parse_to_date(value) and needs_parse_to_date(current_bookmark):
            if utils.strptime_with_tz(value) > utils.striptime_with_tz(current_bookmark):
                singer.write_bookmark(state, self.name, self.replication_key, value)
        elif current_bookmark is None:
            singer.write_bookmark(state, self.name, self.replication_key, value)


    # This function returns boolean and checks if
    # book mark is old.
    def is_bookmark_old(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if current_bookmark is None:
            return True
        if needs_parse_to_date(current_bookmark) and needs_parse_to_date(value):
            if utils.strptime_with_tz(value) >= utils.strptime_with_tz(current_bookmark):
                return True
        else:
            if value >= current_bookmark:
                return True
        return False


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)


    def _add_custom_fields(self, schema): # pylint: disable=no-self-use
        return schema


    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


    def is_selected(self):
        return self.stream is not None


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        if self.replication_method == "INCREMENTAL":
            bookmark = self.get_bookmark(state)
            res = get_data(self.replication_key, bookmark)
            for item in res:
                if self.is_bookmark_old(state, item[self.replication_key]):
                    yield (self.stream, item)
        elif self.replication_method == "FULL_TABLE":
            res = get_data()
            for item in res:
                yield (self.stream, item)
        else:
            raise Exception('Replication key not defined for {stream}'.format(self.name))



class Booking(Stream):
    name = "booking"
    replication_method = "INCREMENTAL"
    replication_key = "onlineBookingsId"
    key_properties = ["onlineBookingsId"]


class BookingAvailability(Stream):
    name = "booking_availability"
    replication_method = "INCREMENTAL"
    replication_key = "heatId"
    key_properties = ["heatId"]


class CheckDetails(Stream):
    name = "check_details"
    replication_method = "INCREMENTAL"
    replication_key = "createdDate"
    key_properties = [ "checkDetailId" ]


class Checks(Stream):
    name = "checks"
    replication_method = "INCREMENTAL"
    replication_key = "openedDate"
    key_properties = [ "checkId" ]


class Customers(Stream):
    name = "customers"
    replication_method = "INCREMENTAL"
    replication_key = "accountCreated"
    key_properties = [ "customerId" ]


class DiscountTypes(Stream):
    name = "discount_types"
    replication_method = "INCREMENTAL"
    key_properties = ["discountId"]
    replication_key = key_properties[0]


class EventHeatDetails(Stream):
    name = "event_heat_details"
    replication_method = "INCREMENTAL"
    replication_key = "added" 
    key_properties = [ "eventId" ]


class EventHeatTypes(Stream):
    name = "event_heat_types"
    replication_method = "INCREMENTAL"
    key_properties = ["eventHeatTypeId"]
    replication_key = key_properties[0]


class EventReservationLinks(Stream): 
    name = "event_reservation_links"
    replication_method = "INCREMENTAL"
    key_properties = ["eventReservationLinkId"]
    replication_key = key_properties[0]


class EventReservations(Stream):
    name = "event_reservations"
    replication_method = "INCREMENTAL"
    replication_key = "startTime"
    key_properties = [ "eventReservationId" ]


class EventReservationTypes(Stream):
    name = "event_reservation_types"
    replication_method = "INCREMENTAL"
    key_properties = ["eventReservationTypeId"]
    replication_key = key_properties[0]


class EventRounds(Stream):
    name = "event_rounds"
    replication_method = "INCREMENTAL"
    key_properties = ["eventRoundId"]
    replication_key = key_properties[0]


class Events(Stream):
    name = "events"
    replication_method = "INCREMENTAL"
    replication_key = "createdHeatTime"
    key_properties = [ "eventId" ]


class EventStatuses(Stream):
    name = "event_statuses"
    replication_method = "INCREMENTAL"
    key_properties = ["eventStatusId"]
    replication_key = key_properties[0]


class EventTasks(Stream):
    name = "event_tasks"
    replication_method = "INCREMENTAL"
    replication_key = "completedAt"
    key_properties = [ "eventTaskId" ]


class EventTaskTypes(Stream):
    name = "event_task_types"
    replication_method = "INCREMENTAL"
    key_properties = [ "eventTaskId" ]
    replication_key = key_properties[0]


class EventTypes(Stream):
    name = "event_types"
    replication_method = "INCREMENTAL"
    key_properties = [ "eventTypeId" ]
    replication_key = key_properties[0]


class GiftCardHistory(Stream):
    name = "gift_card_history"
    replication_method = "INCREMENTAL"
    replication_key = "transactionDate"
    key_properties = [ "giftCardHistoryId" ]


class HeatDetails(Stream):
    name = "heat_details"
    replication_method = "INCREMENTAL"
    replication_key = "timeAdded"
    key_properties = [ "heatId" ]


class HeatMain(Stream):
    name = "heat_main"
    replication_method = "INCREMENTAL"
    replication_key = "heatId"
    key_properties = [ "heatId" ]


class HeatTypes(Stream):
    name = "heat_types"
    replication_method = "INCREMENTAL"
    key_properties = ["heatTypesId"]
    replication_key = key_properties[0]



class Memberships(Stream):
    name = "memberships"
    replication_method = "INCREMENTAL"
    replication_key = "membershipTypeId"
    key_properties = [ "membershipTypeId" ]


class MembershipTypes(Stream):
    name = "membership_types"
    replication_method = "INCREMENTAL"
    key_properties = ["membershipTypeId"]
    replication_key = key_properties[0]


class Payments(Stream):
    name = "payments"
    replication_method = "INCREMENTAL"
    replication_key = "payDate"
    key_properties = [ "paymentId" ]


class ProductClasses(Stream):
    name = "product_classes"
    replication_method = "INCREMENTAL"
    key_properties = ["productClassId"]
    replication_key = key_properties[0]


class Products(Stream):
    name = "products"
    replication_method = "INCREMENTAL"
    key_properties = ["productId"]
    replication_key = key_properties[0]


class Reservations(Stream):
    name = "reservations"
    replication_method = "INCREMENTAL"
    replication_key = "createdAt"
    key_properties = [ "onlineBookingReservationsId" ]


class Sources(Stream):
    name = "sources"
    replication_method = "INCREMENTAL"
    key_properties = ["sourceId"]
    replication_key = key_properties[0]


class Taxes(Stream):
    name = "taxes"
    replication_method = "INCREMENTAL"
    key_properties = ["taxId"]
    replication_key = key_properties[0]


class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    key_properties = ["userId"]
    replication_key = key_properties[0]



STREAMS = {
    "booking": Booking,
    "booking_availability": BookingAvailability,
    "check_details": CheckDetails,
    "checks": Checks,
    "customers": Customers,
    "discount_types": DiscountTypes,
    "event_heat_details": EventHeatDetails,
    "event_heat_types": EventHeatTypes,
    "event_reservation_links": EventReservationLinks,
    "event_reservations": EventReservations,
    "event_reservation_types": EventReservationTypes,
    "event_rounds": EventRounds,
    "events": Events,
    "event_statuses": EventStatuses,
    "event_tasks": EventTasks,
    "event_task_types": EventTaskTypes,
    "event_types": EventTypes,
    "gift_card_history": GiftCardHistory,
    "heat_details": HeatDetails,
    "heat_main": HeatMain,
    "heat_types": HeatTypes,
    "memberships": Memberships,
    "membership_types": MembershipTypes,
    "payments": Payments,
    "product_classes": ProductClasses,
    "products": Products,
    "reservations": Reservations,
    "sources": Sources,
    "taxes": Taxes,
    "users": Users
}
