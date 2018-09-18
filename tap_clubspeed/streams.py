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


class Booking(Stream):
    name = "booking"
    replication_method = "FULL_TABLE"
    key_properties = ["onlineBookingsId"]

    def sync(self, state): 
        bookings = self.client.booking()
        for booking in bookings:
            yield (self.stream, booking)


class BookingAvailability(Stream):
    name = "booking_availability"
    replication_method = "FULL_TABLE"
    key_properties = ["heatId"]

    def sync(self, state):
        availabilities = self.client.booking_availability()
        for availability in availabilities:
            yield (self.stream, availability)


class CheckDetails(Stream):
    name = "check_details"
    replication_method = "INCREMENTAL"
    replication_key = "createdDate"
    key_properties = [ "checkDetailId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        check_details = self.client.check_details(self.replication_key, bookmark)
        for check_detail in check_details:
            if self.is_bookmark_old(state, check_detail[self.replication_key]):
                yield (self.stream, check_detail)



class Checks(Stream):
    name = "checks"
    replication_method = "INCREMENTAL"
    replication_key = "openedDate"
    key_properties = [ "checkId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        checks = self.client.checks(self.replication_key, bookmark)
        for check in checks:
            if self.is_bookmark_old(state, check[self.replication_key]):
                yield (self.stream, check)


class Customers(Stream):
    name = "customers"
    replication_method = "INCREMENTAL"
    replication_key = "accountCreated"
    key_properties = [ "customerId" ]

    # https://rpmstamford.clubspeedtiming.com/api/index.php/customers.json?key=Pc8BULWF4Mp8MSZv&page=1051&limit=100

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        customers = self.client.customers(self.replication_key, bookmark)
        for customer in customers:
            if self.is_bookmark_old(state, customer[self.replication_key]):
                yield (self.stream, customer)



class DiscountTypes(Stream):
    name = "discount_types"
    replication_method = "FULL_TABLE"
    key_properties = ["discountId"]

    def sync(self, state):
        discount_types = self.client.discount_types()
        for discount_type in discount_types:
            yield (self.stream, discount_type)


class EventHeatDetails(Stream):
    name = "event_heat_details"
    replication_method = "INCREMENTAL"
    replication_key = "added" 
    key_properties = [ "eventId" ]
    # Key also could be "added". Docs are unclear.

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        event_heat_details = self.client.event_heat_details(self.replication_key, bookmark)
        for event_heat_detail in event_heat_details:
            if self.is_bookmark_old(state, event_heat_detail[self.replication_key]):
                yield (self.stream, event_heat_detail)


class EventHeatTypes(Stream):
    name = "event_heat_types"
    replication_method = "FULL_TABLE"
    key_properties = ["eventHeatTypeId"]

    def sync(self, state):
        event_heat_types = self.client.event_heat_types()
        for event_heat_type in event_heat_types:
            yield (self.stream, event_heat_type)


class EventReservationLinks(Stream): 
    name = "event_reservation_links"
    replication_method = "FULL_TABLE"
    key_properties = ["eventReservationLinkId"]

    def sync(self, state):
        event_reservation_links = self.client.event_reservation_links()
        for event_reservation_link in event_reservation_links:
            yield (self.stream, event_reservation_link)


class EventReservations(Stream):
    name = "event_reservations"
    replication_method = "INCREMENTAL"
    replication_key = "startTime"
    key_properties = [ "eventReservationId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        event_reservations = self.client.event_reservations(self.replication_key, bookmark)
        for event_reservation in event_reservations:
            if self.is_bookmark_old(state, event_reservation[self.replication_key]):
                yield (self.stream, event_reservation)



class EventReservationTypes(Stream):
    name = "event_reservation_types"
    replication_method = "FULL_TABLE"
    key_properties = ["eventReservationTypeId"]

    def sync(self, state):
        event_reservation_types = self.client.event_reservation_types()
        for event_reservation_type in event_reservation_types:
            yield (self.stream, event_reservation_type)



class EventRounds(Stream):
    name = "event_rounds"
    replication_method = "FULL_TABLE"
    key_properties = ["eventRoundId"]

    def sync(self, state):
        event_rounds = self.client.event_rounds()
        for event_round in event_rounds:
            yield (self.stream, event_round)


class Events(Stream):
    name = "events"
    replication_method = "INCREMENTAL"
    replication_key = "createdHeatTime"
    key_properties = [ "eventId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        events = self.client.events(self.replication_key, bookmark)
        for event in events:
            if self.is_bookmark_old(state, event[self.replication_key]):
                yield (self.stream, event)


class EventStatuses(Stream):
    name = "event_statuses"
    replication_method = "FULL_TABLE"
    key_properties = ["eventStatusId"]

    def sync(self, state):
        event_statuses = self.client.event_statuses()
        for event_status in event_statuses:
            yield (self.stream, event_status)


class EventTasks(Stream):
    name = "event_tasks"
    replication_method = "INCREMENTAL"
    replication_key = "completedAt"
    key_properties = [ "eventTaskId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        event_tasks = self.client.event_tasks(self.replication_key, bookmark)
        for event_task in event_tasks:
            if self.is_bookmark_old(state, event_task[self.replication_key]):
                yield (self.stream, event_task)


class EventTaskTypes(Stream):
    name = "event_task_types"
    replication_method = "FULL_TABLE"
    key_properties = [ "eventTaskTypeId" ]

    def sync(self, state):
        event_task_types = self.client.event_task_types()
        for event_task_type in event_task_types:
            yield (self.stream, event_task_type)


class EventTypes(Stream):
    name = "event_types"
    replication_method = "FULL_TABLE"
    key_properties = [ "eventTypeId" ]

    def sync(self, state):
        event_types = self.client.event_types()
        for event_type in event_types:
            yield (self.stream, event_type)


class GiftCardHistory(Stream):
    name = "gift_card_history"
    replication_method = "INCREMENTAL"
    replication_key = "transactionDate"
    key_properties = [ "giftCardHistoryId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        histories = self.client.gift_card_history(self.replication_key, bookmark)
        for history in histories:
            if self.is_bookmark_old(state, history[self.replication_key]):
                yield (self.stream, history)


class HeatDetails(Stream):
    name = "heat_details"
    replication_method = "INCREMENTAL"
    replication_key = "timeAdded"
    key_properties = [ "heatId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        heat_details = self.client.heat_details(self.replication_key, bookmark)
        for heat_detail in heat_details:
            if self.is_bookmark_old(state, heat_detail[self.replication_key]):
                yield (self.stream, heat_detail)


class HeatMain(Stream):
    name = "heat_main"
    replication_method = "INCREMENTAL"
    replication_key = "heatId"
    key_properties = [ "heatId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        heat_main = self.client.heat_main(self.replication_key, bookmark)
        for item in heat_main:
            if self.is_bookmark_old(state, item[self.replication_key]):
                yield (self.stream, item)


class HeatTypes(Stream):
    name = "heat_types"
    replication_method = "FULL_TABLE"
    key_properties = ["heatTypesId"]

    def sync(self, state):
        heat_types = self.client.heat_types()
        for heat_type in heat_types:
            yield (self.stream, heat_type)



class Memberships(Stream):
    name = "memberships"
    replication_method = "INCREMENTAL"
    replication_key = "membershipTypeId"
    key_properties = [ "membershipTypeId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        memberships = self.client.memberships(self.replication_key, bookmark)
        for item in memberships:
            if self.is_bookmark_old(state, item[self.replication_key]):
                yield (self.stream, item)


class MembershipTypes(Stream):
    name = "membership_types"
    replication_method = "FULL_TABLE"
    key_properties = ["membershipTypeId"]

    def sync(self, state):
        membership_types = self.client.membership_types()
        for membership_type in membership_types:
            yield (self.stream, membership_type)


class Payments(Stream):
    name = "payments"
    replication_method = "INCREMENTAL"
    replication_key = "payDate"
    key_properties = [ "paymentId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        payments = self.client.payments(self.replication_key, bookmark)
        for item in payments:
            if self.is_bookmark_old(state, item[self.replication_key]):
                yield (self.stream, item)


class ProductClasses(Stream):
    name = "product_classes"
    replication_method = "FULL_TABLE"
    key_properties = ["productClassId"]

    def sync(self, state):
        product_classes = self.client.product_classes()
        for product_class in product_classes:
            yield (self.stream, product_class)


class Products(Stream):
    name = "products"
    replication_method = "FULL_TABLE"
    key_properties = ["productId"]

    def sync(self, state):
        products = self.client.products()
        for product in products:
            yield (self.stream, product)


# class Racers(Stream):


class Reservations(Stream):
    name = "reservations"
    replication_method = "INCREMENTAL"
    replication_key = "createdAt"
    key_properties = [ "onlineBookingReservationsId" ]

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        reservations = self.client.reservations(self.replication_key, bookmark)
        for item in reservations:
            if self.is_bookmark_old(state, item[self.replication_key]):
                yield (self.stream, item)


class Sources(Stream):
    name = "sources"
    replication_method = "FULL_TABLE"
    key_properties = ["sourceId"]

    def sync(self, state):
        sources = self.client.sources()
        for source in sources:
            yield (self.stream, source)


class Taxes(Stream):
    name = "taxes"
    replication_method = "FULL_TABLE"
    key_properties = ["taxId"]

    def sync(self, state):
        taxes = self.client.taxes()
        for tax in taxes:
            yield (self.stream, tax)


class Users(Stream):
    name = "users"
    replication_method = "FULL_TABLE"
    key_properties = ["userId"]

    def sync(self, state):
        users = self.client.users()
        for user in users:
            yield (self.stream, user)




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
