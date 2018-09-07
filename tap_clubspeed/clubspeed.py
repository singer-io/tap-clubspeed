
import requests
import logging

log = logging.getLogger()


class Clubspeed(object):


    def __init__(self, subdomain=None, private_key=None, session=None):
        """ Simple Python wrapper for the Clubspeed API. Only supports GET. """
        self.protocol = 'https'
        self.domain = 'clubspeedtiming.com'
        self.subdomain = subdomain
        self.api_prefix = 'api/index.php/'
        self.private_key = private_key
        self._url_template = "{protocol}://{subdomain}.{domain}/{api_prefix}{path}.json?key={private_key}"


    def _get(self, url, **kwargs):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()


    def _construct_endpoint(self, path):
        return self._url_template.format(protocol=self.protocol, 
                                         subdomain=self.subdomain, 
                                         domain=self.domain, 
                                         api_prefix=self.api_prefix,
                                         path=path,
                                         private_key=self.private_key)


    def is_authorized(self):
        endpoint = self._construct_endpoint('payments')
        return self._get(endpoint)


    def booking(self):
        endpoint = self._construct_endpoint('booking')
        json = self._get(endpoint)
        return json['bookings']


    def booking_availability(self):
        endpoint = self._construct_endpoint('bookingAvailability')
        json = self._get(endpoint)
        return json['bookings']


    def check_details(self):
        endpoint = self._construct_endpoint('checkDetails')
        json = self._get(endpoint)
        return json['checkDetails']


    def checks(self):
        endpoint = self._construct_endpoint('checks')
        json = self._get(endpoint)
        return json['checks']


    def check_totals(self):
        endpoint = self._construct_endpoint('checkTotals')
        return self._get(endpoint)


    def customers(self):
        endpoint = self._construct_endpoint('customers')
        return self._get(endpoint)


    def discount_types(self):
        endpoint = self._construct_endpoint('discountType')
        return self._get(endpoint)


    def event_heat_details(self):
        endpoint = self._construct_endpoint('eventHeatDetails')
        return self._get(endpoint)


    def event_heat_types(self):
        endpoint = self._construct_endpoint('eventHeatTypes')
        return self._get(endpoint)


    def event_reservation_links(self):
        endpoint = self._construct_endpoint('eventReservationLinks')
        return self._get(endpoint)


    def event_reservations(self):
        endpoint = self._construct_endpoint('eventReservations')
        return self._get(endpoint)


    def event_reservation_types(self):
        endpoint = self._construct_endpoint('eventReservationTypes')
        return self._get(endpoint)


    def event_rounds(self):
        endpoint = self._construct_endpoint('eventRounds')
        return self._get(endpoint)


    def events(self):
        endpoint = self._construct_endpoint('events')
        return self._get(endpoint)


    def event_statuses(self):
        endpoint = self._construct_endpoint('eventStatuses')
        return self._get(endpoint)


    def event_tasks(self):
        endpoint = self._construct_endpoint('eventTasks')
        return self._get(endpoint)


    def event_task_types(self):
        endpoint = self._construct_endpoint('eventTaskTypes')
        return self._get(endpoint)


    def event_types(self):
        endpoint = self._construct_endpoint('eventTypes')
        return self._get(endpoint)


    def gift_card_history(self):
        endpoint = self._construct_endpoint('giftCardHistory')
        return self._get(endpoint)


    def heat_details(self):
        endpoint = self._construct_endpoint('heatDetails')
        return self._get(endpoint)


    def heat_main(self):
        endpoint = self._construct_endpoint('heatMain')
        return self._get(endpoint)


    def heat_types(self):
        endpoint = self._construct_endpoint('heatTypes')
        return self._get(endpoint)


    def memberships(self):
        endpoint = self._construct_endpoint('memberships')
        return self._get(endpoint)


    def membership_types(self):
        endpoint = self._construct_endpoint('membershipTypes')
        return self._get(endpoint)



    def payments(self):
        endpoint = self._construct_endpoint('payments')
        return self._get(endpoint)


    def product_classes(self):
        endpoint = self._construct_endpoint('productClasses')
        return self._get(endpoint)


    def products(self):
        endpoint = self._construct_endpoint('products')
        json = self._get(endpoint)
        return json["products"]


    def racers(self):
        endpoint = self._construct_endpoint('racers')
        return self._get(endpoint)


    def reservations(self):
        endpoint = self._construct_endpoint('reservations')
        json = self._get(endpoint)
        return json["reservations"]


    def sources(self):
        endpoint = self._construct_endpoint('sources')
        return self._get(endpoint)


    def taxes(self):
        endpoint = self._construct_endpoint('taxes')
        json = self._get(endpoint)
        return json["taxes"]


    def users(self):
        endpoint = self._construct_endpoint('users')
        return self._get(endpoint)





