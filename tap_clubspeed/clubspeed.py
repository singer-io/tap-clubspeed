
import requests
import logging

logger = logging.getLogger()


class Clubspeed(object):


    def __init__(self, subdomain=None, private_key=None, session=None):
        """ Simple Python wrapper for the Clubspeed API. Only supports GET. """
        self.protocol = 'https'
        self.domain = 'clubspeedtiming.com'
        self.subdomain = subdomain
        self.api_prefix = 'api/index.php/'
        self.private_key = private_key
        self._url_template = "{protocol}://{subdomain}.{domain}/{api_prefix}{path}.json?key={private_key}"
        self._limit = 100



    def _get(self, url, **kwargs):
        logger.info("Hitting endpoint {url}".format(url=url))
        response = requests.get(url)
        logger.info("  Response is {status}".format(status=response.status_code))
        if response.status_code == 500:
            return None
        response.raise_for_status()
        return response.json()


    def _construct_endpoint(self, path):
        return self._url_template.format(protocol=self.protocol, 
                                         subdomain=self.subdomain, 
                                         domain=self.domain, 
                                         api_prefix=self.api_prefix,
                                         path=path,
                                         private_key=self.private_key)


    def _add_pagination(self, endpoint):
        if "&page=" not in endpoint:
            endpoint += "&page=0&limit={limit}".format(limit=self._limit)
        else:
            array = endpoint.split('&')
            index = 0
            while index < len(array):
                if "page=" in array[index]:
                    page = int(array[index].split('=', 1)[1])
                    page += 1
                    array[index] = "page=" + str(page)
                index += 1
            endpoint = '&'.join(array)
        return endpoint


    def _add_filter(self, endpoint, api_version, column_name, bookmark):
        if api_version == 'V2':
            endpoint += '&where={{"{column_name}":{{"$gt":"{bookmark}"}}}}'.format(column_name=column_name, bookmark=bookmark)
        else:
            endpoint += '&filter={column_name}>{bookmark}'.format(column_name=column_name, bookmark=bookmark)
        return endpoint


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


    def check_details(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('checkDetails')
        check_details = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V1', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            check_details.extend(res['checkDetails'])
            if len(res) < self._limit:
                break
        return check_details


    def checks(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('checks')
        checks = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V1', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            checks.extend(res['checks'])
            if len(res) < self._limit:
                break
        return checks


    def check_totals(self):
        endpoint = self._construct_endpoint('checkTotals')
        return self._get(endpoint)


    def customers(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('customers')
        customers = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        # endpoint = 'https://rpmstamford.clubspeedtiming.com/api/index.php/customers.json?key=Pc8BULWF4Mp8MSZv&page=1050&limit=100'

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            if res is not None:
                customers.extend(res)
                if len(res) < self._limit:
                    break
        return customers


    def discount_types(self):
        endpoint = self._construct_endpoint('discountType')
        return self._get(endpoint)


    def event_heat_details(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('eventHeatDetails')

        heat_details = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            heat_details.extend(res)
            if len(res) < self._limit:
                break
        return heat_details


    def event_heat_types(self):
        endpoint = self._construct_endpoint('eventHeatTypes')
        return self._get(endpoint)


    def event_reservation_links(self):
        endpoint = self._construct_endpoint('eventReservationLinks')
        return self._get(endpoint)


    def event_reservations(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('eventReservations')
        reservations = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            reservations.extend(res)
            if len(res) < self._limit:
                break
        return reservations


    def event_reservation_types(self):
        endpoint = self._construct_endpoint('eventReservationTypes')
        return self._get(endpoint)


    def event_rounds(self):
        endpoint = self._construct_endpoint('eventRounds')
        return self._get(endpoint)


    def events(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('events')

        events = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            events.extend(res)
            if len(res) < self._limit:
                break
        return events


    def event_statuses(self):
        endpoint = self._construct_endpoint('eventStatuses')
        return self._get(endpoint)


    def event_tasks(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('eventTasks')
        tasks = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            tasks.extend(res)
            if len(res) < self._limit:
                break
        return tasks


    def event_task_types(self):
        endpoint = self._construct_endpoint('eventTaskTypes')
        return self._get(endpoint)


    def event_types(self):
        endpoint = self._construct_endpoint('eventTypes')
        return self._get(endpoint)


    def gift_card_history(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('giftCardHistory')
        histories = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            histories.extend(res)
            if len(res) < self._limit:
                break
        return histories


    def heat_details(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('heatDetails')
        heat_details = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            heat_details.extend(res)
            if len(res) < self._limit:
                break
        return heat_details


    def heat_main(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('heatMain')
        mains = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            mains.extend(res)
            if len(res) < self._limit:
                break
        return mains


    def heat_types(self):
        endpoint = self._construct_endpoint('heatTypes')
        return self._get(endpoint)


    def memberships(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('memberships')
        memberships = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            memberships.extend(res)
            if len(res) < self._limit:
                break
        return memberships


    def membership_types(self):
        endpoint = self._construct_endpoint('membershipTypes')
        return self._get(endpoint)



    def payments(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('payments')
        payments = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            payments.extend(res)
            if len(res) < self._limit:
                break
        return payments


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


    def reservations(self, column_name=None, bookmark=None):
        endpoint = self._construct_endpoint('reservations')
        reservations = []

        if bookmark is not None and column_name is not None:
            endpoint = self._add_filter(endpoint, 'V2', column_name, bookmark)

        while True:
            endpoint = self._add_pagination(endpoint)
            res = self._get(endpoint)
            reservations.extend(res['reservations'])
            if len(res) < self._limit:
                break
        return reservations


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





