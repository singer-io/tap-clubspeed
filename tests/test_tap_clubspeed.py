import itertools
import unittest
import tap_clubspeed.streams as streams

from tap_clubspeed.streams import Stream
from tap_clubspeed.clubspeed import Clubspeed
from singer.catalog import Catalog
from singer.schema import Schema
from singer.utils import strftime


class TestClubspeed(unittest.TestCase):
    def test_construct_endpoint(self):
        client = Clubspeed("subdomain", "private_key")
        endpoint = "https://subdomain.clubspeedtiming.com/api/index.php/path.json?key=private_key"
        self.assertEqual(endpoint, client._construct_endpoint('path'))

    def test_pagination(self):
        client = Clubspeed("subdomain", "private_key")
        endpoint = client._construct_endpoint('path')
        paginated_endpoint = endpoint + '&page=0&limit=100'
        self.assertEqual(paginated_endpoint, client._add_pagination(endpoint))
        next_paginated_endpoint = endpoint + '&page=1&limit=100'
        self.assertEqual(next_paginated_endpoint, client._add_pagination(paginated_endpoint))


    def test_add_filter(self):
        client = Clubspeed("subdomain", "private_key")
        endpoint = client._construct_endpoint('path')
        filtered_endpoint_v2 = endpoint + '&where={"column_name":{"$gt":"bookmark"}}'
        self.assertEqual(filtered_endpoint_v2, client._add_filter(endpoint, 'V2', 'column_name', 'bookmark'))
        filtered_endpoint_v1 = endpoint + '&filter=column_name>bookmark'
        self.assertEqual(filtered_endpoint_v1, client._add_filter(endpoint, 'V1', 'column_name', 'bookmark'))


class TestStreams(unittest.TestCase):
    def test_needs_parse_to_date(self):
        self.assertTrue(streams.needs_parse_to_date('2011-11-03 18:21:26'))
        self.assertFalse(streams.needs_parse_to_date(1))
        self.assertFalse(streams.needs_parse_to_date('this_should_fail'))

    def test_is_bookmark_old(self):
        bookmarks = {
            "bookmarks": {
                "old_timestamp": {
                    "createdAt": "2011-11-03 18:21:26"
                },
                "current_timestamp": {
                    "createdAt": "2018-11-03 18:21:26"
                },
                "old_id": {
                    "id": 0
                },
                "current_id": {
                    "id": 5
                }
            }
        }

        now = "2018-11-02 18:21:26"
        currentId = 4

        OldTimestamp = Stream()
        OldTimestamp.name = "old_timestamp"
        OldTimestamp.replication_key = "createdAt"
        self.assertTrue(Stream.is_bookmark_old(OldTimestamp, bookmarks, now))

        CurrentTimestamp = Stream()
        CurrentTimestamp.name = "current_timestamp"
        CurrentTimestamp.replication_key = "createdAt"
        self.assertFalse(Stream.is_bookmark_old(CurrentTimestamp, bookmarks, now))

        OldId = Stream()
        OldId.name = "old_id"
        OldId.replication_key = "id"
        self.assertTrue(Stream.is_bookmark_old(OldId, bookmarks, currentId))

        CurrentId = Stream()
        CurrentId.name = "current_id"
        CurrentId.replication_key = "id"
        self.assertFalse(Stream.is_bookmark_old(CurrentId, bookmarks, currentId))



if __name__ == '__main__':
    unittest.main()

