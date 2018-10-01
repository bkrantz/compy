import unittest

from compy.event import HttpEvent, Event, CompysitionException
from compy.errors import ResourceNotFound

class TestEvent(unittest.TestCase):
    def setUp(self):
        self.event = Event(data={'foo': 'bar'}, meta_id='123456abcdef')

    def test_distinct_meta_and_event_ids(self):
        self.assertNotEqual(self.event.event_id, self.event.meta_id)


class TestHttpEvent(unittest.TestCase):
    def test_default_status(self):
        self.event = HttpEvent(data='quick brown fox')
        self.assertEquals(self.event.status, (200, 'OK'))

    def test_setting_status_tuple(self):
        error = (404, 'Not Found')
        self.event = HttpEvent(data='quick brown fox')
        self.event.status = error
        self.assertEquals(self.event.status, error)

    def test_setting_status_string_space_delimited(self):
        error = '404 Not Found'
        self.event = HttpEvent(data='quick brown fox')
        self.event.status = error
        self.assertEquals(self.event.status, (404, 'Not Found'))

    def test_setting_status_string_dash_delimited(self):
        error = '404-Not Found'
        self.event = HttpEvent(data='quick brown fox')
        self.event.status = error
        self.assertEquals(self.event.status, (404, 'Not Found'))

    def test_resource_not_found_status_updated(self):
        self.event = HttpEvent()
        self.event.error = ResourceNotFound()
        self.assertEquals(self.event.status, (404, 'Not Found'))

    def test_internal_server_error(self):
        self.event = HttpEvent()
        self.event.error = CompysitionException()
        self.assertEquals(self.event.status, (500, 'Internal Server Error'))
