import json
import unittest

from compy.actors.httpserver import HTTPServer
from compy.event import JSONHttpEvent, HttpEvent, XMLHttpEvent
from compy.testutils.test_actor import TestActorWrapper

class TestHTTPServer(unittest.TestCase):
    """
    Actor for testing the response formatting of the HTTP Server

    TODO: It's currently somewhat annoying that you have to manually insert the event_id into the responders dict
    every test. Wrote a decorator that wrapped the test_* methods and did that automatically, but it felt kind of dirty.
    Could re-vist that or a similar idea if repeating gets to annoying
    """
    def setUp(self):
        self.actor = TestActorWrapper(HTTPServer("actor", address="0.0.0.0", port=8123))

    def tearDown(self):
        self.actor.stop()

    def test_content_type_text_plain(self):
        _input_event = HttpEvent()
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(output.headers['Content-Type'], 'text/plain')


    def test_content_type_json_event(self):
        _input_event = JSONHttpEvent()
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(output.headers['Content-Type'], 'application/json')

    def test_content_type_xml_event(self):
        _input_event = XMLHttpEvent()
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(output.headers['Content-Type'], 'application/xml')

    def test_json_event_formatted_in_data_tag(self):
        expected = {
            "data": {
                "honolulu": "is blue blue"
            }
        }
        _input_event = JSONHttpEvent(data={'honolulu': 'is blue blue'})
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(json.loads(output.body), expected)

    def test_json_event_pagination_links(self):
        expected = {"_pagination": {"next": "/credit_unions/CU00000/places?limit=2&offset=4",
                                    "prev": "/credit_unions/CU00000/places?limit=2&offset=2"},
                    "data": [{"honolulu": "is blue blue"}, {"ohio": "why i go"}]}
        environment = {'PATH_INFO': '/credit_unions/CU00000/places'}

        _input_event = JSONHttpEvent(data=[{'honolulu': 'is blue blue'}, {'ohio': 'why i go'}], environment=environment)
        _input_event._pagination = {'limit': 2, 'offset': 2}
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(json.loads(output.body), expected)


    def test_json_event_pagination_next_link_not_present_if_num_results_less_than_limit(self):
        expected = {"_pagination": {"prev": "/credit_unions/CU00000/places?limit=3&offset=2"},
                    "data": [{"honolulu": "is blue blue"}, {"ohio": "why i go"}]}
        environment = {'PATH_INFO': '/credit_unions/CU00000/places'}

        _input_event = JSONHttpEvent(data=[{'honolulu': 'is blue blue'}, {'ohio': 'why i go'}], environment=environment)
        _input_event._pagination = {'limit': 3, 'offset': 2}
        self.actor.actor.responders[_input_event.event_id] = (type(_input_event), self.actor._output_funnel)
        self.actor.input = _input_event
        output = self.actor.output
        self.assertEqual(json.loads(output.body), expected)


