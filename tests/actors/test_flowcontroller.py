import unittest

from compy.actors.flowcontroller import FlowController
from compy.event import Event

class MockActor:
	event_sent = False
	error_sent = False
	event = None
	error_event = None

	def send_event(self, event):
		self.event_sent = True
		self.event = event

	def send_error(self, event):
		self.error_sent = True
		self.error_event = event

class MockFlowController(MockActor, FlowController):
		pass

class TestFlowController(unittest.TestCase):

	def test_init(self):
		actor = MockFlowController('actor')
		self.assertEqual(actor.trigger_errors, False)
		self.assertEqual(actor.generate_fresh_ids, False)

		actor = MockFlowController('actor', trigger_errors=False, generate_fresh_ids=False)
		self.assertEqual(actor.trigger_errors, False)
		self.assertEqual(actor.generate_fresh_ids, False)

		actor = MockFlowController('actor', trigger_errors=True, generate_fresh_ids=True)
		self.assertEqual(actor.trigger_errors, True)
		self.assertEqual(actor.generate_fresh_ids, True)

	def test_consume(self):
		actor = MockFlowController('actor', trigger_errors=False, generate_fresh_ids=False)
		event = Event()
		start_event_id = event._event_id
		start_meta_id = event.meta_id
		self.assertEqual(actor.event_sent, False)
		self.assertEqual(actor.error_sent, False)
		actor.consume(event=event)
		self.assertEqual(actor.event_sent, True)
		self.assertEqual(actor.error_sent, False)
		self.assertEqual(actor.event._event_id, start_event_id)
		self.assertEqual(actor.event.meta_id, start_meta_id)


		actor = MockFlowController('actor', trigger_errors=False, generate_fresh_ids=False)
		event = Event()
		event.error = "Some Error Object"
		start_event_id = event._event_id
		start_meta_id = event.meta_id
		self.assertEqual(actor.event_sent, False)
		self.assertEqual(actor.error_sent, False)
		actor.consume(event=event)
		self.assertEqual(actor.event_sent, True)
		self.assertEqual(actor.error_sent, False)
		self.assertEqual(actor.event._event_id, start_event_id)
		self.assertEqual(actor.event.meta_id, start_meta_id)

		actor = MockFlowController('actor', trigger_errors=True, generate_fresh_ids=False)
		event = Event()
		event.error = "Some Error Object"
		start_event_id = event._event_id
		start_meta_id = event.meta_id
		self.assertEqual(actor.event_sent, False)
		self.assertEqual(actor.error_sent, False)
		actor.consume(event=event)
		self.assertEqual(actor.event_sent, False)
		self.assertEqual(actor.error_sent, True)
		self.assertEqual(actor.error_event._event_id, start_event_id)
		self.assertEqual(actor.error_event.meta_id, start_meta_id)

		actor = MockFlowController('actor', trigger_errors=False, generate_fresh_ids=True)
		event = Event()
		start_event_id = event._event_id
		start_meta_id = event.meta_id
		self.assertEqual(actor.event_sent, False)
		self.assertEqual(actor.error_sent, False)
		actor.consume(event=event)
		self.assertEqual(actor.event_sent, True)
		self.assertEqual(actor.error_sent, False)
		self.assertNotEqual(actor.event._event_id, start_event_id)
		self.assertNotEqual(actor.event.meta_id, start_meta_id)
		self.assertEqual(actor.event.meta_id, actor.event._event_id)
