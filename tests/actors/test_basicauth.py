import unittest

#from compy.actors.auth import BasicAuth
from compy.actor import Actor
from compy.event import Event
from compy.errors import UnauthorizedEvent

'''
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

def mocked_authenticate(username=False, password=False):
	if username and password:
		return True
	return False

def mocked_raise_authenticate(username, password):
	raise UnauthorizedEvent()

class MockBasicAuth(MockActor, BasicAuth):
	pass

class TestBasicAuth(unittest.TestCase):

	def process_fail_extract_credentials(self, actor, event):
		try:
			actor._extract_credentials(event=event)
			self.assertFalse(True)
		except UnauthorizedEvent:
			pass
		except Exception:
			self.assertFalse(True)

	def test_inheritance(self):
		self.assertEqual(len(BasicAuth.__bases__), 1)
		self.assertEqual(BasicAuth.__bases__[0], Actor)

	def test_extract_credentials(self):
		actor = BasicAuth('actor')

		#test success
		event = Event(environment={'HTTP_AUTHORIZATION': 'Basic dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ='})
		user, password = actor._extract_credentials(event=event)
		self.assertEqual(user, 'test_user')
		self.assertEqual(password, 'test_password')
		event = Event(environment={'HTTP_AUTHORIZATION': 'Basic c2Vjb25kYXJ5X3Rlc3RfdXNlcjpzZWNvbmRhcnlfdGVzdF9wYXNzd29yZA=='})
		user, password = actor._extract_credentials(event=event)
		self.assertEqual(user, 'secondary_test_user')
		self.assertEqual(password, 'secondary_test_password')

		#test malformed tokens
		self.process_fail_extract_credentials(actor=actor, event=Event(environment={'HTTP_AUTHORIZATION': 'BasicN dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ='}))
		self.process_fail_extract_credentials(actor=actor, event=Event(environment={'HTTP_AUTHORIZATION': 'Basic asdf1234'}))
		self.process_fail_extract_credentials(actor=actor, event=Event(environment={'HTTP_AUTHORIZATION': 'Basic dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ= dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ='}))
		self.process_fail_extract_credentials(actor=actor, event=Event(environment={'HTTP_AUTHORIZATION': 'Basic'}))
		self.process_fail_extract_credentials(actor=actor, event=Event(environment={}))
		self.process_fail_extract_credentials(actor=actor, event=Event())

	def test_process_authentication(self):
		actor = MockBasicAuth('actor')
		actor._authenticate = mocked_authenticate
		self.assertFalse(actor.event_sent)
		actor._process_authentication(event=None, user=True, password=True)
		self.assertTrue(actor.event_sent)

		actor = MockBasicAuth('actor')
		actor._authenticate = mocked_authenticate
		self.assertFalse(actor.event_sent)
		try:
			actor._process_authentication(event=None, user=False, password=False)
			self.assertTrue(False)
		except UnauthorizedEvent:
			pass
		self.assertFalse(actor.event_sent)

	def test_process_authentication_error(self):
		actor = MockBasicAuth('actor')
		event = Event()
		self.assertEqual(event.get('status', None), None)
		self.assertEqual(event.get('headers', None), None)
		self.assertEqual(actor.error_event, None)
		self.assertEqual(actor.event, None)
		actor._process_authentication_error(event=event, error=None)
		self.assertNotEqual(actor.error_event, None)
		self.assertEqual(actor.event, None)
		self.assertEqual(actor.error_event.get('status', None), '401 Unauthorized')
		self.assertEqual(actor.error_event.get('headers', None), {'WWW-Authenticate': 'Basic realm="Compysition Authentication"'})

	def test_consume(self):
		#test success
		actor = MockBasicAuth('actor')
		actor._authenticate = mocked_authenticate
		event = Event(environment={'HTTP_AUTHORIZATION': 'Basic dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ='})
		self.assertFalse(actor.event_sent)
		self.assertFalse(actor.error_sent)
		actor.consume(event)
		self.assertTrue(actor.event_sent)
		self.assertFalse(actor.error_sent)

		#test invalid creds
		actor = MockBasicAuth('actor')
		actor._authenticate = mocked_authenticate
		event = Event(environment={'HTTP_AUTHORIZATION': 'Basic ='})
		self.assertFalse(actor.event_sent)
		self.assertFalse(actor.error_sent)
		actor.consume(event)
		self.assertTrue(actor.error_sent)
		self.assertFalse(actor.event_sent)

		#test unauth
		actor = MockBasicAuth('actor')
		actor._authenticate = mocked_raise_authenticate
		event = Event(environment={'HTTP_AUTHORIZATION': 'Basic dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ='})
		self.assertFalse(actor.event_sent)
		self.assertFalse(actor.error_sent)
		actor.consume(event)
		self.assertTrue(actor.error_sent)
		self.assertFalse(actor.event_sent)

	def test_authenticate(self):
		actor = BasicAuth('actor')
		try:
			actor._authenticate(username=None, password=None)
			self.assertTrue(False)
		except NotImplementedError:
			pass
		except:
			self.assertTrue(False)
'''