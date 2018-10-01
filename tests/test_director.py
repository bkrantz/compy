import unittest
import abc
import gevent
import time
import signal
import os

from gevent.event import Event as GEvent
#from gevent.signal import signal as gsignal, getsignal
from gevent import signal as gsignal, event, get_hub

from compy.actor import Actor
from compy.director import Director
from compy.event import Event
from compy.queue import QueuePool, Queue
from compy.logger import Logger
from compy.restartlet import RestartPool
from compy.errors import (QueueConnected, InvalidActorOutput, QueueEmpty, InvalidEventConversion, 
    InvalidActorInput, QueueFull)
from compy.actors.stdout import STDOUT
from compy.actors.eventlogger import EventLogger

class MockedAsyncClass:
    cleared = False
    waited = False
    running = False

    def clear(self):
        self.cleared = True

    def wait(self):
        self.waited = True

    def is_set(self):
        return self.running

    def set(self):
        self.running = True

class MockDirector(Director):
	pass

class MockDirectorAlt(Director):

	_async_class = MockedAsyncClass

	def __init__(self, *args, **kwargs):
		super(MockDirectorAlt, self).__init__(*args, **kwargs)
		self.stopped = False

	def stop(self):
		self.stopped = True

class TestDirector(unittest.TestCase):

	def test_init_signals(self):
		director = MockDirectorAlt()
		self.assertEqual(director.stopped, False)
		os.kill(os.getpid(), signal.SIGTERM)
		gevent.sleep(.1)
		self.assertEqual(director.stopped, True)
		director.stopped = False
		self.assertEqual(director.stopped, False)
		os.kill(os.getpid(), signal.SIGINT)
		gevent.sleep(.1)
		self.assertEqual(director.stopped, True)

	def test_init_defaults(self):
		director = Director()
		self.assertEqual(director.name, "default")
		self.assertEqual(director.actors, {})
		self.assertEqual(director.size, 500)
		self.assertIsInstance(director.log_actor, STDOUT)
		self.assertIsInstance(director.error_actor, EventLogger)
		self.assertEqual(director._Director__running, False)
		self.assertIsInstance(director._Director__block, event.Event)
		self.assertEqual(director.blockdiag_dir, './build/blockdiag')
		self.assertEqual(director.generate_blockdiag, True)
		self.assertEqual(director.blockdiag_out, """diagram admin {\n""")
		self.assertIsInstance(director._Director__block, event.Event) # test block clear

	def test_init(self):
		director = MockDirectorAlt()
		self.assertEqual(director._Director__block.cleared, True)

	def test_get_actor(self):
		pass

	def test_connect_log_queue(self):
		pass

	def test_connect_error_queue(self):
		pass

	def test_connect_queue(self):
		pass

	def test_parse_connect_arg(self):
		pass

	def test_finalize_blockdiag(self):
		pass

	def test_register_actor(self):
		pass

	def test_register_log_actor(self):
		pass

	def test_register_error_actor(self):
		pass

	def test_create_actor(self):
		pass

	def test_setup_default_connections(self):
		pass

	def test_is_running(self):
		pass

	def test_start(self):
		pass

	def test_block(self):
		pass

	def test_stop(self):
		pass