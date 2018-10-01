import random
import unittest
from uuid import uuid4 as uuid

from compy.actors.zeromq import ZMQPush, ZMQPull
from compy.event import JSONEvent, XMLEvent
from compy.testutils.test_actor import TestActorWrapper


class TestPushPullIPC(unittest.TestCase):

    def setUp(self):
        socket = "/tmp/{0}.sock".format(uuid().get_hex())
        self.push = TestActorWrapper(ZMQPush("zmqpush", socket_file=socket, transmission_protocol=ZMQPush.IPC))
        self.pull = TestActorWrapper(ZMQPull("zmqpull", socket_file=socket, transmission_protocol=ZMQPull.IPC))

    def test_push_json_event(self):
        data = {"foo": "barjson"}
        _input = JSONEvent(data=data)
        self.push.input = _input
        _output = self.pull.output
        self.assertEqual(_output.data_string(), _input.data_string())
        self.assertEqual(_output.event_id, _input.event_id)
        self.assertEqual(_output.meta_id, _input.meta_id)

    def test_push_xml_event(self):
        data = "<foo>barxml</foo>"
        _input = XMLEvent(data=data)
        self.push.input = _input
        _output = self.pull.output
        self.assertEqual(_output.data_string(), _input.data_string())
        self.assertEqual(_output.event_id, _input.event_id)
        self.assertEqual(_output.meta_id, _input.meta_id)


class TestZMQPushPullTCP(TestPushPullIPC):

    def setUp(self):
        port = random.randint(8000, 9000)       # This DOES have a 1/1000 chance of self collision
        self.push = TestActorWrapper(ZMQPush("zmqpush", port=port))
        self.pull = TestActorWrapper(ZMQPull("zmqpull", port=port))


class TestZMQPushPullINPROC(TestPushPullIPC):

    def setUp(self):
        socket = "/tmp/{0}.sock".format(uuid().get_hex())
        self.push = TestActorWrapper(ZMQPush("zmqpush", socket_file=socket, transmission_protocol=ZMQPush.INPROC))
        self.pull = TestActorWrapper(ZMQPull("zmqpull", socket_file=socket, transmission_protocol=ZMQPull.INPROC))