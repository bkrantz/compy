#!/usr/bin/env python

import gevent.socket as socket #TODO dont need this and generic gevent import
import gevent

pickle = None
try:
    import cPickle as pickle #Python 2
except ImportError:
    import _pickle as pickle #Python 3

from gevent.server import StreamServer

from compy.actor import Actor

"""
Implementation of a TCP in and out connection using gevent sockets
"""

#TODO: Add options like "Wait for response" and "Send response" for TCPIn and TCPOut, respectively
#TODO: Add non-event TCPIn (Origination from a non-compysition source)

DEFAULT_PORT = 9000
BUFFER_SIZE  = 1024

class TCPOut(Actor):

    """
    Send events over TCP
    """


    def __init__(self, name, port=None, host=None, listen=True, *args, **kwargs):
        super(TCPOut, self).__init__(name, *args, **kwargs)

        self.blockdiag_config["shape"] = "cloud"
        self.port = port or DEFAULT_PORT
        self.host = host or socket.gethostbyname(socket.gethostname())

    def consume(self, event, *args, **kwargs):
        self._send(event)

    def _send(self, event):
        while True:
            try:
                sock = socket.socket()
                sock.connect((self.host, self.port))
                sock.send((pickle.dumps(event)))
                sock.close()
                break
            except Exception as err:
                self.logger.error("Unable to send event over tcp to {host}:{port}: {error}".format(host=self.host, port=self.port, error=err))
                gevent.sleep(0)

class TCPIn(Actor):

    """
    Receive Events over TCP
    """

    def __init__(self, name, port=None, host=None, *args, **kwargs):
        super(TCPIn, self)._init__(name, *args, **kwargs)
        self.blockdiag_config["shape"] = "cloud"
        self.port = port or DEFAULT_PORT
        self.host = host or "0.0.0.0"
        self.server = StreamServer((self.host, self.port), self.connection_handler)

    def consume(self, event, *args, **kwargs):
     pass

    def pre_hook(self):
        self.logger.info("Connecting to {0} on {1}".format(self.host, self.port))
        self.server.start()

    def post_hook(self):
        self.server.stop()

    def connection_handler(self, socket, address):
        event_string = ""
        for l in socket.makefile('r'):
            event_string += l

        try:
            event = pickle.loads(event_string)
            self.send_event(event)
        except Exception:
            self.logger.error("Received invalid event format: {0}".format(event_string))






