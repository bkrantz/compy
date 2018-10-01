import gevent
import zmq.green as zmq
import time

from binascii import hexlify

from . import mdpdefinition as MDPDefinition

class RegistratorSocket(zmq.Socket):
    """
    This class exists simply to add the 'broker' property to a socket. This makes it simple to just grab the originating broker
    from any given socket. The best use case for this is during the POLLIN stage. This was deemed the lowest overhead approach - no searching necessary.

    Some alternatives to this approach would include adding a 'search' method for the static socket identity in the 'brokers' list
    or to create another dict to link the identity (which is available as a socket property) to the broker in question.
    """

    broker = None


class RegistratorContext(zmq.Context):
    """
    This class is simply to link the socket context to our own socket class, since we do not instantiate the socket directly, but through the zmq context
    """

    _socket_class = RegistratorSocket
    pass

class RegistrationService(object):
    """
    The base class for shared properties across the RegistrationService module
    """
    context = None
    manager_subscriber_scope = None
    registration_publisher_port = None
    registration_service_port = None

    def __init__(self, context=None, manager_subscriber_scope=None, registration_service_port=None, registration_publisher_port=None, *args, **kwargs):
        self.context = context or kwargs.get('context', None) or RegistratorContext()
        self.manager_subscriber_scope = manager_subscriber_scope or kwargs.get('manager_subscriber_scope', None) or b"BrokerUpdates"
        self.registration_service_port = registration_service_port or kwargs.get('registration_service_port', None) or "6000"
        self.registration_publisher_port = registration_publisher_port or kwargs.get('registration_publisher_port', None) or "6001"

class Broker(object):

    """
    A broker class used by the BrokerRegistrationService to keep track of relevant broker liveness, hexlified (unique) socket identity, and listening address
    """

    identity = None             # The identity of the Broker, taken as the hexlified address of the dealer socket that sends heartbeats to the BrokerRegistartionService
    port = None                 # Port Address of outbound worker socket
    expiration_time = None      # expires at this point, unless heartbeat
    lifetime = None             # in millis, the amount of time allowed between heartbeats before a broker will be registered as invalid
    liveness = None             # The current liveness of the broker. See MAX_LIVENESS below for more. If this number reaches 0 the broker will be purged from the registration 

    MAX_LIVENESS = None         # The number of times in a row a heartbeat can be missed before a broker will be purged

    def __init__(self, identity=None, port=None, lifetime=None, max_liveness=None, *args, **kwargs):
        self.port = port or kwargs.get('port', None)
        self.identity = identity or kwargs.get('identity', None)
        self.lifetime = lifetime or kwargs.get('lifetime', None) or 5000
        self.MAX_LIVENESS = max_liveness or kwargs.get('max_liveness', None) or 3
        self.refresh_expiration_time()
        
    def refresh_expiration_time(self):
        self.expiration_time = time.time() + 1e-3*self.lifetime
        self.liveness = self.MAX_LIVENESS

class BrokerConnector(Broker):
    """
    The BrokerConnector is an extension of the Broker class that is created on the client side to track the socket address and verification status of any given broker, as sent from the RegistryService
    Each client needs to keep track of liveness and expiration for their personal connection to the broker, so extending the base Broker class was necessary
    """
    outbound_socket = None          # The DEALER socket used to send messages to the broker without blocking inbound queues
    inbound_socket = None           # The DEALER socket used to receive message from the broker without blocking outbound queues
    verified = None                 # Whether or not this broker has been verified as connected by the client itself and not just the RegistrationService
    context = None
    socket_identity = None      
    verification_attempts = None    # The number of times that the implementor has initiated a verification request for this broker
    reconnect_attempts = None       # The number of times that the implementor has initiated a reconnect for this broker

    """
    socket_identity: The identity that will be used in conjunction with a naming convention to allow for non-blocking concurrent sending/receiving
    e.g.    socket_identity = b"foo"
            outbound_socket.identity = b"foo"
            inbound_socket.identity = b"foo_receiver"
    """

    def __init__(self, context=None, socket_identity=None, *args, **kwargs):
        Broker.__init__(self, *args, **kwargs)
        self.context = context or kwargs.get('context', None) or RegistratorContext()
        self.socket_identity = socket_identity or kwargs.get('socket_identity', None)
        self.verification_attempts = 0
        self.reconnect_attempts = 0
        self.reconnect()
        
    def disconnect(self):
        self.outbound_socket.close()
        self.inbound_socket.close()

    def increment_verification_attempts(self):
        self.verification_attempts += 1

    def reconnect(self):
        self.verification_attempts = 0

        if self.outbound_socket:
            self.outbound_socket.close()

        if self.inbound_socket:
            self.inbound_socket.close()

        self.outbound_socket = self.context.socket(zmq.DEALER)
        self.outbound_socket.identity = self.socket_identity
        self.outbound_socket.linger = 0
        self.outbound_socket.broker = self
        self.outbound_socket.connect("tcp://localhost:{0}".format(self.port))

        self.inbound_socket = self.context.socket(zmq.DEALER)
        self.inbound_socket.identity = b"{0}_receiver".format(self.socket_identity)
        self.inbound_socket.linger = 0
        self.inbound_socket.broker = self
        self.inbound_socket.connect("tcp://localhost:{0}".format(self.port))

        self.reconnect_attempts += 1

class BrokerManager(RegistrationService):

    """
    This class is meant to be used to coordinate utilization of brokers that are registered with the registration service
    TODO: Allow for a lack of communication to/from the RegistrationService to initiate a handshake attempt, along with
        a re-initialization of the RegistryService daemon
    """

    controller_identity = None
    subscriber_socket = None
    verified_brokers = None
    unverified_brokers = None
    inbound_sockets = None
    inbound_poller = None

    

    """
    In order to be certain we're not attempting to access a deleted item, and that we can maintain an array that we can pop() and iterate over easily
    without worrying about modification in a the 'listen' method running in a greenlet and modifying the underlying 'brokers' dict
    """
    verified_brokers_index = None
    verified_brokers_index_pointer = None

    heartbeat_manager = None
    verification_manager = None
    

    def __init__(self, controller_identity=None, logger=None, *args, **kwargs):
        RegistrationService.__init__(self, *args, **kwargs)

        self.controller_identity = controller_identity or kwargs.get('controller_identity', None)

        self.subscriber_socket = self.context.socket(zmq.SUB)
        self.subscriber_socket.setsockopt(zmq.SUBSCRIBE, self.manager_subscriber_scope)
        self.subscriber_socket.connect("tcp://localhost:{0}".format(self.registration_publisher_port))

        self.verified_brokers = {}
        self.unverified_brokers = {}
        self.inbound_sockets = []

        self.logger = logger

        self.inbound_poller = zmq.Poller()

        self.heartbeat_manager = HeartbeatManager(*args, **kwargs)
        self.verification_manager = HeartbeatManager(*args, **kwargs)

        self.poller = zmq.Poller()
        self.poller.register(self.subscriber_socket, zmq.POLLIN)

        gevent.spawn(self.listen_for_updates)

    def listen_for_updates(self):
        while True:
            items = self.poller.poll()
            if items:
                message = self.subscriber_socket.recv_multipart()
                self.process_update(message)

    def process_update(self, message):
        assert len(message) >= 5

        subscription = message.pop(0)
        empty = message.pop(0)
        command = message.pop(0)

        if command == MDPDefinition.B_HEARTBEAT:
            while (len(message) >= 2): # Iterate over the identities and addresses provided in the update
                broker_identity = message.pop(0)
                broker_address = message.pop(0)
                if self.verified_brokers.get(broker_identity) is None and self.unverified_brokers.get(broker_identity) is None:
                    self.connect_broker(BrokerConnector(identity=broker_identity, context=self.context, port=broker_address, socket_identity=self.controller_identity))

        elif command == MDPDefinition.B_DISCONNECT:
            broker_identity = message.pop(0)
            broker_address = message.pop(0)
            if self.logger is not None:
                self.logger.info("Received a disconnect command for broker {0} from registration service".format(broker_identity))

            self.disconnect_broker(broker_identity)

    def update_verified_broker_index(self):
        """
        This keeps an index of all verified brokers for use in the queue
        """
        self.verified_brokers_index = self.verified_brokers.keys()

    def get_next_broker_in_queue(self, broker_origin_identity=None, broker_origin_port=None):
        """
        This will round-robin through the brokers that have successfully registered with the BrokerManager.
        Alternatively, implementing classes may use the 'broker_origin_identity' and the 'broker_origin_port' to try to use a the same broker that 
        brokered the request to broker the reply.

        If the broker with the same identity is found, it will use that broker - otherwise, it will attempt to find a broker on that same port, as
        the identity will change with a broker restart. If these do not exist, it will simply return the next broker in the round-robin queue
        """
        self.update_verified_broker_index()
        if len(self.verified_brokers_index) > 0:

            # Attempt to use the origin values for intelligent selection
            if broker_origin_identity is not None or broker_origin_port is not None:
                if self.verified_brokers.get(broker_origin_identity) is not None:
                    return self.verified_brokers.get(broker_origin_identity)
                elif broker_origin_port is not None:
                    for broker in self.verified_brokers.itervalues():
                        if broker.port == broker_origin_port:
                            return broker

            if self.verified_brokers_index_pointer is None:
                self.verified_brokers_index_pointer = 0 # Set/Reset position to beginning of index array
            elif (self.verified_brokers_index_pointer + 1) >= len(self.verified_brokers_index):
                self.verified_brokers_index_pointer = 0
            else:
                self.verified_brokers_index_pointer += 1

            broker = self.verified_brokers.get(self.verified_brokers_index[self.verified_brokers_index_pointer])

            if broker is not None:
                return broker
            else:
                return self.get_next_broker_in_queue()
        else:
            return None

    def send_verification_requests(self, origin, message=[]):
        """
        This sends a verification message to all unverified brokers. Implementations may differ as to what that message is, and the implementing
        class is responsible for interpreting the verification response from the broker and calling "verify_broker" accordingly
        """

        if len(self.unverified_brokers) > 0:
            if not isinstance(message, list):
                message = [message]

            message = ['', origin, MDPDefinition.B_VERIFICATION_REQUEST] + message

            broker_sockets = []

            unverified_brokers = self.unverified_brokers.values()
            for broker in unverified_brokers:
                if broker.verification_attempts >= 3:
                    if broker.reconnect_attempts >= 3:
                        if self.logger is not None:
                            self.logger.info("Unable to verify broker {0}. Disconnecting permanently...".format(broker.identity))
                        self.disconnect_broker(broker.identity)
                    else:
                        if self.logger is not None:
                            self.logger.info("Broker {0} is unresponsive to verification attempts, attempting to reconnect... (Number of reconnect attempts: {1})".format(broker.identity, broker.reconnect_attempts))
                        self.reconnect_broker(broker)
                else:
                    broker_sockets.append(broker.outbound_socket)
                    broker.increment_verification_attempts()
                    if self.logger is not None:
                        self.logger.info("Sending verification request to broker {0} (Number of verification attempts: {1})".format(broker.identity, broker.verification_attempts))

            if len(broker_sockets) > 0:
                self.verification_manager.send_heartbeats(broker_sockets, message)
            

    def verify_broker(self, broker_identity):
        if self.unverified_brokers.get(broker_identity) is not None:
            broker = self.unverified_brokers.pop(broker_identity)
            broker.verified = True
            self.verified_brokers[broker_identity] = broker
            if self.logger is not None:
                self.logger.info("Verified Broker {0}".format(broker.identity))
           
    def connect_broker(self, broker):
        self.unverified_brokers[broker.identity] = broker
        self.inbound_poller.register(broker.inbound_socket, zmq.POLLIN)
        if self.logger is not None:
            self.logger.info("Connected new broker {0} at port {1}".format(broker.identity, broker.port))

    def reconnect_broker(self, broker):
        if (broker.inbound_socket, zmq.POLLIN) in self.inbound_poller.sockets:
                self.inbound_poller.unregister(broker.inbound_socket)
        broker.reconnect()
        if self.logger is not None:
            self.logger.info("Reconnecting broker {0}".format(broker.identity))
        self.inbound_poller.register(broker.inbound_socket, zmq.POLLIN)
            
    def disconnect_broker(self, broker_identity):
        """
        Disconnect the broker, wherever it may exist, and unregister it from the poller
        """
        broker = None
        if self.verified_brokers.get(broker_identity):
            broker = self.verified_brokers.pop(broker_identity, None)
        elif self.unverified_brokers.get(broker_identity):
            broker = self.unverified_brokers.pop(broker_identity, None)

        if broker is not None:
            if self.logger is not None:
                self.logger.info("Disconnecting broker {0}".format(broker.identity))

            if (broker.inbound_socket, zmq.POLLIN) in self.inbound_poller.sockets:
                self.inbound_poller.unregister(broker.inbound_socket)
            broker.disconnect()
            del broker

    def send_heartbeats(self, message=None):
        """
        This operation will not, nor will it ever be concurrent-operation safe with any send operations to the brokers. If this heartbeating is used, it should be used
        non-concurrent with the same logic that executes other send operations on the BrokerConnector 'outbound_socket' socket.
        """
        sockets = []

        for broker in self.verified_brokers.itervalues():
            sockets.append(broker.outbound_socket)

        self.heartbeat_manager.send_heartbeats(sockets, message)



class BrokerRegistrator(RegistrationService):

    registrator = None
    registration_service_endpoint = None
    broker_port = None
    broker_identity = None          # The identity utilized by the broker - must match the identity of the broker main ROUTER socket, or client heartbeat requests will never track back to
    registration_manager = None     # Used to control frequency of registration

    def __init__(self, broker_port=None, broker_identity=None, registration_service_endpoint=None, *args, **kwargs):
        RegistrationService.__init__(self, *args, **kwargs)

        self.broker_port = broker_port or kwargs.get('broker_port', None) or 5555
        self.broker_identity = broker_identity or kwargs.get('broker_identity', None)
        self.registration_service_endpoint = registration_service_endpoint or kwargs.get('registration_service_endpoint', None) or "tcp://localhost:{0}".format(self.registration_service_port)

        self.registrator = self.context.socket(zmq.DEALER)
        self.registrator.identity = self.broker_identity
        self.registrator.connect(self.registration_service_endpoint)
        self.registration_manager = HeartbeatManager(heartbeat_interval=2500)

    def register(self):
        self.registration_manager.send_heartbeats(self.registrator, ['', b"{0}".format(self.broker_port)])

class HeartbeatManager(object):
    """
    This is a util class to handle heartbeat timing and execution. This does NOT handle the reception of heartbeats (yet)
    This class will simply send heartbeats every heartbeat_interval upon the invocation of send_heartbeat from the implementing
    class/object

    As it doesn't handle the reception of heartbeats, it also does not manage liveness
    """

    heartbeat_interval = 5000 # Interval to send heartbeat updates to the provided sockets
    heartbeat_at = None

    def __init__(self, heartbeat_interval=None, *args, **kwargs):
        self.heartbeat_interval = heartbeat_interval or kwargs.get('heartbeat_interval', None) or 2500
        self.refresh_heartbeat_timer()

    def refresh_heartbeat_timer(self):
        self.heartbeat_at = time.time() + 1e-3*self.heartbeat_interval

    def should_heartbeat(self):
        return (time.time() > self.heartbeat_at)

    def send_heartbeats(self, sockets, message):
        if self.should_heartbeat():
            if sockets is None:
                sockets = []
            elif not isinstance(sockets, list):
                sockets = [sockets]

            for socket in sockets:
                socket.send_multipart(message)

            self.refresh_heartbeat_timer()
