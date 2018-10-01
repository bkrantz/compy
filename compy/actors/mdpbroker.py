#!/usr/bin/env python

import gevent
import time
import zmq.green as zmq

from uuid import uuid4 as uuid

from .util import mdpdefinition as MDPDefinition
from .util.mdpregistrar import BrokerRegistrator
from compy.actor import Actor

class Service(object):
    """a single Service"""
    name = None         # Service name
    requests = None     # List of client requests
    workers = None      # List of waiting workers

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.workers = []
        self.unhealthy_workers = []

class Worker(object):
    """a Worker, idle or active"""
    identity = None         # Identity of worker
    address = None          # Address of outbound worker socket
    inbound_address = None  # Address of the socket used to process inbound requests for that worker factory. It is always '{address}-Inbound'
    service = None          # Owning service, if known
    expiry = None           # Worker expires at this point in time, unless heartbeat
    lifetime = None         # How long this worker may go before it is considered expired
    last_heartbeat = None   # The last time that refresh_expiry was called
    liveness = None         # The number of times that a worker has missed it's heartbeat mark

    def __init__(self, identity, address, lifetime, max_liveness=3):
        self.identity = identity
        self.address = address
        self.inbound_address = "{0}_receiver".format(address)
        self.lifetime = lifetime
        self.refresh_expiry()
        self.last_heartbeat = time.time()
        self.MAX_LIVENESS = max_liveness
        self.liveness = self.MAX_LIVENESS

    def refresh_expiry(self):
        self.expiry = time.time() + 1e-3*self.lifetime

    def reduce_liveness(self):
        self.liveness -= 1;
        self.refresh_expiry()

    def register_heartbeat(self):
        if self.liveness < self.MAX_LIVENESS:
            self.liveness += 1
        self.last_heartbeat = time.time()
        self.refresh_expiry()

    @staticmethod
    def generate_inbound_address(address):
        return "{0}_receiver".format(address)


class MDPBroker(Actor):
    """
    Majordomo Protocol broker
    A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8
    """
    HEARTBEAT_LIVENESS = 3 # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500 # msecs
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    # ---------------------------------------------------------------------

    context = None # Our context
    router_socket = None # Socket for clients & workers
    poller = None # our Poller

    heartbeat_at = None # When to send HEARTBEAT
    services = None # Known Services, either requested by a client or registered as ready by a worker
    workers = None # Known Workers. This global list is maintained for ease of determining whether or not a worker has been registered

    registrator = None # Registrator for client broker load balancing

    broker_identity = None

    # ---------------------------------------------------------------------

    def __init__(self, name, port=5555, *args, **kwargs):
        """Initialize broker state."""
        super(MDPBroker, self).__init__(name, *args, **kwargs)
        self.blockdiag_config["shape"] = "cloud"
        self.port = port
        self.broker_identity = uuid().get_hex()
        self.services = {}
        self.workers = {}
        self.context = zmq.Context()
        self.broker_socket = self.context.socket(zmq.ROUTER)
        self.broker_socket.linger = 0
        self.broker_socket.identity = self.broker_identity
        self.poller = zmq.Poller()
        self.poller.register(self.broker_socket, zmq.POLLIN)

        self.logger.info("MDPBroker {0} initialized. Client/Worker ID will be {1}".format(self.broker_identity, self.broker_identity))
        self.bind(self.port, *args, **kwargs)

    # ---------------------------------------------------------------------

    def mediate(self):
        """Main broker work happens here"""
        while self.loop():
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                break 
            if items:
                msg = self.broker_socket.recv_multipart()
                self.process_message(msg)

            self.purge_workers()
            self.registrator.register() # ADD THIS TO THE HEARTBEAT_MANAGER

    def process_message(self, message):
        """
        A multipart message must have the following format in the first frames to be successfully processed by the broker
        Syntax: [sender, '', header, command, ..., ..., ...]
            - sender    :   The identity of the originating socket
            - ''        :   An empty 'delimiter' so that we can retain the possibility of maintaining multiple sender chains for a single socket in the future
                                (e.g.) [sender1, sender2, sender3, '', ...] The empty delimiter frame denotes the end of all 'sender' addresses to be maintained in routing
            - header    :   The type of peer that the message is coming from, a client or a worker. Most processing is dependant on the peer type  
            - command   :   The command from the peer. There are multiple commands definied for the MajorDomo Infrastructure. See 'MDPDefinition' module
            - ...       :   The rest of the message content to actually be used by the worker itself - all of these frame(s) are irrelevant to the broker
        """


        if len(message) >= 4:
            sender = message.pop(0)
            empty = message.pop(0)
            assert empty == ''
            header = message.pop(0)
            command = message.pop(0)

            if (MDPDefinition.C_CLIENT == header):
                self.process_client_message(sender, command, message)
            elif (MDPDefinition.W_WORKER == header):
                self.process_worker_message(sender, command, message)
            else:
                self.logger.error("Received invalid message: {0}")
        else:
            self.logger.error("Received invalid message: {0}".format(message))

    def process_client_message(self, sender, command, message):
        """Process a request coming from a client."""

        if command == MDPDefinition.C_REQUEST:
            # TODO: PROCESS AND LOG REQUEST
            assert len(message) >= 2 # Service name + body
            service = message.pop(0)
            service = self.get_or_create_service(service)
            request_id = message[0]
            self.logger.info("Received client request for service {0} ({1} waiting workers)".format(service.name, len(service.workers)), log_entry_id=request_id)
            # Set reply return address to client sender
            message = [sender,''] + message
            self.dispatch(service, message=message, log_entry_id=request_id)
        elif command == MDPDefinition.B_VERIFICATION_REQUEST:
            self.logger.info("Received verification request from upstream client {0}".format(sender))
            self.broker_socket.send_multipart([b"{0}_receiver".format(sender), '', MDPDefinition.C_CLIENT, MDPDefinition.B_VERIFICATION_RESPONSE, self.broker_identity])

    def process_worker_message(self, sender, command, msg):
        """Process message sent to us by a worker."""

        worker_exists = self.workers.get(sender) is not None
        

        if command == MDPDefinition.B_VERIFICATION_REQUEST:
            """
            This verification request serves two purposes from a worker. 
                A) To respond and verify to the worker that the broker can be connected to, so to add it to potential brokers the worker may communicate with
                B) To register that worker with a service with the MDPBroker
            """
            
            if len(msg) >= 1: # Message must contain the service name to properly register
                service = msg.pop(0)
                # Not first command in session or Reserved service name
                if not worker_exists:
                    
                    self.logger.info("Received verification and registration request from new downstream worker {0} for service {1}".format(sender, service))
                    worker = self.get_or_create_worker(sender)
                    worker.service = self.get_or_create_service(service)
                    worker.service.workers.append(worker)
                    worker.register_heartbeat()
                    self.dispatch(worker.service)
                    self.logger.info("Registered new worker {0} with service {1}. Service has {2} workers total".format(worker.identity, worker.service.name, len(worker.service.workers)))
                    self.send_to_worker(worker, MDPDefinition.B_VERIFICATION_RESPONSE, self.broker_identity)
                else:
                    self.logger.info("Received verification and registration request from existing downstream worker {0} for service {1}".format(sender, service))

        elif MDPDefinition.W_REPLY == command:
            client = b"{0}_receiver".format(msg.pop(0))
            request_id = msg[1]
            msg = [client, '', MDPDefinition.C_CLIENT, MDPDefinition.W_REPLY] + msg
            self.logger.info("Received worker response, routing to waiting client...", log_entry_id=request_id)
            self.broker_socket.send_multipart(msg)

        elif MDPDefinition.W_HEARTBEAT == command:
            if worker_exists:
                worker = self.get_or_create_worker(sender)
                worker.register_heartbeat()
            else:
                # INCOMPLETE
                self.logger.warn("Received heartbeat for non-existant worker {0}. Ordering worker disconnect.".format(sender)); # TODO: Add a reconnect request to worker here
                worker = self.get_or_create_worker(sender, include_in_worker_pool=False)
                self.send_to_worker(worker, MDPDefinition.W_DISCONNECT, self.broker_identity)

        elif (MDPDefinition.W_DISCONNECT == command):
            pass # TODO: Initiate a worker disconnect, as requested (in the event of a clean or exceptional shutdown)
        else:
            self.logger.error("Invalid message received: {0} <sender, command, message>".format([sender, command, msg]))

    def get_or_create_worker(self, address, include_in_worker_pool=True):
        """Finds the worker (creates if necessary)."""
        assert (address is not None)
        
        identity = address
        worker = self.workers.get(identity)

        if (worker is None):
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY)
            if include_in_worker_pool:
                self.workers[identity] = worker

        return worker

    def get_or_create_service(self, service_name):
        """Locates the service (creates if necessary)."""
        assert (service_name is not None)
        service = self.services.get(service_name)

        if (service is None):
            service = Service(service_name)
            self.services[service_name] = service

        return service

    def bind(self, port, *args, **kwargs):
        """Bind broker to endpoint, can call this multiple times.

        We use a single socket for both clients and workers.
        """
        endpoint = "tcp://*:{0}".format(port)
        self.broker_socket.bind(endpoint)
        self.registrator = BrokerRegistrator(broker_port=port, broker_identity=self.broker_identity, *args, **kwargs)
        self.logger.info("MDPBroker {0} is bound and listening at {1}".format(self.broker_identity, endpoint))

    def purge_workers(self):
        """Look for & kill expired workers"""
        workers = self.workers.values()
        for worker in workers:
            if worker.expiry < time.time():
                if worker.liveness == 0:
                    self.logger.info("Downstream worker {0} in service {1} has expired and reached 0 liveness. Last heartbeat was received {2} seconds ago".format(worker.identity, 
                                                                                                                                                                           worker.service.name, 
                                                                                                                                                                           "{0:.2f}".format(time.time() - worker.last_heartbeat)))
                    self.delete_worker(worker)
                else:
                    self.logger.info("Downstream worker {0} in service {1} missed heartbeat window. Current liveness is {2} (MAX: {3}). Last heartbeat was received {4} seconds ago".format(worker.identity, 
                                                                                                                                                                                                   worker.service.name, 
                                                                                                                                                                                                   worker.liveness,
                                                                                                                                                                                                   worker.MAX_LIVENESS, 
                                                                                                                                                                                                   "{0:.2f}".format(time.time() - worker.last_heartbeat)))
                    worker.reduce_liveness()

    def delete_worker(self, worker):
        """Deletes worker from all data structures, and deletes worker."""
        assert worker is not None

        if worker.service is not None:
            service = worker.service
            service.workers.remove(worker)
            self.logger.info("Deleting worker {0} from service {1}. Service has {2} workers remaining".format(worker.identity, service.name, len(service.workers)))

        # In the event that this worker is unhealthy and not completely shut down, send a disconnect command
        self.send_to_worker(worker, MDPDefinition.W_DISCONNECT, message=self.broker_identity)

        del self.workers[worker.identity]

    def dispatch(self, service, message=None, log_entry_id=None):
        """
        Dispatch requests to waiting workers as possible. This will flush an entire service queue if backed up requests exist for a previously workerless service if a
        new worker for that service is connected 
        """
        assert (service is not None)
        if message is not None:                                                     # Queue message if any
            service.requests.append(message)

        self.purge_workers()

        healthy_workers = []                                                        # We only forward to workers that are fully alive

        for worker in service.workers:
            if worker.liveness == worker.MAX_LIVENESS:
                healthy_workers.append(worker)

        if len(healthy_workers) > 0:
            while service.requests:
                message = service.requests.pop(0)
                worker = None

                if len(service.workers) > 1:                                        # Don't pop() and append() if only 1 worker exists for the service, as that is silly
                    if len(healthy_workers) > 1:                                    # Don't pop() and append() healthy workers if only 1 healthy worker exists, as that is also silly
                        worker = service.workers.pop(service.workers.index(healthy_workers.pop(0)))
                        healthy_workers.append(worker)                              # Place this worker at the end of the current healthy workers queue
                        service.workers.append(worker)                              # Place this worker in the end of the service queue
                    else:
                        worker = healthy_workers[0]
                        if service.workers.index(worker) < len(service.workers):    # Cycle the healthy worker to the end of the services total queue if it is not already
                            service.workers.pop(service.workers.index(worker))
                            service.workers.append(worker) 
                            
                    self.send_to_worker(worker, MDPDefinition.W_REQUEST, message=message)
                else:
                    worker = healthy_workers[0]
                    self.send_to_worker(worker, MDPDefinition.W_REQUEST, message=message)
        else:
            if message is not None:                                                 # Only log once, when a new message is received
                self.logger.error("Request for service {0} has no waiting healthy workers, placing in holding queue. Queue size is {1}.".format(service.name, len(service.requests)), log_entry_id=log_entry_id)


    def send_to_worker(self, worker, command, message=None, worker_identity=None, *args, **kwargs):
        """
        Send message to worker.
        If message is provided, sends that message.
        """

        if message is None:
            message = []
        elif not isinstance(message, list):
            message = [message]

        message = [worker.inbound_address, '', MDPDefinition.W_WORKER, command] + message

        self.broker_socket.send_multipart(message)

    def pre_hook(self):
        gevent.spawn(self.mediate)

    def consume(self, *args, **kwargs):
        self.logger.warn("Consume was called on the broker, no action will be taken")
