# A Simple UDP class

import socket
import time
import uuid
import zmq.green as zmq
import gevent

from gevent import event
from gevent.pool import Pool

PING_PORT_NUMBER    = 6999  # The UDP port to ping over
PING_INTERVAL       = 1.0   # The time between sending each ping (seconds)
PEER_EXPIRY         = 10   # How many pings a peer can miss before they are reaped
UUID_BYTES          = 32    # The number of bytes in the UUID
BURN_IN_PINGS       = 5     # The number of pings to wait before a master is determined from peers that report in that time

class UDP(object):
    """
    **An interface to handle the sending, receiving, and interpreting of a UDP broadcast**

    Parameters:

        - port (int):       The port to broadcast and receive over
        - address (str):    (Default: None) The local IP address. If None, it will attempt to determine it's own IP
        - broadcast (str):  (Default: None) The specific subnet to broadcast over. If set to none, it will default to 255.255.255.255

    """

    handle = None   # Socket for send/recv
    port = 0        # UDP port we work on
    address = ''    # Own address
    broadcast = ''  # Broadcast address

    def __init__(self, port, address=None, broadcast=None):
        if address is None:
            local_addrs = socket.gethostbyname_ex(socket.gethostname())[-1]
            for addr in local_addrs:
                if not addr.startswith('127'):
                    address = addr
        if broadcast is None:
            broadcast = '255.255.255.255'

        self.address = address
        self.broadcast = broadcast
        self.port = port
        # Create UDP socket
        self.handle = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        # Ask operating system to let us do broadcasts from socket
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Allow socket to be resused by multiple processes
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind UDP socket to local port so we can receive pings
        self.handle.bind(('', port)) 

    def send(self, buf):
        self.handle.sendto(buf, 0, (self.broadcast, self.port))

    def recv(self, service_length, uuid_length):
        message, addrinfo = self.handle.recvfrom(service_length + uuid_length)

        service = message[0:service_length]
        id = message[service_length:service_length + uuid_length]
        return (service, id)


class Peer(object):
    
    uuid = None         # The uuid the peer broadcasted
    expires_at = None   # The time the peer will expire

    def __init__(self, uuid, expiry=None, interval=None):
        self.uuid = uuid
        self.expiry = expiry or PEER_EXPIRY
        self.interval = interval or PING_INTERVAL
        self.is_alive()

    def is_alive(self):
        """Reset the peers expiry time
        
        Call this method whenever we get any activity from a peer.
        """
        self.expires_at = time.time() + (self.expiry * self.interval)

class UDPInterface(object):
    """
    This class is the fully implemented connector that communicates over UDP multicast broadcasts to peers
    registered to the same port and service configuration.

    This class also deterministically elects a master in the peer group
    """
    
    udp = None                 # UDP object
    uuid = None                # Our UUID as binary blob
    peers = None               # Hash of known peers, fast lookup

    def __init__(self, service, logger=None, interval=None, expiry=None, burn_in_pings=None):
        self.pool = Pool()
        self.udp = UDP(PING_PORT_NUMBER)
        self.uuid = uuid.uuid4().hex.encode('utf8')
        self.poller = zmq.Poller()
        self.poller.register(self.udp.handle, zmq.POLLIN)
        self.service = service
        self.logger = logger

        self.expiry = expiry or PEER_EXPIRY
        self.interval = interval or PING_INTERVAL
        self.burn_in_pings = burn_in_pings or BURN_IN_PINGS;

        self.__master_block = event.Event()
        self.__master_block.clear()

        self.__block = event.Event()
        self.__block.clear()

        self.__burned_in = False

        self.__loop = event.Event()
        self.__loop = False
        self.__set_slave()
        self.peers = {}
        
    
    def stop(self):
        self.__loop = False
    
    def start(self):
        self.__loop = True
        self.pool.spawn(self.run)
        self.pool.spawn(self.send_pings)
        self.pool.spawn(self.__burn_in_timer)

    def __burn_in_timer(self):
        """
        A method which exists to clear the block on master determination
        after a suitable burn in time. This is to prevent several instances being brought up at the same time
        recognizing as master and doing work as such
        """

        gevent.sleep(self.burn_in_pings * self.interval)
        self.__burned_in = True


    def block(self):
        self.__block.wait()

    def wait_until_master(self):
        '''Blocks until stop() is called.'''
        self.__master_block.wait()
    
    def send_pings(self, *args, **kwargs):
        while self.__loop:
            try:
                self.udp.send(self.service + self.uuid)
                gevent.sleep(self.interval)
            except Exception as e:
                self.loop.stop()

    def run(self):
        while self.__loop:
            items = self.poller.poll(self.interval)
            if items:
                service, uuid = self.udp.recv(len(self.service), UUID_BYTES)
                if uuid != self.uuid:
                    if service == self.service:
                        if uuid in self.peers:
                            self.peers[uuid].is_alive()
                        else:
                            self.peers[uuid] = Peer(uuid, interval=self.interval, expiry=self.expiry)
                            self.log("New peer ({0}) discovered in '{1}' peer pool. (Total: {2})".format(uuid, self.service, len(self.peers) + 1))
            else:
                gevent.sleep(1)

            self.reap_peers()
            self.determine_master()
    
    def reap_peers(self):
        now = time.time()
        cur_peers = {}
        for uuid, peer in self.peers.iteritems():
            if peer.expires_at < now:
                self.log("Reaping expired peer ({0}) from '{1}' peer pool. (Total remaining: {2})".format(peer.uuid, self.service, len(self.peers) + 1))
            else:
                cur_peers[uuid] = peer
        self.peers = cur_peers
        
    def determine_master(self):
        if self.__burned_in:
            keys = []
            for key in self.peers.iterkeys():
                keys.append(key)

            if len(keys) > 0:
                keys.sort(reverse=True)
                if keys[0] > self.uuid:
                    if self.is_master():
                        self.log("Changing from master to slave")
                    self.__set_slave()
                else:
                    if self.is_slave():
                        self.log("Changing from slave to master")
                    self.__set_master()
            else:
                if self.is_slave():
                    self.log("Changing from slave to master")
                self.__set_master()

    def __set_slave(self):
        self.__is_master = False
        self.__master_block.clear()

    def __set_master(self):
        self.__is_master = True
        self.__master_block.set()

    def is_master(self):
        return self.__is_master

    def is_slave(self):
        return not self.__is_master

    def log(self, message):
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)
