"""Majordomo Protocol definitions"""
#  This is the version of MDP/Client we implement
C_CLIENT = "MDPC01"

#  This is the version of MDP/Worker we implement
W_WORKER = "MDPW01"

# This is the version of the MDP/Broker we implement
B_BROKER = "MDPB01"

#  MDP/Server commands, as strings

W_READY         =   "\001"
W_REQUEST       =   "\002"
W_REPLY         =   "\003"
W_HEARTBEAT     =   "\004"
W_DISCONNECT    =   "\005"

"""
C_HEARTBEAT_REQUEST     =   This denotes a fully round-trip heartbeat request to a broker. This is how a client confirms that it personally can connect to a broker,
                            before it adds it to its round-robin load balancing array
"""

B_VERIFICATION_REQUEST    =   "\006"
B_VERIFICATION_RESPONSE   =   "\007"
C_REQUEST               =   "\008"

"""
B_HEARTBEAT     =   Designed to convey readiness of the broker to accept client requests to the RegistrationService
B_DISCONNECT    =   This will be sent to the RegistrationService in the event of a graceful broker shutdown to immediately stop all requests being sent to the broker
                    Alternatively, this will also be sent out to all clients in the event that the broker fails to heartbeat and the broker liveness reaches 0
"""
B_HEARTBEAT     =   "\009"
B_DISCONNECT    =   "\010"

commands = [None, "READY", "REQUEST", "REPLY", "HEARTBEAT", "DISCONNECT"]