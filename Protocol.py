#!/usr/bin/env python

from scapy.all import *

class Protocol(Packet):
    # Set the name of protocol
    name = 'Coflow'

    # Define the fields in protocol
    fields_desc = [
        IntField('CoflowId', 0),
        IntField('ArrivalTime', 0),
        IntField('FlowNum', 0),
        IntField('MapperId', 0),
        IntField('ReducerId', 0),
        IntField('PacketArrival',0),
        IntField('PacketSize',0)
    ]

# bind_layers(TCP, Protocol, frag = 0, proto = 99)
# conf.stats_classic_protocols += [Protocol]
# conf.stats_dot11_protocols += [Protocol]