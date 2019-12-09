#!/usr/bin/env python

from scapy.all import *

'''
Define your own protocol
'''
class Protocol(Packet):
    # Set the name of protocol
    name = 'Coflow'

    # Define the fields in protocol
    fields_desc = [
        IntField('coflowId', 0),
        IntField('ArrivalTime', 0),
        IntField('FlowNum', 0),
    ]

'''
Add customized protocol into IP layer
'''
# bind_layers(TCP, Protocol, frag = 0, proto = 99)
# conf.stats_classic_protocols += [Protocol]
# conf.stats_dot11_protocols += [Protocol]