from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
import os
import csv

dir = './result/pcap_record/'
output = './result/result.csv'
file = os.listdir(dir)

def string_to_hex(hex_string):
    result = 0
    for i in range(len(hex_string)):
        result += int(hex_string[len(hex_string)-1-i], base=16) * 16**i
    return result
def parse_payload(payload):
    hex_list = []
    for b in payload:
        h = hex(ord(b))[2:]
        if len(h) == 1:
            hex_list.append('0')
        hex_list.append(h)
    hex_string = ''.join(hex_list)
    return hex_string

with open(output, 'w') as csvfile:
    w = csv.writer(csvfile)
    w.writerow(['CoflowId', 'ArrivalTime', 'FlowNum', 'MapperId', 'ReducerId', 'PacketArrival', 'PacketSize', 'src', 'dst'])
    for filename in sorted(file):
        print "start to parse ", filename
        packet = rdpcap(dir + filename)
        for i in range(len(packet[TCP])):
            if packet[TCP][i].ack != 0:
                continue
            try:
                hex_string = parse_payload(packet[TCP][i].load)
                CoflowId_hex = hex_string[0:8]
                ArrivalTime_hex = hex_string[8:16]
                FlowNum_hex = hex_string[16:24]
                MapperId_hex = hex_string[24:32]
                ReducerId_hex = hex_string[32:40]
                PacketArrival = packet[TCP][i].time
                PacketSize_hex = hex_string[48:56]
                # write into csv
                w.writerow([string_to_hex(CoflowId_hex), string_to_hex(ArrivalTime_hex), string_to_hex(FlowNum_hex), string_to_hex(MapperId_hex), string_to_hex(ReducerId_hex), str(PacketArrival), string_to_hex(PacketSize_hex), packet[TCP][0].src, packet[TCP][0].dst])
            except:
                print("error")