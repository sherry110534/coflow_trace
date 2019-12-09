from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import os
import csv

dir = './result/fix/'
output = './result/result.csv'
file = os.listdir(dir)

with open(output, 'w') as csvfile:
    w = csv.writer(csvfile)
    w.writerow([coflowId', 'ArrivalTime', 'FlowNum', 'src', 'dst'])
    for filename in file:
        packet = rdpcap(dir + filename)
        for i in range(len(packet[TCP])):
            w.writerow([Protocol(str(packet[TCP][0])).coflowId, Protocol(str(packet[TCP][0])).ArrivalTime, Protocol(str(packet[TCP][0])).FlowNum, packet[TCP][0].src, packet[TCP][0].dst])
