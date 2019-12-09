from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import sys
import time
import random

src_ip = sys.argv[1]
dst_ip = sys.argv[2]
coflow_id = int(sys.argv[3])
arrival_time = int(sys.argv[4]) # ms
flow_num = int(sys.argv[5])

def main():
    # Define IP header
    ip = IP(src = src_ip, dst = dst_ip)
    time.sleep(arrival_time/1000)
    print '[INFO] Send packet with coflowId'
    print 'coflow id: ', str(coflow_id)

    # generate 800 packets with a mu of 2 events per unit time
    packet_num = 800
    intervals = [random.expovariate(2) for i in range(packet_num)]
    intervals.sort(reverse=True)

    for i in range(packet_num):
        time.sleep(intervals[i])
        coflow = Protocol(coflowId = coflow_id, ArrivalTime = arrival_time, FlowNum = flow_num)
        payload = bytearray(1200)
        tcp = TCP()
        packet = ip / tcp / coflow / Raw(payload)
        send(packet)
        # packet.show()
    

if __name__ == '__main__':
    main()
