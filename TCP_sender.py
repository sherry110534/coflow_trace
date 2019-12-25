from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import sys
import time
import json
import threading

host_name = sys.argv[1]
host_ip = sys.argv[2]
coflow_id = sys.argv[3]

FILE_NAME = "./task/" + host_name + "_task.json"

def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def sendData(flow_data):
    dst_data = [i*1024/65536 for i in flow_data["Dst data"]]
    packet_count = 0
    max_packet_num = max(flow_data["Dst data"])
    min_packet_num = min(flow_data["Dst data"])
    while packet_count < max_packet_num:
        while packet_count == min_packet_num:
            del flow_data["Dst list"][flow_data["Dst data"].index(packet_count)]
            flow_data["Dst data"].remove(packet_count)
            min_packet_num = min(flow_data["Dst data"])
        # send packet
        ip = IP(src = host_ip, dst = flow_data["Dst list"])
        coflow = Protocol(CoflowId = flow_data["Coflow ID"], ArrivalTime = flow_data["Arrival time"], FlowNum = flow_data["Flow Number"])
        # payload = bytearray(1012) # with 12 bytes customized header, total 1024 bytes
        tcp = TCP()
        packet = ip / tcp / coflow / Raw(RandString(size=65469))
        print "send packet"
        send(packet) 
        packet_count += 1

if __name__ == '__main__':
    print "(sender) " + host_name + " reading tasking file: " + FILE_NAME
    task_data = readData()
    print "(sender) " + host_name + " starts to send coflow " + coflow_id
    flow_list = []
    for index in range(len(task_data)):
        if str(task_data[index]["Coflow ID"]) == str(coflow_id):
            flow_list.append(task_data[index])
    if len(flow_list) == 1:
        sendData(flow_list[0])
    else: # use thread
        thread_list = []
        for flow in flow_list:
            t = threading.Thread(target=sendData, args=(flow, ))
            thread_list.append(t)
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()
        print "Exiting"


#        packet = ip / tcp / coflow / Raw(RandString(size=60000))

# ip = IP(src = ['10.0.0.1'], dst = '10.0.0.2')
# coflow = Protocol(CoflowId = 1, ArrivalTime = 0, FlowNum = 1)
# # payload = bytearray(1012) # with 12 bytes customized header, total 1024 bytes
# tcp = TCP()
# packet = ip / tcp / coflow / Raw(RandString(size=60000))
# print "send packet"
# sr1(packet) 