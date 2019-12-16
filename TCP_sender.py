from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import sys
import time
import json

host_name = sys.argv[1]
host_ip = sys.argv[2]

FILE_NAME = "./task/" + host_name + "_task.json"


def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

if __name__ == '__main__':
    print "(sender) " + host_name + "reading tasking file: " + FILE_NAME
    task_data = readData()
    print "(sender) " + host_name + " starts to send flow"
    for i in range(len(task_data)):
        if i != 0:
            # interval time
            sleep_time = task_data[i]["Arrival time"] - task_data[i-1]["Arrival time"]
            time.sleep(sleep_time/1000)
        for j in range(len(task_data[i]["Reducer list"])):
            # in KB(= 1024 bytes = 1 packet)
            packet_num = int(task_data[i]["Reducer data"][j])
            ip = IP(src = host_ip, dst = task_data[i]["Reducer list"][j])
            for k in range(packet_num):
                coflow = Protocol(CoflowId = task_data[i]["Coflow ID"], ArrivalTime = task_data[i]["Arrival time"], FlowNum = task_data[i]["Flow Number"])
                payload = bytearray(1012) # with 12 bytes customized header, total 1024 bytes
                tcp = TCP()
                packet = ip / tcp / coflow / Raw(payload)
                send(packet) 
