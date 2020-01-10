from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import sys
import time
import json
import threading
import random

host_name = sys.argv[1]
host_ip = sys.argv[2]

FILE_NAME = "./task/" + host_name + "_task.json"

def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def sendData(coflow_id, arrival_time, flow_number, flow_list):
    packet_count = [0] * len(flow_list) # number of packet which mapper n sends to a reducer
    min_packet_num = [min(flow_list[0]['datas'])] * len(flow_list)
    count = 0 # count for waking up flow 
    flow_index = 0 # flow index which is waiting for adding to start list
    i = 0 # count for flow which is in start list
    start_flow = []
    while True:
        # wake up flow
        if flow_index < len(flow_list): 
            while count >= flow_list[flow_index]['sleep']:
                start_flow.append(flow_list[flow_index])
                flow_index += 1
                if flow_index == len(flow_list):
                    break
        # send a packet to all reducer
        for j in range(len(start_flow[i]['dst'])):
            # send packet
            ip = IP(src = host_ip, dst = start_flow[i]['dst'][j])
            coflow = Protocol(CoflowId = coflow_id, ArrivalTime = arrival_time, FlowNum = flow_number, MapperId = start_flow[i]['mapper'], ReducerId = start_flow[i]['reducers'][j])
            tcp = TCP()
            packet = ip / tcp / coflow / Raw(RandString(size=65469)) # the max payload size
            send(packet) 
            count += 1 # packet number
        packet_count[i] += 1
        # delete the complete flow dst
        while packet_count[i] == min_packet_num[i]:
            tmp_index = start_flow[i]['datas'].index(min_packet_num[i])
            del start_flow[i]['datas'][tmp_index]
            del start_flow[i]['dst'][tmp_index]
            del start_flow[i]['reducers'][tmp_index]
            if len(start_flow[i]['datas']) == 0:
                break
            else:
                min_packet_num[i] = min(start_flow[i]['datas'])
        # delete the complete flow src
        if len(start_flow[i]['dst']) == 0:
            del start_flow[i]
        # terminate while loop
        if len(start_flow) == 0 and flow_index == len(flow_list):
            break
        # continue
        else:
            i += 1
            if i >= len(start_flow) and len(start_flow) != 0:
                i = 0

if __name__ == '__main__':
    print "(sender) " + host_name + " reading tasking file: " + FILE_NAME
    task_data = readData()

    thread_list = []
    flow_count = 0
    time_count = 0 # 0.1s
    while flow_count < len(task_data):
        dst_list = []
        dst_data = []
        reducers = []
        coflow_id = 0
        arrival_time = 0
        flow_number = 0
        mapper_id = []
        while time_count == int(task_data[flow_count]["Arrival time"]/100):
            dst_list.append(task_data[flow_count]["Dst list"])  
            dst_data.append(task_data[flow_count]["Dst data"])
            reducers.append(task_data[flow_count]["Reducer ID"])
            coflow_id = task_data[flow_count]["Coflow ID"]
            arrival_time = task_data[flow_count]["Arrival time"]
            flow_number = task_data[flow_count]["Flow Number"]
            mapper_id.append(task_data[flow_count]["Mapper ID"])
            flow_count += 1
            if flow_count >= len(task_data):
                break
        if dst_list != []:
            dst_data_ = [int(i)*1024/65536 for i in dst_data[0]] # packet number in a flow
            sleep_time_list = [random.randint(0, 100) for i in range(len(mapper_id))] # a packet time
            sleep_time_list[random.randint(0, len(mapper_id)-1)] = 0
            find_min = [i for i in sleep_time_list]
            flow_list = []
            while find_min != []:
                tmp = {}
                min_sleep_index = sleep_time_list.index(min(find_min))
                find_min.remove(min(find_min))
                tmp['sleep'] = sleep_time_list[min_sleep_index]
                tmp['mapper'] = mapper_id[min_sleep_index]
                tmp['reducers'] = reducers[0]
                tmp['datas'] = dst_data_
                tmp['dst'] = dst_list[0]
                flow_list.append(tmp)
            t = threading.Thread(target=sendData, name="coflow" + str(coflow_id), args=(coflow_id, arrival_time, flow_number, flow_list,))
            print t.getName(), "start"
            t.start()
            thread_list.append(t)
        time.sleep(0.1)
        time_count += 1
    for t in thread_list:
        t.join()
    print "Exiting"
        

# ip = IP(src = ['10.0.0.1'], dst = ['10.0.0.2', '10.0.0.2', '10.0.0.3','10.0.0.2'])
# coflow = Protocol(CoflowId = 1, ArrivalTime = 0, FlowNum = 1)
# # payload = bytearray(1012) # with 12 bytes customized header, total 1024 bytes
# tcp = TCP()
# packet = ip / tcp / coflow / Raw(RandString(size=60000))
# print "send packet"
# send(packet) 