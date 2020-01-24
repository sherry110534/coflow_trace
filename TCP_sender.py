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
RECORD_FILE = "./result/record/" + host_name + "_time.txt"
FLOW_FILE = "./result/record/" + host_name + "_flow_time.txt"

def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def sendData(coflow_data, sleep_time):
    print "coflow ", coflow_data["Coflow ID"], " start"

    now_packet_count = [0] * len(coflow_data["Mapper ID"]) # record the number of packet which sended by this mapper
    min_packet_num = [min(coflow_data["Dst data"])] * len(coflow_data["Mapper ID"]) # record the min number of packet in this mapper send list
    all_flow_time = [] # record flow start and end time
    time_count_ = 0 # time count for waking up flow 
    flow_index = 0 # index of flow which added into start_mapper list
    start_mapper = []
    start_time = time.time()
    while True:
        while flow_index < len(coflow_data["Mapper ID"]):
            if time_count_ >= sleep_time[flow_index]: # this flow starts
                this_mapper = {}
                this_mapper["Mapper ID"] = coflow_data["Mapper ID"][flow_index]
                this_mapper["Dst data"] = list(coflow_data["Dst data"])
                this_mapper["Reducer ID"] = list(coflow_data["Reducer ID"])
                this_mapper["Dst list"] = list(coflow_data["Dst list"])
                start_mapper.append(this_mapper)
                all_flow_time.append(time.time())
                flow_index += 1
            else:
                break
        all_skip = True # check if all flow skip, time count+1
        for i in range(len(start_mapper)):
            # send a packet to all reducer
            if len(start_mapper[i]["Dst list"]) == 0: # skip if this flow completed
                continue
            else:
                all_skip = False
                for j in range(len(start_mapper[i]["Dst list"])):
                    # send packet
                    ip = IP(src = host_ip, dst = start_mapper[i]["Dst list"][j])
                    coflow = Protocol(CoflowId = coflow_data["Coflow ID"], ArrivalTime = coflow_data["Arrival time"], FlowNum = coflow_data["Flow Number"], MapperId = start_mapper[i]["Mapper ID"], ReducerId = start_mapper[i]["Reducer ID"][j])
                    tcp = TCP()
                    packet = ip / tcp / coflow / Raw(RandString(size=65469)) # the max payload size
                    send(packet) 
                    time_count_ += 1
                now_packet_count[i] += 1
                # delete the complete flow dst
                while now_packet_count[i] >= min_packet_num[i] and  min_packet_num[i] != -1:
                    tmp_index = start_mapper[i]["Dst data"].index(min_packet_num[i])
                    now_time = time.time()
                    record = "COFLOWID" + str(coflow_data["Coflow ID"]) + "\t" + str(start_mapper[i]["Mapper ID"]) + "\t" + str(start_mapper[i]["Reducer ID"][tmp_index]) + "\t" + str(start_mapper[i]["Dst data"][tmp_index]*65469/1024) + "\t" + str(all_flow_time[i]) + "\t" + str(now_time) + "\t" + str(now_time-all_flow_time[i]) + "\n"
                    ff.write(record)
                    del start_mapper[i]["Dst data"][tmp_index]
                    del start_mapper[i]["Dst list"][tmp_index]
                    del start_mapper[i]["Reducer ID"][tmp_index]
                    if start_mapper[i]["Dst data"] != []:
                        min_packet_num[i] = min(start_mapper[i]["Dst data"])
                    else:
                        min_packet_num[i] = -1 # complete
                        break
        if all_skip:
            time_count_ += 1

        # terminate while loop
        complete = True
        for m in min_packet_num:
            if m != -1:
                complete = False
                break
        if complete:
            print str(coflow_data["Coflow ID"]), " complete!"
            end_time = time.time()
            record = "COFLOWID" + str(coflow_data["Coflow ID"]) + "\t" + str(coflow_data["Flow Number"]) + "\t" + str(start_time) + "\t" + str(end_time) + "\t" + str(end_time-start_time) + "\n"
            fw.write(record)
            break

if __name__ == '__main__':
    print "(sender) " + host_name + " reading tasking file: " + FILE_NAME
    task_data = readData()
    fw = open(RECORD_FILE, "w+")
    ff = open(FLOW_FILE, "w+")
    fw.write("CoflowID\tFlowNum\tStarttime\tEndtime\tInterval\n")
    ff.write("CoflowID\tMapper\tReducer\tDataSize(KB)\tStarttime\tEndtime\tInterval\n")
    # create coflow
    thread_list = []
    for flow_count in range(len(task_data)):
        coflow_data = {}
        coflow_data = task_data[flow_count].copy()
        coflow_data["Dst data"] = [int(i)*1024/65469 for i in task_data[flow_count]["Dst data"]]  # cout packet number in a flow
        # create the delay time for all flows in the same coflow
        sleep_time_list = [random.randint(0, min(100,max(coflow_data["Dst data"]))) for i in range(len(coflow_data["Mapper ID"]))] # the time slot is a packet time
        sleep_time_list[random.randint(0, len(coflow_data["Mapper ID"])-1)] = 0 # one flow needs to start at 0s
        sleep_time = sorted(sleep_time_list)
        t = threading.Timer(float(task_data[flow_count]["Arrival time"]/1000),sendData, args=(coflow_data, sleep_time))
        t._name = "coflow" + str(task_data[flow_count]["Coflow ID"])
        thread_list.append(t)
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()
    print "Exiting"
    fw.close()
    ff.close()