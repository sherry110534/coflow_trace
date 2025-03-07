from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import sys
import time
import json
import threading
import random
import numpy as np

host_name = sys.argv[1]
host_ip = sys.argv[2]

FILE_NAME = "./task/" + host_name + "_task.json"
RECORD_FILE = "./result/record/" + host_name + "_time.txt"
FLOW_FILE = "./result/record/" + host_name + "_flow_time.txt"
if not os.path.isdir("./result/record/"):
    os.mkdir("./result/record/")
SEND_TIME = 180 # seconds

def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def sendData(coflow_data, sleep_time, packets_size_list):
    print "coflow ", coflow_data["Coflow ID"], " start"
    this_thread = threading.currentThread()

    now_packet_count = [0] * len(coflow_data["Mapper ID"]) # record the number of packet which sended by this mapper
    min_packet_num = [min(coflow_data["Dst data"])] * len(coflow_data["Mapper ID"]) # record the min number of packet in this mapper send list
    all_flow_time = [] # record flow start and end time
    time_count_ = 0 # time count for waking up flow 
    flow_index = 0 # index of flow which added into start_mapper list
    start_mapper = []
    timeout = True
    start_time = time.time()
    while getattr(this_thread, "do_run", True):
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
                    this_packet_size = int(random.choice(packets_size_list)) + 28 # + header
                    coflow = Protocol(CoflowId = coflow_data["Coflow ID"], ArrivalTime = coflow_data["Arrival time"], FlowNum = coflow_data["Flow Number"], MapperId = start_mapper[i]["Mapper ID"], ReducerId = start_mapper[i]["Reducer ID"][j], PacketArrival = int(time.time()), PacketSize = this_packet_size)
                    tcp = TCP()
                    packet = ip / tcp / coflow / Raw(RandString(size=this_packet_size))
                    send(packet) 
                    time_count_ += 1
                now_packet_count[i] += 1
                # delete the complete flow dst
                while now_packet_count[i] >= min_packet_num[i] and  min_packet_num[i] != -1:
                    tmp_index = start_mapper[i]["Dst data"].index(min_packet_num[i])
                    now_time = time.time()
                    # record = "COFLOWID" + str(coflow_data["Coflow ID"]) + "\t" + str(start_mapper[i]["Mapper ID"]) + "\t" + str(start_mapper[i]["Reducer ID"][tmp_index]) + "\t" + str(start_mapper[i]["Dst data"][tmp_index]*1024/1024) + "\t" + str(all_flow_time[i]) + "\t" + str(now_time) + "\t" + str(now_time-all_flow_time[i]) + "\n"
                    # ff.write(record)
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
            timeout = False
            print str(coflow_data["Coflow ID"]), " complete!"
            end_time = time.time()
            record = "COFLOWID" + str(coflow_data["Coflow ID"]) + "\t" + str(coflow_data["Flow Number"]) + "\t" + str(start_time) + "\t" + str(end_time) + "\t" + str(end_time-start_time) + "\n"
            fw.write(record)
            break
    if timeout: # end by main process
        print str(coflow_data["Coflow ID"]), " complete! (timeout)"
        end_time = time.time()
        record = "COFLOWID" + str(coflow_data["Coflow ID"]) + "\t" + str(coflow_data["Flow Number"]) + "\t" + str(start_time) + "\t" + str(end_time) + "\t" + str(end_time-start_time) + "\n"
        fw.write(record)

    

if __name__ == '__main__':
    print "(sender) " + host_name + " reading tasking file: " + FILE_NAME
    task_data = readData()
    fw = open(RECORD_FILE, "w+")
    # ff = open(FLOW_FILE, "w+")
    fw.write("CoflowID\tFlowNum\tStarttime\tEndtime\tInterval\n")
    # ff.write("CoflowID\tMapper\tReducer\tDataSize(KB)\tStarttime\tEndtime\tInterval\n")
    # create coflow
    thread_list = []
    time_to_stop = []
    for flow_count in range(len(task_data)):
        coflow_data = {}
        coflow_data = task_data[flow_count].copy()
        packet_size_list = np.random.normal(loc=int(task_data[flow_count]["Packet mean"]), scale=int(task_data[flow_count]["Packet scale"]), size=10000) 
        for i in range(len(packet_size_list)):
            while packet_size_list[i] < 0:
                packet_size_list[i] ==  np.random.normal(loc=int(task_data[flow_count]["Packet mean"]), scale=int(task_data[flow_count]["Packet scale"]), size=1)[0]
        coflow_data["Dst data"] = [int(i)*1024/int(task_data[flow_count]["Packet mean"]) for i in task_data[flow_count]["Dst data"]]  # cout packet number in a flow
        # create the delay time for all flows in the same coflow
        sleep_time_list = [random.randint(0, min(100,max(coflow_data["Dst data"]))) for i in range(len(coflow_data["Mapper ID"]))] # the time slot is a packet time
        sleep_time_list[random.randint(0, len(coflow_data["Mapper ID"])-1)] = 0 # one flow needs to start at 0s
        sleep_time = sorted(sleep_time_list)
        t = threading.Timer(float(task_data[flow_count]["Arrival time"]/1000),sendData, args=(coflow_data, sleep_time, packet_size_list))
        t._name = "coflow" + str(task_data[flow_count]["Coflow ID"])
        thread_list.append(t)
        time_to_stop.append(float(task_data[flow_count]["Arrival time"]/1000)+180)
    for t in thread_list:
        t.start()
        
    # when to stop thread
    timer = 0
    time_index = 0
    while True:
        time.sleep(1)
        timer += 1
        if time_index < len(time_to_stop):
            while time_to_stop[time_index] <= timer:
                thread_list[time_index].do_run = False
                time_index += 1
                if time_index < len(time_to_stop):
                    continue
                else:
                    break
        else:
            break
            
    for t in thread_list:
        t.join()
    print "Exiting"
    fw.close()
    # ff.close()