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

def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def sendData(coflow_id, arrival_time, flow_number, flow_list):
    now_packet_count = [0] * len(flow_list) # record the number of packet which sended by this mapper
    min_packet_num = [min(flow_list[0]['datas'])] * len(flow_list) # record the min number of packet in this mapper send list
    time_count_ = 0 # time count for waking up flow 
    flow_index = 0 # index of flow which added into start_flow list
    start_flow = []
    start_time = time.time()
    while True:
        if flow_index < len(flow_list): # this flow starts
            while time_count_ >= flow_list[flow_index]['sleep']:
                start_flow.append(flow_list[flow_index])
                flow_index += 1
                if flow_index >= len(flow_list):
                    break
        all_skip = True # check if all flow skip, time count+1
        i = -1 # index
        for start_f in start_flow:
            i += 1
            # send a packet to all reducer
            if len(start_f['dst']) == 0: # skip if this flow completed
                continue
            else:
                all_skip = False
                for j in range(len(start_f['dst'])):
                    # send packet
                    ip = IP(src = host_ip, dst = start_f['dst'][j])
                    coflow = Protocol(CoflowId = coflow_id, ArrivalTime = arrival_time, FlowNum = flow_number, MapperId = start_f['mapper'], ReducerId = start_f['reducers'][j])
                    tcp = TCP()
                    packet = ip / tcp / coflow / Raw(RandString(size=65469)) # the max payload size
                    send(packet) 
                    time_count_ += 1
                    if flow_index < len(flow_list): # this flow starts
                        while time_count_ >= flow_list[flow_index]['sleep']:
                            start_flow.append(flow_list[flow_index])
                            flow_index += 1
                            if flow_index >= len(flow_list):
                                break
                star_f_index = i
                now_packet_count[star_f_index] += 1
                # delete the complete flow dst
                while now_packet_count[star_f_index] >= min_packet_num[star_f_index]:
                    tmp_index = start_f['datas'].index(min_packet_num[star_f_index])
                    del start_f['datas'][tmp_index]
                    del start_f['dst'][tmp_index]
                    del start_f['reducers'][tmp_index]
                    if start_f['datas'] != []:
                        min_packet_num[star_f_index] = min(start_f['datas'])
                    else:
                        break
        if all_skip:
            time_count_ += 1
        # terminate while loop
        complete = True
        for s in start_flow:
            if len(s['datas']) != 0:
                complete = False

        if complete and flow_index >= len(flow_list):
            print coflow_id, " complete!"
            end_time = time.time()
            record = str(coflow_id) + "\t" + str(start_time) + "\t" + str(end_time) + "\t" + str(end_time-start_time) + "\n"
            fw.write(record)
            break

if __name__ == '__main__':
    print "(sender) " + host_name + " reading tasking file: " + FILE_NAME
    task_data = readData()
    fw = open(RECORD_FILE, "w+")
    fw.write("CoflowID\tStart time\tEndtime\tInterval\n")
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
        while time_count == int(task_data[flow_count]["Arrival time"]/100): # this coflow starts
            dst_list.append(task_data[flow_count]["Dst list"])  # append the list of destination ip(2-D array)
            dst_data.append(task_data[flow_count]["Dst data"]) # append the list of destination data size(2-D array)
            reducers.append(task_data[flow_count]["Reducer ID"]) # append the list of destination ID(2-D array)
            coflow_id = task_data[flow_count]["Coflow ID"] # this coflow ID 
            arrival_time = task_data[flow_count]["Arrival time"] # arrival time of this coflow
            flow_number = task_data[flow_count]["Flow Number"] # total number of this coflow
            mapper_id.append(task_data[flow_count]["Mapper ID"]) # append source ID(1-D array)
            flow_count += 1
            if flow_count >= len(task_data):
                break
        if coflow_id != 0: # there is a coflow which waits for starting
            # because all mappers in the same coflow has same dst list and data
            # we just need to cout one dst list
            dst_data_ = [int(i)*1024/65536 for i in dst_data[0]]  # cout packet number in a flow
            # create the delay time for all flows in the same coflow
            # the time slot is a packet time
            sleep_time_list = [random.randint(0, min(100,max(dst_data_))) for i in range(len(mapper_id))] 
            sleep_time_list[random.randint(0, len(mapper_id)-1)] = 0 # one flow needs to start at 0s
            find_min = [i for i in sleep_time_list] # tmp list copy from sleep_time_list
            flow_list = [] # flow data list sorted by sleep time
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
    fw.close()
            
        

# ip = IP(src = ['10.0.0.1'], dst = ['10.0.0.2', '10.0.0.2', '10.0.0.3','10.0.0.2'])
# coflow = Protocol(CoflowId = 1, ArrivalTime = 0, FlowNum = 1)
# # payload = bytearray(1012) # with 12 bytes customized header, total 1024 bytes
# tcp = TCP()
# packet = ip / tcp / coflow / Raw(RandString(size=60000))
# print "send packet"
# send(packet) 