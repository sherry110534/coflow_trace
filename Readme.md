# Coflow Trace
## Structure
```
coflow_trace
├── FB2010-1Hr-150-0.txt
├── parse_FB_flow.py
├── coflow_data.json
├── Protocol.py
├── fatTree_topo.py
├── TCP_sender.py
├── controller.py
├── fix_pcap.sh
├── parse_pcap.py
├── task
│   ├── ...
│   └── XXX.json
├── result
│   ├── fix
│   │   ├── ...
│   │   └── XXX.pcap
│   ├── origin
│   │   ├── ...
│   │   └── XXX.pcap
│   └── result.csv
```

## Execute

1. execute `python parse_FB_flow.py` to get **coflow_data.json**
2. execute `ryu-manager --observe-links controller.py` to start ryu controller
3. open another terminal and execute `sudo python fatTree_topo.py` to create topology and send flow
4. after all flow finished, use `exit` to leave mininet console and execute `sudo mn -c` to clean up the mininet topology, and controller will also stop
5. execute `./fix_pcap.sh` to fix pcap file
6. execute `python parse_pcap.py ` to get **result.csv**

## Detail

* parse_FB_flow.py
  * parse coflow data from [FB2010-1Hr-150-0.txt](https://github.com/coflow/coflow-benchmark/blob/master/FB2010-1Hr-150-0.txt) into json file(coflow_data.json)
* coflow_data.json
  * contain 526 coflow datas with 150 Mappers and 150 Reducers
    * Coflow ID
    * Arrival time (ms)
    * Mapper num (mapper num in this coflow)
    * Mapper list
    * Reducer num (reducer num in this coflow)
    * Reducer list
    * Reducer data (total data size which Reducer recieved (megabytes))
* Protocol.py
  * customize protocol to record coflow data in packet header
    * CoflowId
    * ArrivalTime
    * FlowNum
* fatTree_topo.py
  * create a fat-tree topology 
    * set pod = 4, density = 2
    * generate 16 hosts
    * 8 hosts for sender (h001, h002, ..., h008)
    * 8 hosts for reciever (h009, h010, ..., h016)
  * use ping to check all host connected
  * read **coflow_data.json** and create task file of each src host
    * assign 150 Mappers and 150 Reducers on these host
      - mapper_id mod src _host_num
      - reducer_id mod  dst_host_num
    * each task file contains all flow data which the host has to send 
      * Coflow ID
      * Arrival time
      * Flow Number
        * total number of flow in this coflow
      * Dst list
        * all ip of dst host which src host has to send flow to
      * Dst data
        * flow size which dst host will recieve from this src host 
  * execute *tcpdump* command in each dst host to record packet in background
  * execute **TCP_sender.py** in each src host to send flow in background
  * after the last coflow starting, we check whether src host still has sent packet or not
  * if all src hosts stop to send packet, kill *tcpdump* command in each dst host to stop recording packet and write all information to pcap file in **./result/origin/**
* TCP_sender.py
  * all src host will execute this file
  * read json data in **./task/** to get flow information  which this host has to send
  * every packet will contain coflow information and payload (total 1024 bytes)
* controller.py
  * a ryu controller
  * find the shortest path in topology
  * reference from [this](https://github.com/wildan2711/multipath) github
* fix_pcap.sh
  * because we will *kill* to stop *tcpdump*, it causes pcap file broken
  * use *pcapfix* command to repair all pcap files in **./result/origin/**, and output fixed file to **./result/fix/**
* parse_pcap.py
  * read all pcap file in **./result/fix/** and extract coflow data of each packet in pcap file
  * write all data to **./result/result.csv**
