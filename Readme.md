# Generate Coflow
## Execute
1. execute `python parse_FB_flow.py` to get coflow_data.json
2. execute `ryu-manager --observe-links controller.py` to start ryu controller
3. open another terminal and execute `sudo python fatTree_topo.py` to create topology and send flow
4. after all flow finished, use `exit` to leave mininet console and execute `sudo mn -c` to clean up the mininet topology, and controller will also stop
5. execute `./fix_pcap.sh` to fix pcap file
6. execute `python parse_pcap.py` to get `result.csv`
## Detail
### Pre-processing
* FB2010-1Hr-150-0.txt
  * CODA使用的FB資料集
  * [github](https://github.com/coflow/coflow-benchmark/blob/master/FB2010-1Hr-150-0.txt)
  * 一共有526個coflow，150個Mapper&150個Reducer
    * Coflow ID
    * Arrival time (ms)
    * Mapper num (mapper num in this coflow)
    * Mapper list
    * Reducer num (reducer num in this coflow)
    * Reducer list
    * Reducer data (total data size which Reducer recieved (megabytes))
* parse_FB_flow.py
  * parse coflow data from `FB2010-1Hr-150-0.txt` into json file(`coflow_data.json`)
  * 這裡會random產生每個coflow中封包的mean和variance，方便之後normal distribution產生封包(這個可以自己設)
    * Packet mean: mean
    * Packet scale: variance
### Controller
* controller. py
  * ryu controller
  * find the shortest path in topology
  * reference from [this](https://github.com/wildan2711/multipath) github
### Topology
* fatTree_topo.py
  * create a fat-tree topology
    * set pod = 4, density = 2
    * generate 16 hosts
    * 8 hosts for sender (h001, h002, ..., h008)
    * 8 hosts for reciever (h009, h010, ..., h016)
  * read coflow_data.json and create task file of each src host
    * assign 150 Mappers and 150 Reducers on these host
      * mapper_id mod src _host_num
      * reducer_id mod dst_host_num
    * each task file contains all flow data which the host has to send
      * Coflow ID
      * Arrival time
      * Flow Number
        * total number of flow in this coflow
      * Mapper ID
      * Reducer ID
      * Dst list
        * all ip of dst host which src host has to send flow to
      * Dst data
        * flow size which dst host will recieve from this src host
      * Packet mean
      * Packet scale
    * 注意: 150個coflow太多了，傳不完，目前設定10個coflow(可以在COFLOW_RANGE改)
  * execute `tcpdump` command in each dst host to record packet in background
  * after the last coflow starting, we check whether src host still has sent packet or not, if all src hosts stop to send packet, kill tcpdump command in each dst host to stop recording packet and write all information to pcap file in `./result/origin/`
    * 注意: 這段被我註解掉了，這種執行方式封包有時候會漏發，不知道bug在哪
    * 通常我會改成在mininet介面下開1~8 host的xterm，一個個執行TCP_sender.py，這樣也方便觀察發送情況，出錯也看得到
### Send Packet
* Protocol.py
  * customize protocol to record coflow data in packet header
    * CoflowId
    * ArrivalTime: coflow arrival time
    * FlowNum: total flow number in this coflow
    * MapperId
    * ReducerId
    * PacketArrival: packet arrival time
      * 注意: 這是個bug...packet arrival time應該要從pcap檔裏面抓才對，但我只抓到packet的相對時間，沒有絕對時間的話沒辦法跟其他pcap檔案比(一共有8個pcap)，所以才在這裡加PacketArrival(實際上我存到的資料只是packet的發送時間)，如果你能抓出絕對時間，請告訴我QAQ
    * PacketSize
* TCP_sender.py
  * 傳送封包的檔案，基本上fatTree_topo.py會自動執行啦，如果出問題的話可以自己在mininet介面上手動開8個host的xterm，一個個執行這個檔案
  * `python TCP_sender.py host_name host_ip`
  * ex. `python TCP_sender.py h001 10.0.0.1`
  * 一共要執行8次TCP_sender.py(每個src host都要執行一次)
  * 因為一個host同時代表多個Mapper，需要同時執行很多個coflow，所以這裡每個coflow就開一個thread傳(這樣才能overlap)
  * 每個coflow裡面的flow不會同時開始，要有一點時間差，所以加上一個sleep_time_list，代表每個flow的延遲時間，單位是一個封包的傳送時間(比如有個flow的sleep time是8，那就要等8個packet傳完之後才能開始)
  * 有的coflow需要傳很久，所以設定一個SEND_TIME，讓他傳完一段時間後就結束
  * sendData這個function是用來讓coflow傳packet的function，因為要讓flow之間彼此也overlap，所以用迴圈的方式讓每個開始傳送的flow輪流發封包
## PCAP
*  fix_pcap.sh
   *  because we will kill to stop tcpdump, it causes pcap file broken
   *  use pcapfix command to repair all pcap files
*  parse_pcap.py
   *  parse data from payload
