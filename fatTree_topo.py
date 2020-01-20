from mininet.net import Mininet
from mininet.node import Controller, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import Link, Intf, TCLink
from mininet.topo import Topo
from mininet.util import dumpNodeConnections
from scapy.config import conf
conf.ipv6_enabled = False
from scapy.all import *
from Protocol import Protocol
import logging
import os
import time
import json

logging.basicConfig(filename='./fattree.log', level=logging.INFO)
logger = logging.getLogger(__name__)

POD_NUM = 4
DENSITY = 2
HOST_NUM = POD_NUM * POD_NUM/2 * DENSITY

FILE_NAME = "./coflow_data.json"
OUTPUT_DIR = "./task/"

class Fattree(Topo):
    logger.debug("Class Fattree")
    CoreSwitchList = []
    AggSwitchList = []
    EdgeSwitchList = []
    HostList = []

    def __init__(self, k, density):
        logger.debug("Class Fattree init")
        self.pod = k
        self.iCoreLayerSwitch = (k/2)**2
        self.iAggLayerSwitch = k*k/2
        self.iEdgeLayerSwitch = k*k/2
        self.density = density
        self.iHost = self.iEdgeLayerSwitch * density

        # Init Topo
        Topo.__init__(self)

    # create link and switch
    def createTopo(self):
        self.createCoreLayerSwitch(self.iCoreLayerSwitch)
        self.createAggLayerSwitch(self.iAggLayerSwitch)
        self.createEdgeLayerSwitch(self.iEdgeLayerSwitch)
        self.createHost(self.iHost)

    def _addSwitch(self, number, level, switch_list):
        # 1001/1002/.../2001/2002/...
        for x in xrange(1, number+1):
            PREFIX = str(level) + "00"
            if x >= int(10):
                PREFIX = str(level) + "0"
            switch_list.append(self.addSwitch('s' + PREFIX + str(x), failMode = 'standalone'))

    def createCoreLayerSwitch(self, number):
        logger.debug("Create Core Layer")
        self._addSwitch(number, 1, self.CoreSwitchList)

    def createAggLayerSwitch(self, number):
        logger.debug("Create Agg Layer")
        self._addSwitch(number, 2, self.AggSwitchList)

    def createEdgeLayerSwitch(self, number):
        logger.debug("Create Edge Layer")
        self._addSwitch(number, 3, self.EdgeSwitchList)

    def createHost(self, number):
        logger.debug("Create Host")
        for x in xrange(1, number+1):
            PREFIX = "h00"
            if x >= int(10):
                PREFIX = "h0"
            elif x >= int(100):
                PREFIX = "h"
            self.HostList.append(self.addHost(PREFIX + str(x)))

    # add link
    def createLink(self, bw_c2a=0.2, bw_a2e=0.1, bw_h2a=0.5):
        logger.debug("Add link Core to Agg.")
        end = self.pod/2
        for x in xrange(0, self.iAggLayerSwitch, end):
            for i in xrange(0, end):
                for j in xrange(0, end):
                    self.addLink(self.CoreSwitchList[i*end+j], self.AggSwitchList[x+i], bw=bw_c2a)

        logger.debug("Add link Agg to Edge.")
        for x in xrange(0, self.iAggLayerSwitch, end):
            for i in xrange(0, end):
                for j in xrange(0, end):
                    self.addLink(self.AggSwitchList[x+i], self.EdgeSwitchList[x+j], bw=bw_a2e)

        logger.debug("Add link Edge to Host.")
        for x in xrange(0, self.iEdgeLayerSwitch):
            for i in xrange(0, self.density):
                self.addLink(self.EdgeSwitchList[x], self.HostList[self.density * x + i], bw=bw_h2a)

    def set_ovs_protocol_13(self,):
        self._set_ovs_protocol_13(self.CoreSwitchList)
        self._set_ovs_protocol_13(self.AggSwitchList)
        self._set_ovs_protocol_13(self.EdgeSwitchList)

    def _set_ovs_protocol_13(self, sw_list):
        for sw in sw_list:
            cmd = "sudo ovs-vsctl set bridge %s protocols=OpenFlow13" % sw
            os.system(cmd)

def iperfTest(net, topo):
    logger.debug("Start iperfTEST")
    h1000, h1015, h1016 = net.get(topo.HostList[0], topo.HostList[14], topo.HostList[15])

    #iperf Server
    h1000.popen('iperf -s -u -i 1 > iperf_server_differentPod_result', shell=True)

    #iperf Server
    h1015.popen('iperf -s -u -i 1 > iperf_server_samePod_result', shell=True)

    #iperf Client
    h1016.cmdPrint('iperf -c ' + h1000.IP() + ' -u -t 10 -i 1 -b 100m')
    h1016.cmdPrint('iperf -c ' + h1015.IP() + ' -u -t 10 -i 1 -b 100m')
    
def pingTest(net):
    logger.debug("Start Test all network")
    return net.pingAll()


def readData():
    with open(FILE_NAME, "r") as f:
        load_data = json.load(f)
        return load_data

def getSrcHostId(mapper_id):
    return mapper_id % (HOST_NUM/2) + 1

def getDstHostId(reducer_id):
    return (reducer_id % (HOST_NUM/2)) + 1 + (HOST_NUM/2)

def getHostName(hostid):
    PREFIX = "h00"
    if hostid >= int(10):
        PREFIX = "h0"
    elif hostid >= int(100):
        PREFIX = "h"
    return PREFIX + str(hostid)

if __name__ == '__main__':
    setLogLevel('info')
    if os.getuid() != 0:
        logger.debug("You are NOT root")
    elif os.getuid() == 0:
        logging.debug("LV1 Create Fattree")
        topo = Fattree(POD_NUM, DENSITY)
        topo.createTopo()
        topo.createLink(bw_c2a=1000, bw_a2e=500, bw_h2a=250) # Mbps

        logging.debug("LV1 Start Mininet")
        CONTROLLER_IP = "127.0.0.1"
        CONTROLLER_PORT = 6653
        net = Mininet(topo=topo, link=TCLink, controller=None, autoSetMacs=True, autoStaticArp=False)
        net.addController("controller", controller=RemoteController,ip=CONTROLLER_IP, port=CONTROLLER_PORT)
        # net.addNAT().configDefault() # add NAT to connect to real network
        net.start()

        # Set OVS's protocol as OF13
        topo.set_ovs_protocol_13() 
        logger.debug("LV1 dumpNode")
        # print all connection relationship
        dumpNodeConnections(net.hosts) 
        # check all host connect
        drop = 100
        while 1 :
            if drop == 0 :
                break
            drop = pingTest(net)
        # use tcpdump to record packet in background
        for i in range(HOST_NUM/2, HOST_NUM):
            print "start to record trace in ", Fattree.HostList[i]
            tmp = net.get(Fattree.HostList[i])
            tmp_cmd = "tcpdump tcp and tcp[tcpflags] = tcp-syn and dst host " + tmp.IP() + " -s 100 -w ./result/origin/" + Fattree.HostList[i] + ".pcap &"
            print tmp_cmd
            tmp.cmd(tmp_cmd)

        # create host task and src_list
        src_list = []
        coflow_data = readData()
        output_data = []
        output_data_ = []
        for i in range(HOST_NUM/2):
            output_data.append([])
            output_data_.append([])
        # for i in range(len(coflow_data)):
        for i in range(10):
            flow_num = coflow_data[i]["Mapper num"] * coflow_data[i]["Reducer num"]
            for j in range(coflow_data[i]["Mapper num"]):
                hostid = getSrcHostId(coflow_data[i]["Mapper list"][j])
                this_coflow_data = {}
                this_coflow_data["Coflow ID"] = coflow_data[i]["Coflow ID"]
                this_coflow_data["Arrival time"] = coflow_data[i]["Arrival time"]
                this_coflow_data["Flow Number"] = flow_num
                this_coflow_data["Mapper ID"] = coflow_data[i]["Mapper list"][j]
                this_coflow_data["Reducer ID"] = []
                this_coflow_data["Dst list"] = []
                this_coflow_data["Dst data"] = []
                for k in range(coflow_data[i]["Reducer num"]):
                    flow_size = int(coflow_data[i]["Reducer data"][k]*1024 / coflow_data[i]["Mapper num"]) # data size per flow (transfer MB to KB)
                    dst = net.get(getHostName(getDstHostId(coflow_data[i]["Reducer list"][k])))
                    dst_ip = dst.IP()
                    this_coflow_data["Dst list"].append(dst_ip)
                    this_coflow_data["Dst data"].append(flow_size)
                    this_coflow_data["Reducer ID"].append(coflow_data[i]["Reducer list"][k])
                output_data[hostid-1].append(this_coflow_data)

        for i in range(len(output_data)):
            flow_count = 0
            while flow_count < len(output_data[i]):
                this_coflow_data = {}
                this_coflow_data["Coflow ID"] = output_data[i][flow_count]["Coflow ID"]
                this_coflow_data["Arrival time"] = output_data[i][flow_count]["Arrival time"]
                this_coflow_data["Flow Number"] = output_data[i][flow_count]["Flow Number"]
                this_coflow_data["Reducer ID"] = output_data[i][flow_count]["Reducer ID"]
                this_coflow_data["Dst list"] = output_data[i][flow_count]["Dst list"]
                this_coflow_data["Dst data"] = output_data[i][flow_count]["Dst data"]
                this_coflow_data["Mapper ID"] = []
                this_coflow_data["Mapper ID"].append(output_data[i][flow_count]["Mapper ID"])
                # find flows in same coflow 
                while flow_count+1 < len(output_data[i]):
                    if output_data[i][flow_count]["Coflow ID"] == output_data[i][flow_count+1]["Coflow ID"]:
                        flow_count += 1
                        this_coflow_data["Mapper ID"].append(output_data[i][flow_count]["Mapper ID"]) # append source ID(1-D array)
                    else:
                        break
                flow_count += 1
                output_data_[i].append(this_coflow_data)

        for i in range(len(output_data)):
            hostname = getHostName(getSrcHostId(i+1)) 
            with open(OUTPUT_DIR + hostname + "_task.json", "w") as f:
                json.dump(output_data_[i], f)
        print "complete output task"

        # time.sleep(5) # wait for executing tcpdump

        # for src_host in Fattree.HostList[0: HOST_NUM/2]:
        #     src = net.get(src_host)
        #     src_ip = src.IP()
        #     # python TCP_sender.py hostname hostip coflowId &
        #     tmp_cmd = "python TCP_sender.py " + str(src_host) + " " + str(src_ip) + " &"
        #     result = src.cmd(tmp_cmd)
        #     print tmp_cmd 

        # # check jobs still run
        # flag = 1
        # check = 0
        # while flag :
        #     time.sleep(20) # after 20 seconds, check again
        #     for host in Fattree.HostList[0: HOST_NUM/2]:
        #         tmp = net.get(host)
        #         now_jobs = tmp.cmd("jobs")
        #         if len(now_jobs) != 0 : # still send data
        #             if check == 0:
        #                 check += 1
        #             elif check == 1:
        #                 check -= 1
        #             print "jobs in ", host, " still run ", check
        #             flag = 1
        #             break
        #         else: # complete
        #             flag = 0
        #             continue

        # # close tcpdump
        # for i in range(HOST_NUM/2, HOST_NUM):
        #     print "close to trace flow in ", Fattree.HostList[i]
        #     tmp = net.get(Fattree.HostList[i])
        #     tmp_cmd = "ps aux | grep tcpdump | awk '{print $2}' | xargs sudo kill -9"

        CLI(net)
        net.stop()