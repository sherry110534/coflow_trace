from mininet.net import Mininet
from mininet.node import Controller, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import Link, Intf, TCLink
from mininet.topo import Topo
from mininet.util import dumpNodeConnections
import logging
import os
import random
import time

COFLOW_NUM = 10
MAX_FLOW_NUM = 5

logging.basicConfig(filename='./fattree.log', level=logging.INFO)
logger = logging.getLogger(__name__)

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

    def createCoreLayerSwitch(self, NUMBER):
        logger.debug("Create Core Layer")
        self._addSwitch(NUMBER, 1, self.CoreSwitchList)

    def createAggLayerSwitch(self, NUMBER):
        logger.debug("Create Agg Layer")
        self._addSwitch(NUMBER, 2, self.AggSwitchList)

    def createEdgeLayerSwitch(self, NUMBER):
        logger.debug("Create Edge Layer")
        self._addSwitch(NUMBER, 3, self.EdgeSwitchList)

    def createHost(self, NUMBER):
        logger.debug("Create Host")
        for x in xrange(1, NUMBER+1):
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

def read_data():
    data_line = []
    with open('FB2010-1Hr-150-0.txt', 'r') as f:
        for line in f:
            data_line.append(line.replace('\n', '').split(' '))
    data_line = data_line[1:]
    return data_line

if __name__ == '__main__':
    setLogLevel('info')
    if os.getuid() != 0:
        logger.debug("You are NOT root")
    elif os.getuid() == 0:
        logging.debug("LV1 Create Fattree")
        topo = Fattree(4, 2) # pod = 4, density = 2
        topo.createTopo()
        topo.createLink(bw_c2a=0.2, bw_a2e=0.1, bw_h2a=0.05)

        logging.debug("LV1 Start Mininet")
        CONTROLLER_IP = "127.0.0.1"
        CONTROLLER_PORT = 6653
        net = Mininet(topo=topo, link=TCLink, controller=None, autoSetMacs=True, autoStaticArp=False)
        net.addController('controller', controller=RemoteController,ip=CONTROLLER_IP, port=CONTROLLER_PORT)
        net.start()

        topo.set_ovs_protocol_13() # Set OVS's protocol as OF13
        logger.debug("LV1 dumpNode")

        dumpNodeConnections(net.hosts) # print all connection relationship
        # check all host connect
        drop = 100
        while(1):
            if(drop == 0):
                break
            drop = pingTest(net)
        # tcpdump
        for i in range(len(Fattree.HostList)/2, len(Fattree.HostList)):
            print 'start to record trace in ', Fattree.HostList[i]
            tmp = net.get(Fattree.HostList[i])
            tmp_cmd = 'tcpdump -w ./result/origin/' + Fattree.HostList[i] + '.pcap &'
            tmp.cmd(tmp_cmd)

        # open wireshark
        # for i in range(len(Fattree.HostList)/2, len(Fattree.HostList)):
        #     print 'open wireshark in ', Fattree.HostList[i]
        #     tmp = net.get(Fattree.HostList[i])
        #     tmp.cmd('wireshark &')

        # get coflow data
        trace = read_data()
        # print 'wait for open wireshark'
        # time.sleep(20)
        sleep_time = 0
        # create coflow 
        print 'start to create coflow'
        for i in range(COFLOW_NUM):
            flow_num = random.randint(1, MAX_FLOW_NUM) # determine the flow number in a coflow
            dst = net.get(random.sample(Fattree.HostList[len(Fattree.HostList)/2: len(Fattree.HostList)], 1)[0])
            dst_ip = dst.IP()
            for j in range(flow_num): # random choose flow_num host to send data
                src = net.get(random.sample(Fattree.HostList[0: len(Fattree.HostList)/2], 1)[0])
                src_ip = src.IP()
                print 'from ', src_ip, ' to ', dst_ip
                print 'coflow id: ', i
                # TCP_sender.py src_ip dst_ip coflow_id arrival_time flow_num
                tmp_cmd = 'python TCP_sender.py ' + str(src_ip)  + ' ' + str(dst_ip) + ' ' + str(i)
                tmp_cmd = tmp_cmd + ' ' + str(trace[i][1]) + ' ' + str(flow_num) + ' &'
                result = src.cmd(tmp_cmd)
                sleep_time = int(trace[i][1])
                print result
        
        time.sleep(sleep_time/1000)
        print 'the last coflow is start'

        # check jobs still run
        flag = 1
        while(flag):
            for host in Fattree.HostList[0: len(Fattree.HostList)/2]:
                tmp = net.get(host)
                now_jobs = tmp.cmd('jobs')
                if len(now_jobs) != 0 :
                    print 'jobs in ', host, ' still run'
                    flag = 1
                    time.sleep(10)
                    break
                else:
                    flag = 0
                    continue


        # close tcpdump
        for i in range(len(Fattree.HostList)/2, len(Fattree.HostList)):
            print 'close to trace flow in ', Fattree.HostList[i]
            tmp = net.get(Fattree.HostList[i])
            tmp_cmd = "ps aux | grep tcpdump | awk '{print $2}' | xargs sudo kill -9"

        CLI(net)
        net.stop()