from threading import Lock
import sys
from itertools import islice


MACHINE = 'fa17-cs425-g25-{server_id}.cs.illinois.edu'
JOINMSG = 'join-{server_name}'
STR_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
all_members = ['01', '02', '04', '05', '06', '07', '08', '09', '10']
FINISH = '02'

K = 3
SERVERNAME = sys.argv[1]
num_inactive = 0
members = []
pre = ''
suc = ''
myHost = ''
memberlist = {}
failed = {}
replicas = {}
file_times = {}

replica_time = None
SSHPort = 2015
REPLICA_PORT = 2016
CREATE_REP = 2017
UPDATE_Port = 2018
masterPort = 2001
serverPort = 2002
GET_PORT = 2019
JOINPORT = 2003
GOSSPORT = 2004
GOSSREP_PORT = 2020
DELETE_PORT = 2021
pingPort = 2022

replica_time_mutex = Lock()
members_mutex = Lock()
pre_mutex = Lock()
suc_mutex = Lock()
memberlist_mutex = Lock()
failed_mutex = Lock()
replicas_mutex = Lock()
workers_mutex = Lock()

num_inactive_mutex = Lock()
worker_id_mutex = Lock()
partition_nums_mutex = Lock()
partition_nodes_mutex = Lock()
nodes_mutex = Lock()
peer_messages_mutex = Lock()
curr_messages_mutex = Lock()
next_messages_mutex = Lock()
finish_mutex = Lock()
fail_mutex = Lock()
leave_mutex = Lock()
yes_mutex = Lock()

class worker:
    def __init__(self, worker_id, partition_nums, partition_nodes, divs):
        self.worker_id = worker_id
        self.partition_nums = partition_nums
        self.partition_nodes = partition_nodes
        self.divs = divs
        self.nodes = {}
        self.peer_messages = {}
        self.curr_messages = {}
        self.next_messages = {}

    def load_input(self, class_name):
        print 'loading'
        with open('com-amazon.ungraph.txt') as f:
            start = self.divs[0]
            end = self.divs[1]
            print self.divs
            lines = list(islice(f, start, end))
            barrier = Barrier((len(lines) / 10) + 1)
            n = (len(lines) / 10)
            for i in range(0, len(lines), n):
                thread = Thread(target=self.load_chunks, args=(lines[i:i + n],))
                thread.daemon = True
                thread.start()
        barrier.wait()

            for (v_id, v) in self.nodes:
                self.curr_messages[v_id] = []

    def load_input(self, class_name):

        with open('com-amazon.ungraph.txt') as f:
            start = self.divs[self.worker_id][0]
            end = self.divs[self.worker_id][-1]
            lines = list(islice(f, start, end))

            for line in lines:
                line = line.strip('\n')
                node_pair = line.split()
                v1 = node_pair[0]
                v2 = node_pair[1]

                self.load_pair(v1, v2, class_name)
                self.load_pair(v2, v1, class_name)

        for (v_id, v) in self.nodes:
            self.curr_messages[v_id] = []

    def load_pair(self, v1, v2, class_name):

        self.peer_messages = {}

        v = class_name(0, v1, [v2])
        if self.own_node(v1) and v1 not in self.nodes:
            self.nodes[v1] = v
        elif self.own_node(v1) and v1 in self.nodes:
            self.nodes[v1].outgoing_edges.append(v2)
        else:
            owner = self.find_node_owner(v1)
            message = (v, "load")
            if owner not in self.peer_messages:
                self.peer_messages[owner] = [message]
            else:
                self.peer_messages[owner].append(message)

    def reload(self, all_nodes):

        nodes_mutex.acquire()
        self.nodes = []
        nodes_mutex.release()

        partition_nums_mutex.acquire()
        worker_id_mutex.acquire()
        for num in self.partition_nums[self.worker_id]:
            partition_nodes_mutex.acquire()
            for node in self.partition_nodes[num]:
                nodes_mutex.acquire()
                v = all_nodes[node]
                self.nodes[node] = v
                nodes_mutex.release()
            self.partition_nodes.release()
        worker_id_mutex.release()
        partition_nums_mutex.release()

    def own_node(self, v):
        partition_nums_mutex.acquire()
        worker_id_mutex.acquire()
        for num in self.partition_nums[self.worker_id]:
            partition_nodes_mutex.acquire()
            if v in self.partition_nodes[num]:
                partition_nodes_mutex.release()
                worker_id_mutex.release()
                partition_nums_mutex.release()
                return True
            partition_nodes_mutex.release()
        worker_id_mutex.release()
        partition_nums_mutex.release()
        return False

    def find_node_owner(self, v):

        partition_nums_mutex.acquire()
        for (worker, num) in self.partition_nums.items():
            partition_nodes_mutex.acquire()
            for arr in self.partition_nodes[num]:
                if v in arr:
                    partition_nodes_mutex.release()
                    partition_nums_mutex.release()
                    return worker
            partition_nodes_mutex.release()
        partition_nums_mutex.release()
