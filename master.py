from socket import *
import numpy as np
from threading import Thread
import os
from threading import Lock
import sys
import marshal
import datetime
import logging
import paramiko
import shutil
from stat import *
import heapq
import math

from worker import worker

pairs = []
nodes = []
partitions = {}

workers_parts = {}
workers_parts_mutex = Lock()
divs = {}
divs_mutex = Lock()
workers = {}
workers_mutex = Lock()

launch = 0
launch_mutex = Lock()
update = 0
update_mutex = Lock()
active = 0
active_mutex = Lock()
output = 0
output_mutex = Lock()
files = []
files_mutex = Lock()
fail = None
fail_mutex = Lock()
leave = False
leave_mutex = Lock()
yes_mutex = Lock()

MACHINE = 'fa17-cs425-g25-{server_id}.cs.illinois.edu'
JOINMSG = 'join-{server_name}'
all_members = ['01', '02', '04', '05', '06', '07', '08', '09', '10']
STR_DATETIME = '%Y-%m-%d %H:%M:%S.%f'

clientPort = 2000
workerPort = 2001
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

replica_time_mutex = Lock()
members_mutex = Lock()
pre_mutex = Lock()
suc_mutex = Lock()
memberlist_mutex = Lock()
failed_mutex = Lock()
replicas_mutex = Lock()

num_inactive_mutex = Lock()
worker_id_mutex = Lock()
partition_nums_mutex = Lock()
partition_nodes_mutex = Lock()
nodes_mutex = Lock()
peer_messages_mutex = Lock()
curr_messages_mutex = Lock()
next_messages_mutex = Lock()


def read_input():
    with open('com-amazom.ungraph.txt') as f:
        for line in f:
            line = line.strip('\n')
            node_pair = line.split(' ')

            v1 = node_pair[0]
            v2 = node_pair[1]
            pairs.append((v1, v2))

            if v1 not in nodes:
                nodes.append(v1)
            if v2 not in nodes:
                nodes.append(v2)


def partition_graph():
    num_nodes = len(nodes)
    num_partitions = (num_nodes / 10) + (num_nodes % 10)

    for member in memberlist:
        workers_parts[member] = []

    partition = 0
    while (partition < num_partitions):
        for (key, val) in workers_parts.items():
            partitions[partition] = []
            workers_parts[key].append(partition)
            partition += 1

        for node in nodes:
            hash_val = node % num_partitions
            partitions[hash_val].append(node)


def divide_input():
    num_div = (len(pairs) / 10)
    count = 0
    index = 0
    for pair in pairs:
        if count > num_div:
            index += 1 % len(memberlist)
            count = 0
        divs[memberlist[index].keys()].append(pair)
        count += 1


def mark_failed(worker):
    while (1):
        update_mutex.acquire()
        workers_mutex.acquire()
        global update
        if update == len(workers):
            break
        workers_mutex.release()
        update_mutex.release()
    workers_mutex.acquire()
    del workers[worker.worker_id]
    workers_mutex.release()

    workers_parts_mutex.acquire()
    del workers_parts[worker.worker_id]
    workers_parts_mutex.release()

    divs_mutex.acquire()
    del divs[worker.worker_id]
    divs_mutex.release()

    update_mutex.acquire()
    global update
    update = 0
    update_mutex.release()


def reassign(parts, nodes):
    index = 0
    for partition in parts:
        workers_mutex.acquire()
        index += 1 % len(workers)
        name = workers[index].worker_id
        workers_mutex.release()
        workers_parts_mutex.acquire()
        workers_parts[name].append(partition)
        workers_parts_mutex.release()

        workers_mutex.acquire()
        workers_parts_mutex.acquire()
        partition_nums_mutex.acquire()
        partition_nodes_mutex.acquire()
        divs_mutex.acquire()
        for (w, val) in workers.items():
            val.partition_nums = workers_parts[w]
            val.partition_nodes = partitions
            val.divs = divs
        divs_mutex.release()
        partition_nodes_mutex.release()
        partition_nums_mutex.release()
        workers_parts_mutex.release()
        workers_mutex.release()

    send_message(("update", nodes))


def send_message(message):
    workers_mutex.acquire()
    for (worker, val) in workers.items():
        d = socket(AF_INET, SOCK_DGRAM)
        address = MACHINE.format(worker)
        serverHost = (address, workerPort)
        try:
            if "launch" in message:
                (mess, class_name) = message
                message = (mess, class_name, val)
            data = marshal.dumps(message)
            d.sendto(data, serverHost)
        except error:
            break
        finally:
            d.close()
    workers_mutex.release()


def listen_client_requests():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, clientPort)
    sock.bind(server_addr)
    while 1:
        try:
            data, address = sock.recvfrom(4096)
            if data:
                message = marshal.load(data)

                read_input()
                partition_graph()
                divide_input()

                memberlist_mutex.acquire()
                for member in memberlist:
                    if member != '01':
                        w = worker(member, workers_parts[member], partitions, divs)
                        workers[member] = w
                memberlist_mutex.release()
                launch_workers(message)
        except error:
            break


def listen_worker_responses():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, workerPort)
    sock.bind(server_addr)
    while 1:
        try:
            data, address = sock.recvfrom(4096)
            if data:
                data = marshal.load(data)

                if "launch" in data:
                    (mess, val) = data
                    launch_mutex.acquire()
                    global launch
                    launch += 1
                    launch_mutex.release()
                    active_mutex.acquire()
                    global active
                    active += val
                    active_mutex.release()
                elif "output" in data:
                    output_mutex.acquire()
                    global output
                    output += 1
                    output_mutex.release()
                elif "update" in data:
                    update_mutex.acquire()
                    global update
                    update += 1
                    update_mutex.release()
                break
        except error:
            break


def reduce_files():
    output = []
    heapq.heapify(output)
    memberlist_mutex.acquire()
    for member in memberlist:
        filename = 'output' + str(member) + '.txt'
        get(filename)
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip('\n')
                heapq.heappush(output, line)
    memberlist_mutex.release()
    with open('results.txt', 'w') as f:
        f.write(output)


def worker_rejoins():
    partition_graph()
    workers_mutex.acquire()
    workers_parts_mutex.acquire()
    partition_nums_mutex.acquire()
    partition_nodes_mutex.acquire()
    all_nodes = {}
    for (w,val) in workers.items():
        all_nodes.update(w.nodes)
        val.partition_nums = workers_parts[w]
        val.partition_nodes = partitions
        thread = Thread(target=val.reload, args=(all_nodes,))
        thread.daemon = True
        thread.start()
    partition_nodes_mutex.release()
    partition_nums_mutex.release()
    workers_parts_mutex.release()
    workers_mutex.release()


def launch_workers(class_name):
    superstep = 10
    iteration = 0

    while iteration < superstep:

        send_message(("launch", class_name))

        while (1):
            launch_mutex.acquire()
            workers_mutex.acquire()
            if launch == len(workers):
                send_message("done")
                break
            workers_mutex.release()
            launch_mutex.release()

        fail_mutex.acquire()
        global fail
        if fail:
            mark_failed(fail)
            reassign(workers_parts[fail.worker_id], fail.nodes)
            iteration = 0
            fail = None
            fail_mutex.release()
            continue
        fail_mutex.release()

        leave_mutex.acquire()
        global leave
        if leave:
            worker_rejoins()
            leave = False
        leave_mutex.release()

        active_mutex.acquire()
        global  active
        if active == 0:
            workers_mutex.acquire()
            send_message("output")
            workers_mutex.release()

            while (1):
                output_mutex.acquire()
                workers_mutex.acquire()
                global  output
                if output == len(workers):
                    break
                workers_mutex.release()
                output_mutex.release()
            break
        active_mutex.release()

    launch_mutex.acquire()
    global launch
    launch = 0
    launch_mutex.release()
    active_mutex.acquire()
    global active
    active = 0
    active_mutex.acquire()
    reduce_files()


def get(filename):
    replicas_mutex.acquire()
    servers = replicas[filename]
    replicas_mutex.release()
    if filename not in replicas:
        print "the file is not available"
        return
    if SERVERNAME in servers:
        return
    N = 6
    index = 0
    latest_server = None
    latest_time = None
    for server in servers:
        if index >= math.floor(N / 2 + 1):
            break
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.settimeout(3)
        address = MACHINE.format(server_id=server)
        sock_send = marshal.dumps(filename)
        try:
            sock.sendto(sock_send, (address, GET_PORT))
            try:
                response, address = sock.recvfrom(4096)
                if response:
                    (time, server) = marshal.loads(response)
                    time = datetime.datetime.strptime(time, STR_DATETIME)
                    if latest_server == None or time > latest_time:
                        latest_time = time
                        latest_server = server
                    index += 1
            except Exception as e:
                continue
        except Exception as e:
            continue
        sock.close()


def send_get(filename, machine):
    ssh = createSSHClient(MACHINE.format(server_id=machine), "askrish2", "Qbmstfu1")
    sftp = ssh.open_sftp()
    sftp.put("/home/files/" + filename, "/home/files/" + filename)
    sftp.close()
    ssh.close()


def listen_get():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, GET_PORT)
    sock.bind(server_addr)
    #sock.settimeout(1)
    while 1:
        try:
            response, address = sock.recvfrom(4096)
            (filename, machine) = marshal.loads(response)
            if "send" in filename:
                arr = filename.split(" ")
                f = arr[1]
                send_get(f, arr[2])
            if filename in replicas:
                send = marshal.dumps((str(file_times[filename]), SERVERNAME))
                try:
                    sock.sendto(send, address)
                except Exception as e:
                    continue
        except Exception as e:
            break
    sock.close()


def update_pre_suc():
    global members, pre, suc
    members_mutex.acquire()
    memberlist_mutex.acquire()
    members = []
    for key, (hb_time, times, active, counter) in memberlist.iteritems():
        if active:
            members.append(key)
    len_mems = len(members)
    if len_mems >= 3:
        members = sorted(members)
        my_index = members.index(SERVERNAME)
        pre_ind = my_index - 1
        suc_ind = my_index + 1
        pre_mutex.acquire()
        suc_mutex.acquire()
        if pre_ind is -1:
            pre = members[len_mems - 1]
        else:
            pre = members[pre_ind]
        if suc_ind is (len_mems):
            suc = members[0]
        else:
            suc = members[suc_ind]
        pre_mutex.release()
        suc_mutex.release()
    memberlist_mutex.release()
    members_mutex.release()


def send_member(member, sock, memberlist_send):
    if member != SERVERNAME:
        address = MACHINE.format(server_id=member)
        sock.sendto(memberlist_send, (address, GOSSPORT))


def createSSHClient(server, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, username=user, password=password)
    return client


def put(path, file_name):
    print "put"
    time = datetime.datetime.utcnow()
    replica_time_mutex.acquire()
    global replica_time
    replica_time = time
    file_times[file_name] = time
    replica_time_mutex.release()
    replicate(path, file_name)
    shutil.copy2(path, "/home/files/" + file_name)


def update_file(path, file_name, server):
    file_path = "/home/files/" + file_name
    print path, file_name, server
    ssh = createSSHClient(MACHINE.format(server_id=server), "askrish2", "Qbmstfu1")
    sftp = ssh.open_sftp()
    sftp.put(path, file_path)
    sftp.close()
    ssh.close()

def replicate(path, file_name):
    file_path = "/home/files/" + file_name
    memberlist_mutex.acquire()
    keys = memberlist.keys()
    memberlist_mutex.release()
    index = keys.index(SERVERNAME)
    replicas_mutex.acquire()

    if file_name in replicas and SERVERNAME not in replicas[file_name]:
        replicas[file_name].append(SERVERNAME)
    else:
        replicas[file_name] = [SERVERNAME]
    replicas_mutex.release()
    i = 1
    replicas_mutex.acquire()
    while len(replicas[file_name]) < 6:
        replicas_mutex.release()
        replica = (index + i) % len(keys)
        server = keys[replica]
        ssh = createSSHClient(MACHINE.format(server_id=server), "askrish2", "Qbmstfu1")
        sftp = ssh.open_sftp()
        sftp.put(path, file_path)
        replicas_mutex.acquire()
        if server not in replicas[file_name]:
            replicas[file_name].append(server)
        replicas_mutex.release()
        sftp.close()
        ssh.close()
        i += 1
        replicas_mutex.acquire()
    servers = replicas[file_name]
    replicas_mutex.release()

    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (MACHINE.format('01'), GOSSREP_PORT)
    message = marshal.dumps((file_name, servers))
    sock.sendto(message, server_addr)


def put_update(path, file_name):
    if file_name in replicas:
        index = 0
        N = len(replicas[file_name])
        for member in replicas[file_name]:
            if member == SERVERNAME:
                continue;
            if index >= math.floor(N / 2 + 1):
                break
            update_file(path, file_name, member)
            address = MACHINE.format(server_id=member)
            put_sock = socket(AF_INET, SOCK_DGRAM)
            put_sock.settimeout(10)
            put_send = marshal.dumps((path, file_name, member))
            put_sock.sendto(put_send, (address, UPDATE_Port))
            try:
                put_response, put_address = put_sock.recvfrom(4096)
                if put_response:
                    index += 1
                else:
                    put_sock.close()
                    continue
            except Exception as e:
                print "put error " + str(e)
            finally:
                put_sock.close()
            shutil.copy2(path, "/home/files/" + file_name)


def do_put():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, UPDATE_Port)
    sock.bind(server_addr)
    while 1:
        try:
            response, address = sock.recvfrom(4096)
            if response:
                data = marshal.loads(response)
                (path, file_name, time) = data
                put(path, file_name)
                update_time = datetime.datetime.utcnow()
                file_times[file_name] = update_time
                put_sock = socket(AF_INET, SOCK_DGRAM)
                put_send = marshal.dumps("ack")
                try:
                    put_sock.sendto(put_send, address)
                except:
                    "return put error"
                put_sock.close()

        except Exception as error:
            print "Do put", error
            break
    sock.close()

# get command line input
def get_command():
    while 1:
        print 'Enter "j" to join, "l" to leave, "m" to list the membership list, and "s" to list self id'
        line = sys.stdin.readline().strip('\n')
        if line == 'j':
            join_thread = Thread(target=join)
            join_thread.daemon = True
            join_thread.start()
        elif line == 'm':
            print memberlist
        elif line == 's':
            print SERVERNAME
        elif line == 'l':
            (times, active) = memberlist[SERVERNAME]
            time = str(datetime.datetime.utcnow())
            times.append(time)
            memberlist_mutex.acquire()
            memberlist[SERVERNAME] = (times, False)
            memberlist_mutex.release()
        elif 'put' in line:
            arr = line.split(' ')
            local_path = arr[1]
            file_name = arr[2]
            if file_name in replicas.keys():
                time = datetime.datetime.utcnow()
                answer = 'y'
                if file_name in file_times and (file_times[file_name] - time).total_seconds() < 60:
                    print 'Confirm update (y/n)'
                    i = 1
                    while i < 30:
                        yes_mutex.acquire()
                        answer = sys.stdin.readline().strip('\n')
                        yes_mutex.release()
                        if answer:
                            break
                        i += 1
                if file_name in file_times and (file_times[file_name] - time).total_seconds() >= 60 or 'y' in answer:
                    print 'Put update'
                    put_up_thread = Thread(target=put_update, args=(local_path, file_name,))
                    put_up_thread.daemon = True
                    put_up_thread.start()
            else:
                print "New put"
                put_thread = Thread(target=put, args=(local_path, file_name,))
                put_thread.daemon = True
                put_thread.start()
        elif 'store' in line:
            replicas_mutex.acquire()
            for k, v in replicas.iteritems():
                if SERVERNAME in v:
                    print k
            replicas_mutex.release()
        elif 'ls' in line:
            arr = line.split(' ')
            sdfs_name = arr[1].strip('\n')
            replicas_mutex.acquire()
            print replicas[sdfs_name]
            replicas_mutex.release()
        elif 'get' in line:
            arr = line.split(' ')
            get_thread = Thread(target=get, args=(arr[1].strip('\n'),))
            get_thread.daemon = True
            get_thread.start()
        elif 'delete' in line:
            arr = line.split(' ')
            delete_thread = Thread(target=delete, args=(arr[1].strip('\n'),))
            delete_thread.daemon = True
            delete_thread.start()

def delete(sdfs_name):
    replicas_mutex.acquire()
    servers = replicas[sdfs_name]
    del replicas[sdfs_name]
    replicas_mutex.release()
    sock = socket(AF_INET, SOCK_DGRAM)
    for server in servers:
        send_msg = marshal.dumps(sdfs_name)
        sock.sendto(send_msg, (MACHINE.format(server_id=server), DELETE_PORT))

def listen_delete():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, DELETE_PORT)
    sock.bind(server_addr)
    while 1:
        data, address = sock.recvfrom(4096)
        data = marshal.loads(data)
        if data:
            replicas_mutex.acquire()
            del replicas[data]
            replicas_mutex.release()

def listen_replicate():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, CREATE_REP)
    sock.bind(server_addr)
    while 1:
        data, address = sock.recvfrom(4096)
        data = marshal.loads(data)
        if data:
            replicas_mutex.acquire()
            if len(replicas[data]) >= 6:
                replicas_mutex.release()
                continue
            replicas_mutex.release()
            replicate_thread = Thread(target=replicate, args=("/home/files/" + data, data))
            replicate_thread.daemon = True
            replicate_thread.start()
            sock.sendto("ack", address)
    sock.close()

# This method sends a message to the introducer in order to join
def join():
    directory = "/home/files"
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory)

    os.chmod(directory, S_IRUSR | S_IWUSR | S_IXUSR | S_IWGRP | S_IXGRP | S_IRGRP | S_IROTH | S_IWOTH | S_IXOTH)

    global memberlist
    join_message = JOINMSG.format(server_name=SERVERNAME)

    intro = MACHINE.format(server_id='01')
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.settimeout(1)
    server_addr = (intro, JOINPORT)
    join_message = marshal.dumps(join_message)
    sock.sendto(join_message, server_addr)
    try:
        response, address = sock.recvfrom(4096)
        if response:
            memberlist_mutex.acquire()
            memberlist = marshal.loads(response)
            time = str(datetime.datetime.utcnow())
            memberlist_mutex.release()
            update_pre_suc()
            sock.close()
            logging.info('joined: ' + str(memberlist))
            return
        else:
            sock.close()
    except timeout:
        print 'join timeout'
    except error as e:
        print 'join error', e

def listen_replica():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, REPLICA_PORT)
    sock.bind(server_addr)
    while 1:
        try:
            response, address = sock.recvfrom(4096)
            if response:
                data = marshal.loads(response)
                (file_name, servers) = data
                replicas_mutex.acquire()
                replicas[file_name] = servers
                replicas_mutex.release()
                replica_time_mutex.acquire()
                global replica_time
                replica_time = datetime.datetime.utcnow()
                replica_time_mutex.release()
            else:
                break
        except error:
            break

def listen_gossip():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = ('', GOSSPORT)
    sock.bind(server_addr)
    while 1:
        try:
            response, address = sock.recvfrom(4096)
            if response:
                data = marshal.loads(response)
                for (k, v) in data.items():
                    (times, boolean) = v
                    memberlist_mutex.acquire()
                    if k in memberlist and k != SERVERNAME:
                        (my_times, my_boolean) = memberlist[k]
                        latest = times[-1]
                        latest = datetime.datetime.strptime(latest, STR_DATETIME)
                        my_latest = times[-1]
                        my_latest = datetime.datetime.strptime(my_latest, STR_DATETIME)
                        if latest > my_latest:
                            memberlist[k] = (my_times.append(latest), boolean)
                            leave_mutex.acquire()
                            global leave
                            leave = True
                            leave_mutex.release()
                            update_pre_suc()
                    if k not in memberlist and k != SERVERNAME:
                        memberlist[k] = v
                        leave_mutex.acquire()
                        leave = True
                        leave_mutex.release()
                        update_pre_suc()
                    memberlist_mutex.release()

                memberlist_mutex.acquire()
                for (k, v) in memberlist.items():
                    if k not in data:
                        del memberlist[k]
                        fail_mutex.acquire()
                        workers_mutex.acquire()
                        global fail
                        fail = workers[k]
                        workers_mutex.release()
                        fail_mutex.release()
                        update_pre_suc()
                memberlist_mutex.release()

                thread = Thread(target=gossip)
                thread.daemon = True
                thread.start()
            else:
                break
        except error as e:
            print "error", e
            break

# select k random processes to gossip memberlist to
def gossip():
    memberlist_mutex.acquire()
    member_keys = memberlist.keys()
    memberlist_send = marshal.dumps(memberlist)
    memberlist_mutex.release()
    if len(member_keys) >= 3:
        chosen_members = np.random.choice(member_keys, K, replace=False)
        sock = socket(AF_INET, SOCK_DGRAM)
        for member in chosen_members:
            send_thread = Thread(target=send_member, args=(member, sock, memberlist_send,))
            send_thread.daemon = True
            send_thread.start()

def listen_ping_requests():
    sock = socket(AF_INET, SOCK_DGRAM)
    server_addr = (myHost, pingPort)
    sock.bind(server_addr)
    while 1:
        try:
            data, address = sock.recvfrom(4096)
            data = marshal.loads(data)
            if data:
                if "ping" in data:
                    thread = Thread(target=ping, args=(address,))
                    thread.daemon = True
                    thread.start()
            elif "ping req" in data:
                (mess, server) = data
                thread = Thread(target=ping_req, args=(server, address))
                thread.daemon = True
                thread.start()
            break
        except error:
            break

def ping(address):
    d = socket(AF_INET, SOCK_DGRAM)
    try:
        message = marshal.dumps("ack")
        d.sendto(message, address)
    except error:
        print error
    finally:
        d.close()

def ping_req(server, address):
    while 1:
        pr = socket(AF_INET, SOCK_DGRAM)
        server_addr = (MACHINE.format(server), pingPort)
        pr.settimeout(7)
        try:
            print 'ping' + server
            message = marshal.dumps("ping")
            print 'be'
            pr.sendto(message, server_addr)
            print 'af'
            data, server = pr.recvfrom(4096)
            if data:
                message = marshal.dumps("success")
                pr.sendto(message, address)
        except timeout:
            print 'timeout'
        except error:
            print('sock err')
        finally:
            pr.close()

if __name__ == "__main__":
    memberlist[SERVERNAME] = ([str(datetime.datetime.utcnow())], True)
    command_thread = Thread(target=get_command)
    command_thread.daemon = True
    command_thread.start()
    for i in range(4):
        gossip_thread = Thread(target=gossip)
        gossip_thread.daemon = True
        gossip_thread.start()
    listen_gossip_thread = Thread(target=listen_gossip)
    listen_gossip_thread.daemon = True
    listen_gossip_thread.start()
    listen_replicate_thread = Thread(target=listen_replicate)
    listen_replicate_thread.daemon = True
    listen_replicate_thread.start()
    listen_replica_thread = Thread(target=listen_replica)
    listen_replica_thread.daemon = True
    listen_replica_thread.start()
    put_thread = Thread(target=do_put)
    put_thread.daemon = True
    put_thread.start()
    listen_get_thread = Thread(target=listen_get)
    listen_get_thread.daemon = True
    listen_get_thread.start()
    listen_delete_thread = Thread(target=listen_delete)
    listen_delete_thread.daemon = True
    listen_delete_thread.start()
    listen_ping_thread = Thread(target=listen_ping_requests)
    listen_ping_thread.daemon = True
    listen_ping_thread.start()

    print("done")

    client_thread = Thread(target=listen_client_requests)
    client_thread.daemon = True
    client_thread.start()

    worker_thread = Thread(target=listen_worker_responses)
    worker_thread.daemon = True
    worker_thread.start()

