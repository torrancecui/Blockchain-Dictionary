import socket
import sys
from datetime import datetime
import time
from _thread import *
import threading
import json
import os
import pickle

from clientConstants import *


def EXITprogram():

    print("---CLIENT LOGGING OFF---")

    global SERVERS
    sys.stdout.flush()
    for sid, conn in SERVERS.items():
        conn.close()
    os._exit(0)

def HANDLEinput(client_pid, server_info):
    while True:
        try:
            inp = input()
            inp = inp.split()
            if inp[0] == 'connect':
                for pid, info in server_info.items():
                    port = info["port"]
                    (threading.Thread(target=CONNECTserver, args=(int(pid),port))).start()
            elif inp[0] == 'exit':
                EXITprogram()
            elif inp[0] == "leader":
                leader_id = int(inp[1])
                threading.Thread(target = CHOOSEleader, args = (leader_id,)).start()
            elif inp[0] == "put":
                threading.Thread(target = SENDput, args = (inp,)).start()
            elif inp[0] == "get":
                threading.Thread(target = SENDget, args = (inp,)).start()

        except EOFError:
            pass

def CHOOSEleader(leader_id):
    global SERVERS
    global MY_PID
    leader_sock = SERVERS[leader_id]
    message = {}
    message["type"] = "leader_request"
    message["client_id"] = MY_PID
    encoded_message = pickle.dumps(message)
    print(f"Sending leader request to server {leader_id}")
    time.sleep(5)
    leader_sock.sendall(encoded_message)

def CONNECTserver(server_pid,port):
    global SERVERS
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    address = (socket.gethostname(), int(port))
    server_sock.connect(address)
    client_message = "client " + str(MY_PID)
    server_sock.send(client_message.encode('utf-8'))
    SERVERS[server_pid] = server_sock
    print(f"Connected to server {server_pid}")
    (threading.Thread(target=LISTENserver, args=(server_sock,))).start()

def SENDput(inp):
    global OP_DEQ
    global WAITING

    message = {}
    message["type"] = "put"
    message["id"] = inp[1]
    message["value"] = inp[2]
    message["client_id"] = MY_PID

    OP_DEQ.append(message)

    while(OP_DEQ[0] != message):
        pass
    encoded_message = pickle.dumps(message)
    print(f"Sending put operation to server {CURRENT_LEADER}")
    conn = SERVERS[CURRENT_LEADER]
    time.sleep(5)
    conn.sendall(encoded_message)
    WAITING = True
    # time = datetime.now()
    #Wait to hear back for server
    send_time = datetime.now()
    while(WAITING == True):
        if (datetime.now() - send_time).total_seconds() > TIMEOUT:
            NEWleader()
            WAITING = False
        pass
    OP_DEQ.popleft()

def SENDget(inp):
    global OP_DEQ
    global WAITING

    message = {}
    message["type"] = "get"
    message["id"] = inp[1]
    message["value"] = ""
    message["client_id"] = MY_PID

    OP_DEQ.append(message)

    while(OP_DEQ[0] != message):
        pass
    encoded_message = pickle.dumps(message)
    print(f"Sending get operation to server {CURRENT_LEADER}")
    conn = SERVERS[CURRENT_LEADER]
    time.sleep(5)
    conn.sendall(encoded_message)
    WAITING = True
    # time = datetime.now()
    #Wait to hear back for server
    send_time = datetime.now()
    while(WAITING == True):
        if (datetime.now() - send_time).total_seconds() > TIMEOUT:
            NEWleader()
            WAITING = False
        pass
    OP_DEQ.popleft()

def NEWleader():
    global CURRENT_LEADER
    print("Timeout!")
    NEWleader = (CURRENT_LEADER + 1) % 5
    CHOOSEleader(NEWleader)

def LISTENserver(sock):
    global CURRENT_LEADER
    global WAITING
    try:
        while True:
            data = (sock.recv(1024))
            message = pickle.loads(data)
            if message["type"] == "leader_broadcast":
                CURRENT_LEADER = message["leader"]
                print(f"Server {CURRENT_LEADER} elected leader")
            if message["type"] == "server_response":
                WAITING = False
                print(message["response"])
    except EOFError:
        pass
            
if __name__ == "__main__":
    MY_PID = int(sys.argv[1])

    with open('config.json') as conf:
        server_info = json.load(conf)
    SERVERS = {}

    HANDLEinput(MY_PID, server_info)

