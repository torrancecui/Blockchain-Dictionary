import json
import hashlib
import collections
import pickle
import random
import string
import hashlib
import socket
import threading
import time
import sys
import os

from blockchain import *

def EXITprogram():
    global OTHER_SERVERS
    global CLIENTS
    sys.stdout.flush()

    print("---CLOSING DOWN SERVER---")

    for sid, sock in OTHER_SERVERS.items():
        sock.close()

    for cid, sock in CLIENTS.items():
        sock.close()
    os._exit(0)

def HANDLEinput(listeningSocket, server_pids):
    global MY_PID
    global OTHER_SERVERS

    while True:
        try:
            cmd = input()
            cmd = cmd.split()
            if cmd[0] == 'exit':
                EXITprogram()
            #connect to all servers
            elif cmd[0] == 'connect':
                quorum = server_pids[str(MY_PID)]["quorum"]
                for pid, info in server_pids.items():
                    if MY_PID != int(pid):
                        port = info["port"]
                        (threading.Thread(target=server_connect, args=(int(pid),port,quorum))).start()
            elif cmd[0] == "print":
                if cmd[1] == "blockchain":
                    threading.Thread(target=PRINTblockchain).start()
                elif cmd[1] == "kv":
                    threading.Thread(target=PRINTkvs).start()
                elif cmd[1] == "queue":
                    threading.Thread(target=PRINTqueue).start()
            elif cmd[0] == "failLink":
                src = int(cmd[1])
                dest = int(cmd[2])
                threading.Thread(target=fail_link, args=(src, dest)).start()
            elif cmd[0] == "fixLink":
                src = int(cmd[1])
                dest = int(cmd[2])
                threading.Thread(target=fix_link, args=(src, dest)).start()
        except EOFError:
            pass

def PRINTblockchain():
    global BLOCKCHAIN
    for block_num in range(len(BLOCKCHAIN)):
        print(BLOCKCHAIN[block_num])

def PRINTkvs():
    global DICT
    for key, value in DICT.items():
        print(f"{key}: {value}")

def PRINTqueue():
    global QUEUE
    print(QUEUE)

def server_connect(server_pid, port, quorum):
    global OTHER_SERVERS
    global MY_QUORUM
    server_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_to_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_to_server.connect((socket.gethostname(),port))
    server_to_server.sendall(b"server")
    OTHER_SERVERS[server_pid] = server_to_server
    if (server_pid in quorum):
        MY_QUORUM[server_pid] = server_to_server
    print(f"Connected to SERVER {server_pid}")

def fail_link(src, dest):
    global OTHER_SERVERS
    global MY_QUORUM
    global MY_PID
    global FAILED_LINKS

    conn = OTHER_SERVERS[dest]
    message = {}
    message["type"] = "faillink"
    message["sender"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(DELAY)
    conn.sendall(encoded_message)

    FAILED_LINKS[dest] = conn
    del OTHER_SERVERS[dest]

    print(f"Link failed with SERVER {dest}")

    if MY_QUORUM.get(dest, -1) != -1:
        del MY_QUORUM[dest]
        for pid, conn in OTHER_SERVERS.items():
            if pid != dest and MY_QUORUM.get(pid, -1) == -1:
                print(f"Added SERVER {pid} to QUORUM")
                MY_QUORUM[pid] = conn
                break

def fix_link(src, dest):
    global OTHER_SERVERS
    global FAILED_LINKS

    conn = FAILED_LINKS[dest]
    OTHER_SERVERS[dest] = conn
    del FAILED_LINKS[dest]
    message = {}
    message["type"] = "fixlink"
    message["sender"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(DELAY)
    conn.sendall(encoded_message)

    print(f"Link fixed with SERVER {dest}")

#MULTIPAXOS------------------

def leader_accept():
    global MY_PID
    global LEADER
    global QUEUE
    global NUM_ACCEPTED
    global ENOUGH_ACCEPTED

    while True:
        while(LEADER != MY_PID or len(QUEUE) == 0):
            pass
        increment_seq_num()
        ballot = get_ballot_num()
        message = {}
        message["type"] = "accept"
        message["ballot"] = ballot
        message["sender_pid"] = MY_PID
        #make block by finding nonce
        block = newBlock(QUEUE[0])
        message["value"] = block
        encoded_message = pickle.dumps(message)
        time.sleep(DELAY)
        for s_pid, conn in OTHER_SERVERS.items():
            print(f"Sending ACCEPT req to SERVER {s_pid}")
            conn.sendall(encoded_message)

        while(ENOUGH_ACCEPTED == False):
            pass
        increment_seq_num()
        print("MAJORITY SERVERS ACCEPTED")
        decide(block)
        NUM_ACCEPTED = 0
        ENOUGH_ACCEPTED = False

def decide(block):
    global BLOCKCHAIN
    global DICT
    global QUEUE
    global DEPTH
    #Add block to blockchain
    increment_seq_num()
    BLOCKCHAIN.append(block)
    DEPTH += 1
    #Update key-value store
    key = block["operation"]["key"]
    if block["operation"]["op"] == "put":
        value = block["operation"]["value"]
        DICT[key] = value
    client_id = QUEUE[0]["client_id"]
    #Pop operation from queue
    QUEUE.popleft()

    client_message = {}
    client_message["type"] = "server_response"
    if block["operation"]["op"] == "put":
        client_message["response"] = "---ACKNOWLEDGED---"
    else:
        value = DICT[key]
        client_message["response"] = "VALUE {} : KEY {}".format(key,value)
    print(f"Responding to CLIENT {client_id}")
    conn = CLIENTS[str(client_id)]
    encoded_message = pickle.dumps(client_message)
    time.sleep(DELAY)
    conn.sendall(encoded_message)

    server_message = {}
    server_message["type"] = "decide"
    server_message["value"] = block

    encoded_message = pickle.dumps(server_message)
    for s_pid, conn in OTHER_SERVERS.items():
        print(f"Sending decision to SERVER {s_pid}")
        conn.sendall(encoded_message)
    #Notify servers and clients about the block

def leader_request(client_id):
    global MY_PID
    global OTHER_SERVERS
    global NUM_PROMISES
    global PREPARE_NACKED
    global LEADER_ELECTION

    if LEADER_ELECTION == False:
        LEADER_ELECTION = True
        increment_seq_num()
        ballot = get_ballot_num()
        message = {}
        message["type"] = "prepare"
        message["ballot"] = ballot
        message["sender_pid"] = MY_PID
        message["client_id"] = client_id
        encoded_message = pickle.dumps(message)
        time.sleep(DELAY)
        for s_pid, conn in MY_QUORUM.items():
            print(f"Sending BALLOT {ballot} to SERVER {s_pid}")
            conn.sendall(encoded_message)
        while(ENOUGH_PROMISES != True):
            if(PREPARE_NACKED == True):
                #retry prepare: set promises to zero, set PREPARE_NACKED to False, sleep 5 seconds, call leader_request
                # print("I got PREPARE_NACKED on prepare request")
                LEADER_ELECTION = False
                threading.Thread(target = retry_prepare, args=(client_id,)).start()
                break
            pass
        increment_seq_num()
        if(PREPARE_NACKED == False):
            leader_broadcast()

def leader_broadcast():
    global MY_PID 
    global CLIENTS
    global LEADER
    global LEADER_ELECTION

    message = {}
    message["type"] = "leader_broadcast"
    message["leader"] = MY_PID
    encoded_message = pickle.dumps(message)
    print(f"NEW LEADER: SERVER {MY_PID}")
    time.sleep(DELAY)
    for s_pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)
    for c_pid, conn in CLIENTS.items():
        conn.sendall(encoded_message)
    LEADER = MY_PID
    NUM_PROMISES = 0
    ENOUGH_PROMISES = False
    LEADER_ELECTION = False

def retry_prepare(client_id):
    global NUM_PROMISES
    global PREPARE_NACKED

    NUM_PROMISES = 0
    time.sleep(DELAY)
    PREPARE_NACKED = False
    increment_seq_num()
    leader_request(client_id)

def increment_seq_num():
    global SEQ_NUM
    LOCK.acquire()
    SEQ_NUM += 1
    LOCK.release()

def get_ballot_num():
    global SEQ_NUM
    global MY_PID
    global DEPTH
    return (SEQ_NUM, MY_PID, DEPTH)

#HANDLERS------------------

def HANDLEserver(stream):
    global NUM_PROMISES
    global LEADER
    global MY_PID
    global ENOUGH_PROMISES
    global PREPARE_NACKED
    global NUM_ACCEPTED
    global ENOUGH_ACCEPTED
    global BLOCKCHAIN
    global DICT
    global DEPTH

    while(True):
        data = stream.recv(4096)
        if not data:
            break
        message = pickle.loads(data)
        #Handle prepare message
        if message["type"] == "prepare":
            sender = message["sender_pid"]
            print(f"Received PREPARE message from SERVER {sender}")
            threading.Thread(target=HANDLEprepare, args = (message,)).start()
        #Handle promise message
        elif message["type"] == "promise":
            LOCK.acquire()
            NUM_PROMISES += 1
            PROMISED_VALS.append(message["promise_values"])
            if message["promise_values"]["depth"] < DEPTH:
                serv = message["sender"]
                print("Updating server {sender}")
                reply = {}
                reply["type"] = "update"
                reply["blockchain"] = BLOCKCHAIN
                reply["DICT"] = DICT
                conn = OTHER_SERVERS[message["sender"]]
                encoded_reply = pickle.dumps(reply)
                conn.sendall(encoded_reply)
            if NUM_PROMISES >=2:
                #Check to see if accepted vals sent are null
                highest_accepted_num = 0
                new_val = None
                for prom_val in PROMISED_VALS:
                    if prom_val["accepted_val"]:
                        if prom_val["accepted_num"] > highest_accepted_num:
                            new_val = prom_val["accepted_val"]["operation"]
                            new_val["client_id"] = prom_val["client_id"]
                if new_val:
                    QUEUE.appendleft(new_val)
                ENOUGH_PROMISES = True
            LOCK.release()
        elif message["type"] == "leader_broadcast":
            LEADER = message["leader"]
            print(f"Assigned leader to {LEADER}")
        elif message["type"] == "prepare_nack":
            if message["behind"] == True:
                print("I was behind, updating now.")
                BLOCKCHAIN = message["blockchain"]
                DICT = message["DICT"]
                DEPTH = len(BLOCKCHAIN)
            PREPARE_NACKED = True
        elif message["type"] == "accept":
            sender = message["sender_pid"]
            print(f"Received accept from server {sender}")
            threading.Thread(target=HANDLEaccept, args = (message,)).start()
        elif message["type"] == "accepted":
            LOCK.acquire()
            NUM_ACCEPTED += 1
            if NUM_ACCEPTED >= 2:
                ENOUGH_ACCEPTED = True
            LOCK.release()
        elif message["type"] == "decide":
            print(f"Received decision from leader")
            DEPTH += 1
            block = message["value"]
            BLOCKCHAIN.append(block)
            key = block["operation"]["key"]
            value = block["operation"]["value"] 
            DICT[key] = value
            increment_seq_num()
        elif message["type"] == "update":
            print("updating values and blockchain")
            BLOCKCHAIN = message["blockchain"]
            DICT = message["DICT"]
            DEPTH =len(BLOCKCHAIN)

def HANDLEprepare(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global BLOCKCHAIN
    global DICT
    global MY_PID
    global SEQ_NUM

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    proposer_ballot = message["ballot"]
    client_id = message["client_id"]
    print(f"Proposer BALLOT: {proposer_ballot}")
    #compare depth, seq_num, andpid of proposer to currently held values
    if(proposer_ballot[2] < DEPTH or proposer_ballot[0] < SEQ_NUM  or (proposer_ballot[0] == SEQ_NUM and proposer_ballot[1] < MY_PID)):
        my_bal = get_ballot_num()
        print(f"Proposal no good, smaller than {my_bal}")
        reply["type"] = "prepare_nack"
        reply["ballot"] = proposer_ballot
        reply["error"] = "prepare rejected"
        if proposer_ballot[2] < DEPTH:
            reply["behind"] = True
            reply["blockchain"] = BLOCKCHAIN
            reply["DICT"] = DICT
        else:
            reply["behind"] = False
        reply_encoded = pickle.dumps(reply)
        time.sleep(DELAY)
        conn.sendall(reply_encoded)
    #All clear to send promise
    else:
        SEQ_NUM = proposer_ballot[0]
        my_bal = get_ballot_num()         
        reply["type"] = "promise"
        reply["sender"] = MY_PID
        reply["promise_values"] = {"bal": my_bal, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH, "client_id": client_id}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending promise to LEADER")
        time.sleep(DELAY)
        increment_seq_num()
        conn.sendall(reply_encoded)
    increment_seq_num()

def HANDLEaccept(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global SEQ_NUM
    global MY_PID

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    sender_ballot = message["ballot"]
    my_bal = get_ballot_num()
    print(f"--Sender ballot: {sender_ballot}--\n--Current ballot: {my_bal}--")
    if(sender_ballot[2] < DEPTH or sender_ballot[0] < SEQ_NUM  or (sender_ballot[0] == SEQ_NUM and sender_ballot[1] < MY_PID)):
        print(f"Accept no good")
        reply["type"] = "accept_nack"
        reply["ballot"] = sender_ballot
        reply["error"] = "accept rejected"
        reply_encoded = pickle.dumps(reply)
        time.sleep(DELAY)
        conn.sendall(reply_encoded)
    else:
        SEQ_NUM = sender_ballot[0] 
        my_bal = get_ballot_num()
        ACCEPTED_NUM = my_bal  
        ACCEPTED_VAL = message["value"]      
        reply["type"] = "accepted"
        reply["accepted_vals"] = {"bal": my_bal, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending accepted to the leader")
        time.sleep(DELAY)
        increment_seq_num()
        conn.sendall(reply_encoded)
    increment_seq_num()

def HANDLEclient(stream):
    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = pickle.loads(data)
        if message["type"] == "leader_request":
            client_id = message["client_id"]
            threading.Thread(target = leader_request, args = (client_id,)).start()
        elif message["type"] == "put" or message["type"] == "get":
            client_id = message["client_id"]
            mtype = message["type"]
            print(f"Received {mtype} requ from CLIENT {client_id}")
            op = {}
            LOCK.acquire()
            QUEUE.append({"op": message["type"], "key": message["id"], "value":message["value"], "client_id": message["client_id"]})
            LOCK.release()

if __name__ == "__main__":

    MY_PID = int(sys.argv[1])
    PORT = SERVERPORTS[MY_PID]

    listeningSocket = socket.socket()
    listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listeningSocket.bind((socket.gethostname(), int(PORT)))
    listeningSocket.listen()

    print(f'SERVER {MY_PID} live on port {PORT}.')

    OTHER_SERVERS = {}

    CLIENTS = {}

    with open('config.json') as conf:
        server_pids = json.load(conf)

    threading.Thread(target=HANDLEinput, args=(listeningSocket, server_pids)).start()

    threading.Thread(target = leader_accept).start()

    while True:
        try:
            stream, address = listeningSocket.accept()
            data = stream.recv(1024)
            if not data:
                break
            message = data.decode('utf-8')
            message = message.split()

            if message[0] == "server":
                threading.Thread(target=HANDLEserver, args=(stream,)).start()
            
            elif message[0] == "client":
                CLIENTS[message[1]] = stream
                client_id = message[1]
                print(f"Listening to CLIENT {client_id} ...")
                threading.Thread(target=HANDLEclient, args=(stream,)).start()
        except KeyboardInterrupt:
            EXITprogram()

