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

from serverConstants import *

genesisString ="Genesis"
genesisHash = hashlib.sha256(genesisString.encode()).hexdigest()

def createNonce():
    nonce = ''.join(random.choice(string.ascii_letters) for i in range(10))
    return nonce

def newBlock(op_info):
    global DEPTH
    global genesisHash
    nonce = createNonce()

    hashable = op_info["op"] + op_info["key"] + op_info["value"] + nonce
    curr_hash = hashlib.sha256(hashable.encode()).hexdigest()

    while (curr_hash[-1] != "0" and curr_hash[-1] != "1" and curr_hash[-1] != "2"):
        nonce = createNonce()
        hashable = op_info["op"] + op_info["key"] + op_info["value"] + nonce
        curr_hash = hashlib.sha256(hashable.encode()).hexdigest()

    print(f"Found nonce: {nonce} for hash value: {curr_hash}")

    hash_ptr = None
    if len(BLOCKCHAIN) != 0:
        hashable = BLOCKCHAIN[-1]["operation"]["key"] + BLOCKCHAIN[-1]["operation"]["value"] +  BLOCKCHAIN[-1]["header"]["nonce"] + BLOCKCHAIN[-1]["header"]["hash"]
        hash_ptr = hashlib.sha256(hashable.encode()).hexdigest()
    else:
        hash_ptr = genesisHash

    block = {
    "header": {"nonce": nonce, "hash": hash_ptr}, 
    "operation": {"op": op_info["op"], "key": op_info["key"], "value": op_info["value"]}
    }

    return block