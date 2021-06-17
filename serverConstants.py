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


QUEUE = collections.deque()
DICT = dict()

LOCK = threading.Lock()
MY_QUORUM = {}

#Global variables for paxos
LEADER = None
LEADER_ELECTION = False
PREPARE_NACKED = False
SEQ_NUM = 0
MY_PID = 0
DEPTH = 0
NUM_PROMISES = 0
ENOUGH_PROMISES = False
PROMISED_VALS =[]
ACCEPTED_NUM = 0
ACCEPTED_VAL = None
ACCEPTED_PID = 0
NUM_ACCEPTED = 0
ENOUGH_ACCEPTED = False

ACCEPT_COUNT = 0

BLOCKCHAIN = []

SERVERPORTS = {}
FAILED_LINKS = {}

SERVERPORTS[1] = 5001
SERVERPORTS[2] = 5002
SERVERPORTS[3] = 5003
SERVERPORTS[4] = 5004
SERVERPORTS[5] = 5005

DELAY = 2