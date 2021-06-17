import collections

CURRENT_LEADER = None
WAITING = False
OP_DEQ = collections.deque()
TIMEOUT = 25.0