from eventlet import queue
from collections import deque

class Master():

    def __init__(self):
        self.metadata_queue = queue.LightQueue()
        self.info_queue = deque()

    def log(self, infohash, address=None):
        print(infohash, address)
        self.info_queue.append((infohash, address))