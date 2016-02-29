# -*- encoding: utf-8 -*-


from dht import DHTServer
from bt_metadata import download_metadata

from eventlet import queue, GreenPool, sleep
from Queue import Queue
from threading import Thread
import json
from bencode import bdecode

class Master(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.metadata_queue = queue.LightQueue()
        self.infohash_queue = Queue()
        self._pool = GreenPool()
        self.downloaded = set()

    def get_torrent(self):
        while True:
            infohash, address, metadata = self.metadata_queue.get()
            if metadata and len(metadata) > 0:
                self.downloaded.add(infohash)
                with open('../torrent/'+infohash.encode('hex'), 'w') as f:
                    json.dump(bdecode(metadata), f)

    def run(self):
        self._pool.spawn_n(self.get_torrent)
        while True:
            infohash, address = self.infohash_queue.get()
            if infohash in self.downloaded:
                continue
            self._pool.spawn_n(download_metadata, address, infohash, self.metadata_queue)
            sleep(1)

    def log(self, infohash, address=None):
        print(infohash, address)
        self.infohash_queue.put((infohash, address))


# using example
if __name__ == "__main__":

    # print namespace.host, namespace.port
    # max_node_qsize bigger, bandwith bigger, speed higher

    master = Master()
    master.start()
    dht = DHTServer(master, '0.0.0.0', 6881, max_node_qsize=200)
    dht.start()
    dht.join()
    master.join()
