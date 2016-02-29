# -*- encoding: utf-8 -*-


from dht import DHTServer
from bt_metadata import download_metadata

from eventlet import queue, GreenPool, sleep
from Queue import Queue
from threading import Thread

class Master(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.metadata_queue = queue.LightQueue()
        self.infohash_queue = Queue()
        self._pool = GreenPool()

    def run(self):
        downloaded = set()
        while True:
            infohash, address = self.infohash_queue.get()
            if infohash in downloaded:
                continue
            self._pool.spawn_n(download_metadata, address, infohash, self.metadata_queue)
            sleep(1)
            while not self.metadata_queue.empty():
                infohash, address, metadata = self.metadata_queue.get()
                if metadata and len(metadata) > 0:
                    downloaded.add(infohash)
                    print 'Total metadata downloaded:', len(downloaded)
                    with open('../torrent/'+infohash.encode('hex'), 'w') as f:
                        f.write(metadata)

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
    dht.auto_send_find_node()
