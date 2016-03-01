# -*- encoding: utf-8 -*-


from dht import DHTServer
from bt_metadata import download_metadata

from eventlet import queue, GreenPool, sleep

import json
from bencode import bdecode
from threading import Thread
from Queue import Queue

class Master(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.metadata_queue = queue.LightQueue()
        self.infohash_queue = Queue()
        self._pool = GreenPool()
        self.downloaded = set()

    def get_torrent(self):
        while True:
            infohash, address, metadata = self.metadata_queue.get()
            if metadata and len(metadata) > 0:
                try:
                    try:
                        metadata = metadata[:metadata.index('6:pieces')] + 'e'
                    except Exception:
                        pass
                    d_metadata = bdecode(metadata)
                    length = ''
                    if 'name' in d_metadata:
                        name = d_metadata['name']
                    if 'utf-8.name' in d_metadata:
                        name = d_metadata['utf-8.name']
                    if 'length' in d_metadata:
                        length = d_metadata['length']
                    print '\n'
                    print 'name:', name.decode('utf8')
                    print 'length:', d_metadata.get('length', '')
                    print 'files:' , d_metadata.get('files', [])
                    print 'from: %s:%d' % address
                    print 'infohash: %s' % infohash.encode('hex')
                    self.downloaded.add(infohash)
                except Exception as e:
                    print e
                    print 'metadata:', metadata

    def run(self):
        self._pool.spawn_n(self.get_torrent)
        while True:
            infohash, address = self.infohash_queue.get()
            if infohash in self.downloaded:
                continue
            self._pool.spawn_n(download_metadata, address, infohash, self.metadata_queue)
            sleep(1)

    def log(self, infohash, address=None):
        # print(infohash, address)
        self.infohash_queue.put((infohash, address))


# using example
if __name__ == "__main__":

    # print namespace.host, namespace.port
    # max_node_qsize bigger, bandwith bigger, speed higher
    pool = GreenPool()

    master = Master()
    master.start()
    dht = DHTServer(master, '0.0.0.0', 6881, max_node_qsize=4000)
    pool.spawn_n(dht.run)
    try:
        pool.waitall()
    except KeyboardInterrupt:
        exit(0)


