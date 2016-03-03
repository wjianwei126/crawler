# -*- encoding: utf-8 -*-


from dhtspider import DHTSpider
from btclient import btclient

from bencode import bdecode
import sys
import logging
import time
from threading import Thread

logger = logging.getLogger(__name__)
dhtspider_logger = logging.getLogger('dhtspider')
btclient_logger = logging.getLogger('btclient')
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(levelname)s %(created)f] (%(message)s) {%(name)s %(lineno)d}')
handler.setFormatter(formatter)

logger.addHandler(handler)
dhtspider_logger.addHandler(handler)
btclient_logger.addHandler(handler)

# logger.setLevel(logging.DEBUG)
# dhtspider_logger.setLevel(logging.DEBUG)
# btclient_logger.setLevel(logging.DEBUG)

class Master(Thread):

    def __init__(self, metadata_queue):
        Thread.__init__(self)
        self.metadata_queue = metadata_queue
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            infohash, address, d_metadata, spend_time = self.metadata_queue.get()
            if not infohash:
                continue

            name = d_metadata.get('name', '') or d_metadata.get('name.utf-8', '')
            try:
                name = name.decode('utf8')
            except Exception:     # decode error
                pass
            print '\n'
            print 'name: %s' % name
            print 'length:', d_metadata.get('length', '')
            print 'files:', d_metadata.get('files', [])
            print 'from: %s:%d' % address
            print 'infohash: %s' % infohash.encode('hex')
            print 'download spend time: %s' % spend_time
            print 'last seen: %s' % time.asctime()

    def stop(self):
        self.running = False
        self.metadata_queue.put((None, None, None, None))



# using example
if __name__ == "__main__":

    # print namespace.host, namespace.port
    # max_node_qsize bigger, bandwith bigger, speed higher

    start_time = time.time()

    dht = DHTSpider('0.0.0.0', 6881, max_node_qsize=4000)
    bt = btclient(dht.infohash_queue)
    master = Master(bt.metadata_queue)
    dht.start()
    bt.start()
    master.start()
    try:
        while dht.isAlive() and bt.isAlive() and master.isAlive():
                time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        print 'Stopping crawler.....'
        print 'dht is alive: %s, bt is alive: %s, master is alive: %s' % \
                        (dht.is_alive(), bt.is_alive(), master.is_alive())
        dht.stop()
        bt.stop()
        master.stop()
        dht.join()
        bt.join()
        master.join()
        print 'Exited, Total run time: %f(S)' % (time.time() - start_time)
        exit(0)


