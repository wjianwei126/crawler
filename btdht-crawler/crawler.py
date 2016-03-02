# -*- encoding: utf-8 -*-


from dhtspider import DHTSpider
from btclient import btclient

from bencode import bdecode
import sys
import logging
import time

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

logger.setLevel(logging.DEBUG)
dhtspider_logger.setLevel(logging.DEBUG)
btclient_logger.setLevel(logging.DEBUG)

def get_torrent(metadata_queue):
    while True:
        infohash, address, d_metadata, spend_time = metadata_queue.get()
        name = d_metadata.get('name', '') or d_metadata.get('utf-8.name', '')
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



# using example
if __name__ == "__main__":

    # print namespace.host, namespace.port
    # max_node_qsize bigger, bandwith bigger, speed higher

    dht = DHTSpider('0.0.0.0', 6881, max_node_qsize=4000)
    bt = btclient(dht.infohash_queue)
    dht.start()
    bt.start()
    try:
        get_torrent(bt.metadata_queue)
    except KeyboardInterrupt:
        exit(0)


