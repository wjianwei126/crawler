# -*- encoding: utf-8 -*-


from dhtspider import DHTSpider
from btclient import btclient

from eventlet import queue, GreenPool, sleep

from bencode import bdecode
import sys
import logging

logger = logging.getLogger(__name__)
dhtspider_logger = logging.getLogger('dhtspider')
btclient_logger = logging.getLogger('btclient')
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
handler_info = logging.StreamHandler(sys.stdout)
handler_info.setLevel(logging.INFO)

formatter = logging.Formatter('[%(levelname)s %(created)f] (%(message)s)')
handler.setFormatter(formatter)
handler_info.setFormatter(formatter)

logger.addHandler(handler)
dhtspider_logger.addHandler(handler)
btclient_logger.addHandler(handler)

logger.setLevel(logging.DEBUG)
dhtspider_logger.setLevel(logging.DEBUG)
btclient_logger.setLevel(logging.DEBUG)

def get_torrent(metadata_queue):
    while True:
        infohash, address, metadata, spend_time = metadata_queue.get()
        if metadata and len(metadata) > 0:
            try:
                if '6:pieces' in metadata:
                    metadata = metadata[:metadata.index('6:pieces')] + 'e'

                d_metadata = bdecode(metadata)
                name = d_metadata.get('name', '') or d_metadata.get('utf-8.name', '')
                print '\n'
                print 'name:', name.decode('utf8')
                print 'length:', d_metadata.get('length', '')
                print 'files:' , d_metadata.get('files', [])
                print 'from: %s:%d' % address
                print 'infohash: %s' % infohash.encode('hex')
            except Exception as e:
                logger.error(str(e)+' metadata:'+metadata)




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


