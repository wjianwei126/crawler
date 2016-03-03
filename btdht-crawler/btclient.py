# encoding: utf-8

import eventlet
from eventlet.green import socket
from eventlet import sleep, GreenPool

import math
from struct import pack
from time import time
from random import randint
from bencode import bencode, bdecode
from threading import Thread
from Queue import  Queue

import logging

logger = logging.getLogger(__name__)

BT_PROTOCOL = "BitTorrent protocol"
BT_MSG_ID = 20
EXT_HANDSHAKE_ID = 0


def random_chars(length):
    return "".join(chr(randint(0, 255)) for _ in xrange(length))


def send_packet(the_socket, msg):
    the_socket.send(msg)


def send_message(the_socket, msg):
    msg_len = pack(">I", len(msg))
    send_packet(the_socket, msg_len + msg)


def send_handshake(the_socket, infohash):
    bt_header = chr(len(BT_PROTOCOL)) + BT_PROTOCOL
    ext_bytes = "\x00\x00\x00\x00\x00\x10\x00\x00"
    peer_id = random_chars(20)
    packet = bt_header + ext_bytes + infohash + peer_id

    send_packet(the_socket, packet)


def check_handshake(packet, self_infohash):
    try:
        bt_header_len, packet = ord(packet[:1]), packet[1:]
        if bt_header_len != len(BT_PROTOCOL):
            return False
    except TypeError:
        return False

    bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
    if bt_header != BT_PROTOCOL:
        return False

    packet = packet[8:]
    infohash = packet[:20]
    if infohash != self_infohash:
        return False

    return True


def send_ext_handshake(the_socket):
    msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencode({"m":{"ut_metadata": 1}})
    send_message(the_socket, msg)


def request_metadata(the_socket, ut_metadata, piece):
    """bep_0009"""
    msg = chr(BT_MSG_ID) + chr(ut_metadata) + bencode({"msg_type": 0, "piece": piece})
    send_message(the_socket, msg)


def get_ut_metadata(data):
    ut_metadata = "_metadata"
    index = data.index(ut_metadata)+len(ut_metadata) + 1
    return int(data[index])


def get_metadata_size(data):
    metadata_size = "metadata_size"
    start = data.index(metadata_size) + len(metadata_size) + 1
    data = data[start:]
    return int(data[:data.index("e")])


def recvall(the_socket, timeout=5):
    the_socket.setblocking(0)
    total_data = []
    data = ""
    begin = time()

    while True:
        sleep(0.05)
        if total_data and time()-begin > timeout:
            break
        elif time()-begin > timeout*2:
            break
        try:
            data = the_socket.recv(1024)
            if data:
                total_data.append(data)
                begin = time()
        except Exception:
            pass
    return "".join(total_data)



class btclient(Thread):

    def __init__(self, infohash_queue):
        Thread.__init__(self)
        self.setDaemon(True)
        self.infohash_queue = infohash_queue
        self.metadata_queue = Queue()
        self.dowloaded = set()
        self.pool = GreenPool()
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            if self.infohash_queue.empty():
                sleep(3)
            else:
                infohash, address= self.infohash_queue.get()
                self.pool.spawn_n(self.download_metadata, address, infohash, self.metadata_queue)

    def stop(self):
        self.running = False

    def metadata_queue(self):
        return self.metadata_queue

    def download_metadata(self, address, infohash, metadata_queue, timeout=5):
        metadata = []
        start_time = time()
        if infohash in self.dowloaded:
            return
        try:
            the_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # the_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # the_socket.bind(('0.0.0.0', 9000))
            the_socket.settimeout(timeout)
            the_socket.connect(address)

            # handshake
            send_handshake(the_socket, infohash)
            packet = the_socket.recv(4096)

            # handshake error
            if not check_handshake(packet, infohash):
                return

            # ext handshake
            send_ext_handshake(the_socket)
            packet = the_socket.recv(4096)

            # get ut_metadata and metadata_size
            ut_metadata, metadata_size = get_ut_metadata(packet), get_metadata_size(packet)

            # request each piece of metadata
            for piece in range(int(math.ceil(metadata_size/(16.0*1024)))):
                if infohash in self.dowloaded:
                    break
                request_metadata(the_socket, ut_metadata, piece)
                packet = recvall(the_socket, timeout)   # the_socket.recv(1024*17)
                metadata.append(packet[packet.index("ee")+2:])
                if '6:pieces' in packet:
                    break

        except socket.timeout:
            logger.debug('Connect timeout to %s:%d' % address)
            # TODO: Maybe need NAT Traversa
        except socket.error as error:
            errno, err_msg = error
            if errno == 10052:
                logger.debug('Network dropped connection on reset(10052) %s:%d' % address)
            elif errno == 10061:
                logger.debug('Connection refused(10061) %s:%d' % address)
            else:
                logger.error(err_msg)
        except Exception:
            pass
        finally:
            the_socket.close()
            metadata = "".join(metadata)
            if metadata.startswith('d') and '6:pieces' in metadata:
                metadata = metadata[:metadata.index('6:pieces')] + 'e'
                try:
                    d_metadata = bdecode(metadata)
                except Exception as e:
                    logger.error(str(e) + 'metadata: ' + metadata)
                else:
                    self.dowloaded.add(infohash)
                    metadata_queue.put((infohash, address, d_metadata, time()-start_time))