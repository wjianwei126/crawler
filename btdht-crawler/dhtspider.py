import eventlet
from eventlet.green import socket
from eventlet import sleep

from random import randint
from struct import unpack
from socket import inet_ntoa
from bencode import bencode, bdecode
from threading import Thread
from Queue import Queue

import logging


logger = logging.getLogger(__name__)

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)

TID_LENGTH = 2
TOKEN_LENGTH = 2

class KNode(object):

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTSpider(Thread):

    def __init__(self, bind_ip, bind_port, max_node_qsize):
        Thread.__init__(self)
        self.max_node_qsize = max_node_qsize
        self.nid = self.random_chars(20)
        self.nodes = eventlet.queue.LightQueue(maxsize=max_node_qsize)
        self.message_queue = eventlet.queue.LightQueue(maxsize=4000)
        self.ips = set()
        self.infohash_queue = Queue()
        self.bind_ip = bind_ip
        self.bind_port = bind_port

        self.process_request_actions = {
            "ping": self.on_ping_request,
            "find_node": self.on_find_node_request,
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))
        self.pool = eventlet.GreenPool()
        self.running = False

    def infohash_queue(self):
        return self.infohash_queue

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
        while self.running:
            try:
                node = self.nodes.get(timeout=1)
                self.send_find_node((node.ip, node.port), node.nid)
            except eventlet.queue.Empty:
                sleep(3)    # wait for new node in.
                self.re_join_dht()
            except Exception as e:
                logger.error(e)
        sleep(wait)

    def send_find_node(self, address, nid=None):
        nid = self.get_neighbor(nid, self.nid) if nid else self.nid
        tid = self.random_chars(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": self.random_chars(20)
            }
        }
        self.send_krpc(msg, address)

    def response(self):
        while self.running:
            try:
                p = self.ufd.recvfrom(65536)
            except socket.error as e:
                errno, err_msg = e
                if errno == 10052:
                    logger.debug('Network dropped connection on reset(10052) %s:%d' % address)
                else:
                    logger.error(err_msg)

            if p and len(p) == 2:
                data, address = p

                try:
                    msg = bdecode(data)
                except:
                    pass
                self.on_message(msg, address)

    def send_krpc(self, msg, address):
        self.message_queue.put((bencode(msg), address))

    def send_message_queue(self):
        while self.running:
            msg, address = self.message_queue.get()
            self.ufd.sendto(msg, address)

    def re_join_dht(self):
        if not self.nodes.empty():
            return
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def run(self):
        self.running = True
        self.pool.spawn_n(self.auto_send_find_node)
        self.pool.spawn_n(self.response)
        self.pool.spawn_n(self.send_message_queue)
        self.pool.waitall()

    def stop(self):
        self.running = False

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if "nodes" in msg['r']:
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

    def process_find_node_response(self, msg, address):
        nodes = self.decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20:
                continue
            if ip == self.bind_ip:
                continue
            if port < 1 or port > 65535:
                continue
            n = KNode(nid, ip, port)
            self.nodes.put(n)
            if logger.level == logging.DEBUG and len(self.ips) < 100000 and ip not in self.ips:
                self.ips.add(ip)
                logger.debug('ips: %d, nodes: %d' % (len(self.ips), self.nodes.qsize()))

    def on_ping_request(self, msg, address):
        msg = {
            "t": msg["t"],
            "y": "r",
            "r": {
                # peers may be found the nid is changed and reject this node.
                self.nid
            }
        }
        self.send_krpc(msg, address)

    def on_find_node_request(self, msg, address):
        msg = {
            "y": "r",
            "r": {
                "id": self.get_neighbor(msg["target"], self.nid),
                "nodes": ""
            }
        }
        self.send_krpc(msg, address)

    def on_get_peers_request(self, msg, address):
        try:
            info_hash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = info_hash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": self.get_neighbor(info_hash, nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]

            if infohash[:TOKEN_LENGTH] == token:
                if "implied_port" in msg['a'] and msg["a"]["implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                    if port < 1 or port > 65535:
                        return
                self.infohash_queue.put((infohash, (address[0], port)))
        except Exception as e:
            logging.error(e)
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            tid = msg["t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def ok(self, msg, address):
        try:
            tid = msg["t"]
            nid = msg["a"]["id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": self.get_neighbor(nid, self.nid)
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    @staticmethod
    def random_chars(length):
        return "".join(chr(randint(0, 255)) for _ in xrange(length))

    @staticmethod
    def get_neighbor(target, nid, end=10):
        return target[:end] + nid[end:]

    @staticmethod
    def decode_nodes(nodes):
        n = []
        length = len(nodes)
        if (length % 26) != 0:
            return n

        for i in range(0, length, 26):
            nid = nodes[i:i+20]
            ip = inet_ntoa(nodes[i+20:i+24])
            port = unpack("!H", nodes[i+24:i+26])[0]
            n.append((nid, ip, port))

        return n




