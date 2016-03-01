import eventlet
from eventlet.green import socket
from eventlet import sleep

from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Thread
from collections import deque
from bencode import bencode, bdecode
import time

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


class DHTClient(object):

    def __init__(self, max_node_qsize):
        self.max_node_qsize = max_node_qsize
        self.nid = self.random_chrs(20)
        self.nodes = eventlet.queue.LightQueue(maxsize=max_node_qsize)
        self.ips = set()
        self.ufd = None
        self._pool_client = eventlet.GreenPool()

    @staticmethod
    def random_chrs(length):
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

    def send_krpc(self, msg, address):
        self.ufd.sendto(bencode(msg), address)

    def send_find_node(self, address, nid=None):
        nid = self.get_neighbor(nid, self.nid) if nid else self.nid
        tid = self.random_chrs(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": self.random_chrs(20)
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def re_join_DHT(self):
        if self.nodes.empty():
            self.join_DHT()

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
        while True:
            try:
                node = self.nodes.get(timeout=1)
                self._pool_client.spawn_n(self.send_find_node, (node.ip, node.port), node.nid)
            except eventlet.queue.Empty:
                self._pool_client.spawn_n(self.re_join_DHT)
            except Exception as e:
                print 'auto_send_find_node', e
            sleep(wait)

    def process_find_node_response(self, msg, address):
        nodes = self.decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            if ip == self.bind_ip: continue
            if port < 1 or port > 65535: continue
            n = KNode(nid, ip, port)
            self.nodes.put(n)
            if len(self.ips) < 100000:
                self.ips.add(ip)
                print len(self.ips), self.nodes.qsize(), ip, port


class DHTServer(DHTClient):

    def __init__(self, master, bind_ip, bind_port, max_node_qsize):
        DHTClient.__init__(self, max_node_qsize)

        self.master = master
        self.bind_ip = bind_ip
        self.bind_port = bind_port

        self.process_request_actions = {
            #"ping": self.on_ping_request,
            "find_node": self.on_find_node_request,
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))
        self._pool_server = eventlet.GreenPool()

    def response(self):
        while True:
            try:
                p = self.ufd.recvfrom(65536)
                if p and len(p) == 2:
                    data, address = p
                    msg = bdecode(data)
                    self._pool_server.spawn_n(self.on_message, msg, address)
            except Exception as e:
                print 'DHTServer run', e

    def run(self):
        self.re_join_DHT()
        self._pool_server.spawn_n(self.auto_send_find_node)
        self._pool_server.spawn_n(self.response)
        self._pool_server.waitall()


    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if msg["r"].has_key("nodes"):
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

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
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = infohash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": self.get_neighbor(infohash, self.nid),
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
                if msg["a"].has_key("implied_port") and msg["a"]["implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                    if port < 1 or port > 65535:
                        return
                self.master.log(infohash, (address[0], port))
        except Exception as e:
            print 'on_announce', e
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


class Master(object):

    def log(self, infohash, address=None):
        print "%s from %s:%s" % (
            infohash.encode("hex"), address[0], address[1]
        )



