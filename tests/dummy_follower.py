import init
import zmq
from dummy_params1 import raftParams
import threading
from core import Log
import logger
import msgpack
import random
import _globals
import time

class Follower:

    def __init__(self, servers, leaderID):

        self.servers = servers
        self.sockets = {}
        self.leaderID = leaderID
        self.term = 1
        self.log = Log(None, None, None, True)

        for server in self.servers.items():
            serverID, IP_port = server
            self.createSocket(IP_port[0], IP_port[1], serverID)

        self.initServerThreads()


    def initServerThreads(self):

        for serverID in self.sockets:
            socket = self.sockets[serverID]
            t = threading.Thread(target=self.server_loop, args=(serverID, socket),)
            t.start()


    def createSocket(self, IP, port, serverID):

        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        
        if IP == 'localhost':
            addr = "tcp://*:{0}".format(port)
        else:
            addr = "tcp://{0}:{1}".format(IP, port)

        socket.bind(addr)
        self.sockets[serverID] = socket 
        _globals._print("From Server:", serverID, "Created Socket:", addr)


    def server_loop(self, serverID, socket):
        recent_LogIdx = None 
        
        while True:
            clientID, msg = socket.recv_multipart()
            if b'Connection Request' == msg[:18]:
                _globals._print("From server:",serverID,"|"msg)
                time.sleep(2.1)
                socket.send_multipart([clientID, b'Connection ACK'])
                continue

            else:
                if recent_LogIdx is not None and recent_LogIdx == self.log.get_entry_logIndex(msg):
                    continue

                recent_LogIdx = self.log.get_entry_logIndex(msg)
                with _globals.print_lock:
                    print("Reporting from server:", serverID)
                    print("Received a message from leader:", ord(clientID))
                    logger.print_entry_buffer(self.log, msg)

                success = True
                followerTerm = self.term
                RESP = msgpack.packb([success, followerTerm])
                delay = random.uniform(0.3, 0.8)
                time.sleep(delay)
                socket.send_multipart([clientID, RESP])





def run():
    numFollowers = raftParams['numServers'] - 1
    followers = Follower(raftParams['servers'], raftParams['serverID'])

if __name__ == '__main__':
    run()   
