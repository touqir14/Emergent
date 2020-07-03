import zmq
import threading
import queue
import time
import random

class Router():
    """
    Router Limited to localhost
    """
    def __init__(self, routes, CURVE=False):

        self.routes = routes
        self.connectionTimeout = 2000
        # self.connectionTimeout = 1000
        self.protocol = 'tcp'
        self.serverSockets = {}
        self.clientSockets = {}
        self.CURVE = CURVE
        self.termination = False
        self.latency_range = [800, 1200]
        self.drop_msg_prob = 0.95
        self.drop_msg_period = 1000 * 10 # 10 seconds in milliseconds
        # self.drop_msg_period = 1000 * 5 # 10 seconds in milliseconds
        self.last_drop_msg_time = None
        self.use_latency = False
        self.use_drop_msg = True

        if self.CURVE:
            self.createServerSocket = self._createServerSocket_CURVE
            self.createClientSocket = self._createClientSocket_CURVE
        else:
            self.createServerSocket = self._createServerSocket_simple
            self.createClientSocket = self._createClientSocket_simple


    def start(self):

        for routeID in range(len(self.routes)):
            self.createServerSocket(routeID)
            self.createClientSocket(routeID)
            msgQueue = queue.Queue()
            server_thread = threading.Thread(target=self.run_server, args=[routeID, msgQueue])
            client_thread = threading.Thread(target=self.run_client, args=[routeID, msgQueue])
            server_thread.start()
            client_thread.start()


    def run_server(self, routeID, msgQueue):

        serverSocket = self.serverSockets[routeID]

        while not self.termination:
            try:
                msg = serverSocket.recv_multipart()
            except Exception as e:
                continue

            msgQueue.put([msg, time.time()])


    def run_client(self, routeID, msgQueue):

        clientSocket = self.clientSockets[routeID]
        serverSocket = self.serverSockets[routeID]

        while not self.termination:
            try:
                msg, arrival_time = msgQueue.get(timeout=self.connectionTimeout/1000)
            except Exception as e:
                continue

            delay = self.compute_latency(arrival_time)
            if delay > 0:
                print("total delay:", delay)
                time.sleep(delay)

            if self.toDropMsg():
                print("Dropping Msg!")
                continue

            clientID = msg[0]
            clientSocket.send_multipart(msg[1:])
            try:
                RESP = clientSocket.recv()
            except Exception as e:
                continue
            serverSocket.send_multipart([clientID, RESP])


    def compute_latency(self, arrival_time):

        if self.use_latency: 
            latency = random.uniform(self.latency_range[0], self.latency_range[1])
            current_time = time.time()
            delay = arrival_time + latency/1000 - current_time
            return delay
        else:
            return 0


    def toDropMsg(self):

        if not self.use_drop_msg:
            return False

        toDrop = False
        if self.last_drop_msg_time is None:
            self.last_drop_msg_time = time.time() 

        if time.time() - self.last_drop_msg_time >  self.drop_msg_period / 1000:
            self.last_drop_msg_time = time.time()
            if random.random() < self.drop_msg_prob:
                toDrop = True

        return toDrop


    def _createServerSocket_simple(self, routeID):

        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        socket.RCVTIMEO = self.connectionTimeout
        port = self.routes[routeID][0]
        addr = "{0}://*:{1}".format(self.protocol, port)
        socket.bind(addr)
        self.serverSockets[routeID] = socket

        return socket



    def _createClientSocket_simple(self, routeID):

        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        routeID_bin = str.encode(chr(routeID))
        socket.setsockopt(zmq.IDENTITY, routeID_bin)
        IP, port = '127.0.0.1', self.routes[routeID][1]
        addr = "{0}://{1}:{2}".format(self.protocol, IP, port)
        socket.RCVTIMEO = self.connectionTimeout
        socket.connect(addr)
        socket.connect(addr)
        self.clientSockets[routeID] = socket

        return socket


    def _createServerSocket_CURVE(self):
        pass

    def _createClientSocket_CURVE(self):
        pass

    def terminate(self):

        self.termination = True



def createRouter(raftConfigs):

    servers = {}
    routes = []

    for raftParams in raftConfigs:
        for k,v in raftParams['servers'].items():
            if k not in servers:
                servers[k] = v

    for raftParams in raftConfigs:
        input_port = servers[raftParams['serverID']][-1]
        output_port = raftParams['port']
        routes.append([input_port, output_port])

    router_instance = Router(routes)
    return router_instance