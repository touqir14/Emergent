import random
import msgpack
import utils
import multiprocessing
from multiprocessing import shared_memory
import threading
import queue
import llist
import time
import math
from runpy import run_path
from argparse import Namespace
import zmq
from testkit import TestKit
from collections.abc import Iterable
import _globals
import logger





def dummy_stateNotifier(state):
    _globals._print("State changed to", state)

class RaftLauncher:

    def __init__(self, param_path, testMode=False):

        pipe_a, pipe_b = multiprocessing.Pipe()
        msgQueue = multiprocessing.Queue()
        self.testMode = testMode
        self.raft = RaftServer(pipe_a, msgQueue, testMode)
        self.loadParams(param_path)
        self.raft.initLog()
        # self.raft.initEverything(testMode)
        self.stateMachine = StateMachine(pipe_b, msgQueue, dummy_stateNotifier, testMode)
        # self.launchProcesses()

    def loadParams(self, param_path):
        
        param_mod = Namespace(**run_path(param_path))
        raftParams = param_mod.raftParams
        for k in raftParams:
            if k in self.raft.__dict__:
                self.raft.__dict__[k] = raftParams[k]


    def launchProcesses(self, testkit=None):
        
        state = 'leader'
        self.raftProcess = multiprocessing.Process(target=self.raft.start, args=(state, testkit),)
        self.raftProcess.start()

    def start(self):

        if self.testMode:
            pipe_a, pipe_b = multiprocessing.Pipe()
            self.testkit = TestKit(self.raft)
            self.testkit.setPipes(pipe_a, pipe_b)
            self.launchProcesses(self.testkit)
            return self.stateMachine, self.testkit
        else:
            self.launchProcesses()
            return self.stateMachine


    def exit(self):
        self.stateMachine.terminate()


class RaftServer:

    """
    self.state : [0,1,2]. 0 identifies a leader, 1 identifies a follower, 2 identifies a candidate.
    self.currentTerm : a non-negative integer. Denotes the current term of the server.
    self.commitIndex : a non-negative integer. Denotes the highest log entry index known to be committed.
    self.lastApplied : a non-negative integer. Denotes the highest log entry index applied to the state machine.
    self.nextIndex : (leader) a list of positive integer initialized to 1 for all servers. i'th entry denotes the next log entry to be sent to the i'th follower.
    self.matchIndex : (leader) a list of non-negative integers initialized to 0 for all servers. i'th entry denotes index of highest log entry replicated to i'th follower.
    """

    def __init__(self, stateMachinePipe, msgQueue, testMode=False):
        
        self.state = None
        self.commitIndex = 0
        self.currentTerm = 1
        self.lastLogIndex = 0
        self.lastLogTerm = 1
        self.prevLogIndex = -1
        self.prevLogTerm = 1
        self.lastApplied = 0 # For follower
        # self.nextIndex = []
        self.matchIndex = {}
        self.numServers = 1
        self.servers = {}
        self.serverID = None
        self.port = None
        self.timeoutRange = [0.1,0.2]
        self.protocol = "tcp"
        self.logMaxSize = 10**6
        self.logMaxLength = 10**8
        self.workerProcesses = [] 
        self.numProcesses = 2
        self.maxThreadPerProcess = 2
        self.processToIteratorIDs = {}
        self.processPipes = {}
        self.iteratorIDs = []
        self.replicationHistory = {}
        self.numMajorityServers = 0
        self.stateMachinePipe = stateMachinePipe
        self.listenMsgQueue = msgQueue

        self.testMode = testMode

        random.seed(time.time())

        # self.initLog()


    def initEverything(self):

        # self.initLog()
        # self.testMode = testMode

        self.initAuxiliaryThreads()
        with _globals.processForkLock:
            self.initWorkerProcesses()
            self.initServers()


    def initLog(self):

        self.clearLog_event = multiprocessing.Event()
        self.log = Log(self.logMaxSize, self.logMaxLength, self.clearLog_event, self.testMode)
        self.log.matchIndex = self.matchIndex
        self.log.initIterators(self.iteratorIDs, [])


    def initAuxiliaryThreads(self):

        self.termination_event = multiprocessing.Event()

        self.entryQueue = queue.Queue()
        self.fetchPendingLogQueue = queue.Queue()
        self.commitQueue = queue.Queue()
        # self.listenMsgQueue = multiprocessing.Queue()
        
        self.serverThread = threading.Thread(target=self.start_server, )
        self.serverThread.start()

        self.listenerThread = threading.Thread(target=self.listen_loop, )
        self.listenerThread.start()

        self.entryAdderThread = threading.Thread(target=self.entryAdd_loop, )
        self.entryAdderThread.start()


    def initWorkerProcesses(self):

        for i in range(self.numProcesses):
            processName = 'p-'+str(i)
            manager = CommunicationManager(self.serverID, self.testMode)
            pipes_in = []
            pipes_out = []
            for j in range(self.maxThreadPerProcess):
                p1, p2 = multiprocessing.Pipe()
                p1.name, p2.name = [processName,j], [processName,j]
                pipes_in.append(p1)
                pipes_out.append(p2)

            self.processPipes[processName] = pipes_in
            pipe_a, pipe_b = multiprocessing.Pipe()
            pipe_a.name, pipe_b.name = 'main', 'main'
            p = multiprocessing.Process(target=manager.start, args=(self.listenMsgQueue, pipe_b, pipes_out, i),)
            p.name = processName
            self.workerProcesses.append(p)
            self.processPipes[p.name].append(pipe_a)
            p.start()

        self.assignIteratorToWorker()


    def initServers(self):

        for process_name in self.processPipes:
            mainPipe = self.processPipes[process_name][-1]
            iteratorIDs = self.processToIteratorIDs[process_name]
            servers = {}
            for iteratorID in iteratorIDs:
                servers[iteratorID] = self.servers[iteratorID]
            mainPipe.send(['updateServer', servers])


    def assignIteratorToWorker(self):

        slot_size = math.floor(len(self.iteratorIDs) / self.numProcesses)
        remaining = len(self.iteratorIDs) - slot_size*self.numProcesses
        idx = 0
        for p in self.workerProcesses:
            self.processToIteratorIDs[p.name] = self.iteratorIDs[idx:idx+slot_size]
            idx += slot_size

        if remaining != 0:
            self.processToIteratorIDs[self.workerProcesses[-1].name] += self.iteratorIDs[-remaining:]


    def changeState(self, new_state):

        if new_state == 'leader':
            self.state = 'leader'
            # params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]
            params = {}
            params['lastLogIndex'] = self.lastLogIndex
            params['term'] = self.currentTerm
            params['prevLogIndex'] = self.prevLogIndex
            params['prevLogTerm'] = self.prevLogTerm
            params['commitIndex'] = self.commitIndex
            params['leaderID'] = self.serverID
            self.stateMachinePipe.send(['newState', new_state, params])

        elif new_state == 'follower':
            self.state = 'follower'
            pass

        elif new_state == 'candidate':
            self.state = 'candidate'
            pass


    def start(self, new_state, testkit):

        if type(testkit) is TestKit:
            self.testkit = testkit
            self.testkit.initListenThread()
            # _globals._print("yeeee")
        else:
            self.testkit = None

        self.initEverything()
        self.changeState(new_state)
        self.terminate()


    def listen_loop(self):

        while True:
            msg = self.listenMsgQueue.get()
            # _globals._print("From RaftServer.listen_loop")
            # _globals._print(msg)
            # code = msg[0]
            if msg[0] == 'fetchLogEntry':
                command, iteratorID, numEntries, pipe_name = msg[1], msg[2], msg[3], msg[4]
                self.fetchLogEntry(command, iteratorID, numEntries, pipe_name)

            elif msg[0] == 'followerTerm':
                followerTerm = msg[1]
                # Add code for processing state transitions (to follower) if followerTerm is larger than leader's current term.
                self.processStaleTerm(followerTerm) 

            elif msg[0] == 'successfulReplication':
                logIndex, iteratorID = msg[1], msg[2]
                self.successfulReplication_handler(logIndex, iteratorID)

            elif msg[0] == 'addLogEntry':
                shm_name, params, total_size = msg[1], msg[2], msg[3]
                self.entryQueue.put([shm_name, params, total_size])
                # self.addLogEntry(shm_name, params, total_size) 

            elif msg[0] == 'terminate':
                if msg[1]: # if msg[1] is True, then self.terminate will be executed by the main thread, else not.
                    self.termination_event.set()
                else:
                    _globals._print("From Raft Class: listen_loop terminating")
                    break

            elif msg[0] == 'stopThread':
                # _globals._print("From RaftServer.listen_loop")
                pipe_name = msg[1]
                processName, pipe_idx = pipe_name[0], pipe_name[1]
                pipe = self.processPipes[processName][pipe_idx]
                pipe.send(['terminate'])

            else:
                _globals._print("Wrong command - {0}: From RaftServer.listen_loop".format(msg[0]))



    def successfulReplication_handler(self, logIndex, iteratorID):

        self.matchIndex[iteratorID] = logIndex

        if self.testMode:
            logger.log_successfulReplication(logIndex, iteratorID)

        if logIndex not in self.replicationHistory:
            self.replicationHistory[logIndex] = [iteratorID]
        else:
            if self.replicationHistory[logIndex] is None:
                return 

            self.replicationHistory[logIndex].append(iteratorID)


        if len(self.replicationHistory[logIndex]) >= self.numMajorityServers:
            # self.commitQueue.put(logIndex)
            self.commitIndex = logIndex
            self.sendCommitACK(logIndex)
            self.replicationHistory[logIndex] = None # Make sure to clean up self.replicationHistory variable!

            if self.testMode:
                logger.log_committed(logIndex, self.replicationHistory[logIndex])



    def processStaleTerm(self, followerTerm):
        pass


    def fetchLogEntry(self, command, iteratorID, numEntries, pipe_name):
        
        shm_names = []
        flags = [0]
        logIndices = []
        terms = []

        if command == 'next':
            for i in range(numEntries):
                entry = self.log.next(iteratorID, getNode=True)
                if entry is not None:
                    shm_names.append(entry.value[0])
                    term = self.log.get_entry_term(entry)
                    terms.append(term)
                    logIndex = self.log.get_entry_logIndex(entry)
                    logIndices.append(logIndex)
            
                else:
                    flags[0] = 1
                    break

        elif command == 'prev':
            for i in range(numEntries):
                entry = self.log.prev(iteratorID, getNode=True)
                if entry is not None:
                    shm_names.append(entry.value[0])
                    term = self.log.get_entry_term(entry)
                    terms.append(term)
                    logIndex = self.log.get_entry_logIndex(entry)
                    logIndices.append(logIndex)

                else:
                    shm_names.append(None)
                    break

        if len(shm_names) > 0:
            processName, pipe_idx = pipe_name[0], pipe_name[1]
            pipe = self.processPipes[processName][pipe_idx]
            pipe.send([shm_names, logIndices, terms, flags])

        if flags[0] > 0:
            self.fetchPendingLogQueue.put([command, iteratorID, 1, pipe_name])


    def sendHeartBeat(self):
        pass

    def sendCommitACK(self, logIndex):

        self.stateMachinePipe.send(['executeLogEntry', logIndex])


    def entryAdd_loop(self):

        while True:
            msg = self.entryQueue.get()
            if msg == ['terminate']:
                _globals._print("From Raft Class: entryAdd_loop terminating")
                break
            shm_name, params, entry_size = msg
            self.addLogEntry(shm_name, params, entry_size)


    def addLogEntry(self, shm_name, params, entry_size):

        params += [entry_size, False] # Adding isCommitted=False.
        self.lastLogIndex = params[0]
        self.lastLogTerm = params[1]
        self.prevLogIndex = params[2]
        self.prevLogTerm = params[3]
        self.log.addEntry(shm_name, params)
        self.stateMachinePipe.send(['addLogEntry-ACK'])
        self.processPendingEntries()


    def processPendingEntries(self):

        while not self.fetchPendingLogQueue.empty():
            command, iteratorID, numEntries, pipe_name = self.fetchPendingLogQueue.get()
            self.fetchLogEntry(command, iteratorID, numEntries, pipe_name)


    def start_server(self):

        self.connector = Connector(self.serverID, self.protocol, testMode=self.testMode)
        socket = self.connector.createServerSocket(self.port)
        self.connector.server_listen(socket, self.serverRequest_handler)


    def serverRequest_handler(self, data):
        """
        A heartbeat response will always contain success=True
        """

        clientID, msgType, msg = data[:3]

        if msgType == b'AppendEntries':
            extra_params = msgpack.unpackb(data[3])
            leaderCommitIndex, leaderTerm = extra_params['leaderCommitIndex'], extra_params['leaderTerm']
            if leaderTerm < self.currentTerm:
                RESP = msgpack.packb([False, self.currentTerm])
                return 'reply', RESP

            lastEntry = self.log.get_lastEntry()
            if lastEntry is not None:
                lastLogIdx, lastLogTerm = self.get_entry_logIndex(lastEntry), self.get_entry_term(lastEntry) 
                if lastLogTerm == self.get_entry_term(msg) and lastLogIdx == self.get_entry_logIndex(msg):
                    RESP = msgpack.packb([True, self.currentTerm])
                    return 'reply', RESP

                if lastLogTerm == self.get_entry_prevLogTerm(msg) and lastLogIdx == self.get_entry_prevLogIndex(msg):
                    shm_name, logIdx = self.addEntry_follower(msg, leaderCommitIndex)
                    self.addCommitQueue(shm_name, logIdx, leaderCommitIndex)
                    RESP = msgpack.packb([True, self.currentTerm])
                else:
                    RESP = msgpack.packb([False, self.currentTerm])

                return 'reply', RESP

            else:
                pass

        elif msgType == b'Heartbeat':
            RESP = msgpack.packb([True, self.currentTerm])
            return 'reply', RESP

        elif msgType == b'RequestVote':
            pass

        return


    def addCommitQueue(self, shm_name, logIdx, leaderCommitIndex):

        sendForExecution = []
        self.commitQueue.put([shm_name, logIdx])
        while not self.commitQueue.empty():
            name, idx = self.commitQueue.queue[0]
            if idx <= leaderCommitIndex:
                self.commitQueue.get()
                sendForExecution.append(name)
            else:
                break

        self.commitIndex = leaderCommitIndex
        self.stateMachinePipe.send(['executeLogEntry', self.commitIndex, sendForExecution])

    # def replicateLogEntry(self):
    #   pass

    # def requestVote(self):
    #   pass

    # def followerToCandidate(self):
    #   pass

    # def candidateToFollower(self):
    #   pass

    # def candidateToLeader(self):
    #   pass

    # def leaderToFollower(self):
    #   pass

    def resetTimer(self, oldTimer, runnable, startTimer=True):
        """
        Use threading.Timer for setting a timer. 
        See https://stackoverflow.com/questions/24968311/how-to-set-a-timer-clear-a-timer-in-python
        threading.Timer.interval gives the duration.
        """
        if oldTimer is not None:
            oldTimer.cancel()

        timerDuration = random.uniform(self.timeoutRange[0], self.timeoutRange[1])
        newTimer = threading.Timer(timerDuration, runnable)
        
        if startTimer: 
            newTimer.start()

        return newTimer


    def terminate(self):

        self.termination_event.wait()
        self.connector.terminateServer = True
        # time.sleep(2)
        for p in self.workerProcesses:
            self.processPipes[p.name][-1].send(['terminate'])

        # time.sleep(3)
        # _globals._print('1')
        for p in self.workerProcesses:
            p.join()
            # p.kill()
            # p.close()

        # time.sleep(3)
        # _globals._print('2')
        self.stateMachinePipe.send(['terminate'])
        # time.sleep(3)
        # _globals._print('3')
        self.listenMsgQueue.put(['terminate', False])
        # time.sleep(3)
        # _globals._print('4')
        self.entryQueue.put(['terminate'])
        # time.sleep(3)
        # _globals._print('5')
        self.listenerThread.join()
        self.entryAdderThread.join()
        self.serverThread.join()
        self.log.deleteAllEntries()

        _globals._print("Exiting RaftServer instance")
        return


class StateMachine:

    def __init__(self, pipe, msgQueue, stateNotifier, testMode=False):


        # params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]
        self.params = {}
        self.commandBuffer = {}
        self.msgQueue = msgQueue
        self.pipe = pipe
        self.log = Log(None, None, None, testMode)
        self.isLeader = None
        self.testMode = testMode
        # self.stateNotifier needs to be called to notify the native server that the state \in \{leader, follower, candidate\} has changed.
        self.stateNotifier = stateNotifier
        self.loopThreadKill = False

        self.addEntryEvent = threading.Event()
        self.addEntryEvent.set()

        self.paramsLock = threading.Lock()

        self.loopThread = threading.Thread(target=self.listen_loop,)
        self.loopThread.start()


    def terminate(self):

        self.loopThreadKill = True
        self.msgQueue.put(['terminate', True])
        self.loopThread.join()


    def updateParams(self, params):

        if params is None:
            return

        if self.params == {}:
            self.params = params
        else:
            with self.paramsLock:
                for k in params:
                    if k in self.params:
                        self.params[k] = params[k]


    def processStateChange(self, newState):

        """
        newState in {"leader", "follower", "candidate"}
        """

        if newState == 'leader':
            if self.isLeader == False or self.isLeader == None:
                self.isLeader = True
                self.stateNotifier(newState)

        elif newState == 'follower' or newState == 'candidate':
            if self.isLeader == True or self.isLeader == None:
                self.isLeader = False
                self.stateNotifier(newState)
        # elif newState == 'candidate':
        #   pass


    def executeCommand(self, func, args, callback=None):
        """
        When a Raft instance shifts transforms into a leader, how should the native server know it 
        """

        self.addEntryEvent.wait()
        self.addEntryEvent.clear()

        if not self.isLeader:
            self.addEntryEvent.set()
            return None

        if self.loopThreadKill:
            return None

        with self.paramsLock:
            self.params['lastLogIndex'] += 1 
            lastLogIndex = self.params['lastLogIndex']
            self.params['prevLogIndex'] += 1
            prevLogIndex = self.params['prevLogIndex']
            term = self.params['term']
            prevLogTerm = self.params['prevLogTerm']
            self.params['prevLogTerm'] = term 
            commitIndex = self.params['commitIndex']
            leaderID = self.params['leaderID']

        params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]
        self.commandBuffer[lastLogIndex] = [func, args]
        shm_name, total_size = self.log.createLogEntry(func, args, params)
        # time.sleep(12)
        self.msgQueue.put(('addLogEntry', shm_name, params, total_size))

        return True



    def executeLogEntry(self, logIdxToExecute):

        func, args = self.commandBuffer.pop(logIdxToExecute)
        func(*args)

        if self.testMode:
            logger.log_entryExecution(logIdxToExecute)


    def loadExecuteLogEntry(self, shm_names):

        for name in shm_names:
            shm = shared_memory.SharedMemory(name)
            func_name, args = self.log.get_entry_data(shm)
            execute_str = '{0}(*args)'.format(func_name)
            eval(execute_str)
            utils.freeSharedMemory(shm, clear=False) 
            """
            Needs to be clear=True if the log is needed
            to be cleared regularly.
            """


    def listen_loop(self):

        while True:
            if (not self.pipe.poll()) and self.loopThreadKill:
                _globals._print("From StateMachine class: listen_loop terminating")
                break

            msg = self.pipe.recv()
            
            if msg[0] == 'executeLogEntry':
                commitIndex = msg[1]
                self.updateParams({'commitIndex':commitIndex})
                if len(msg) <= 2:
                    self.executeLogEntry(commitIndex)
                else:
                    self.loadExecuteLogEntry(msg[2]) # msg[2]: a list of shm_names                    

            elif msg[0] == 'addLogEntry-ACK':
                self.addEntryEvent.set()

            elif msg[0] == 'terminate':
                _globals._print("From StateMachine class: listen_loop terminating")
                break

            elif msg[0] == 'resetParams':
                # Add function for sending 'resetParams' in Raft class!
                new_params = msg[1]
                self.updateParams(new_params)

            elif msg[0] == 'newState':
                # Add function for 'newState' in Raft Class!
                new_state, new_params = msg[1], msg[2]
                self.updateParams(new_params)
                self.processStateChange(new_state)

            else:
                _globals._print("Wrong command - {0}: From StateMachine.listen_loop".format(msg[0]))



class Connector:

    def __init__(self, serverID, protocol, curve_auth=False, cert_path=None, testMode=False):
        
        self.serverID = serverID
        self.protocol = protocol
        self.connectionTimeout = 1000 # in milliseconds. Maybe have different timeouts for connection and AppendRPCs ??????????
        self.connectionRetries = 3
        self.curve_auth = curve_auth
        self.cert_path = cert_path
        self.terminateServer = False
        self.terminateClient = False
        self.testMode = testMode

        if self.curve_auth:
            self.createClientSocket = self._createClientSocket_CURVE
            self.createServerSocket = self._createServerSocket_CURVE
        else:
            self.createClientSocket = self._createClientSocket_simple
            self.createServerSocket = self._createServerSocket_simple


    def connect(self, socket, addr, destServerID, trials=None):

        if trials is None:
            trials = self.connectionRetries

        success = False
        for i in range(trials):
            socket.connect(addr)
            msgType = bytes('Connection Request', 'ascii')
            msg = bytes('Connection Request from Server: {0}'.format(self.serverID), 'ascii')
            socket.send_multipart([msgType, msg])

            try:
                received = socket.recv()
            except Exception as e:
                continue

            if received == b'Connection ACK':
                success = True
                break

        if self.testMode:
            if success:
                _globals._print("SUCCESS: socket to server {0} connected at: {1}".format(destServerID, socket.LAST_ENDPOINT))
            else:
                _globals._print("ERROR: socket to server {0} failed to connect at: {1}".format(destServerID, socket.LAST_ENDPOINT))

        return success


    def _createClientSocket_simple(self, servers, destServerID):

        IP, port = servers[destServerID]
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        serverID_bin = str.encode(chr(self.serverID))
        socket.setsockopt(zmq.IDENTITY, serverID_bin)
        addr = "{0}://{1}:{2}".format(self.protocol, IP, port)
        socket.RCVTIMEO = self.connectionTimeout
        success = self.connect(socket, addr, destServerID)
        
        return socket, success, addr, destServerID


    def _createClientSocket_CURVE(self, servers, destServerID):

        """
        Uses Ironhouse encryption based on CURVE
        """
        pass


    def _createServerSocket_simple(self, port):

        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        socket.RCVTIMEO = self.connectionTimeout
        addr = "{0}://*:{1}".format(self.protocol, port)
        socket.bind(addr)
        
        return socket


    def _createServerSocket_CURVE(self, servers, destServerID):

        """
        Uses Ironhouse encryption based on CURVE
        """
        pass


    def loadCertificates(self):
        pass


    def server_listen(self, socket, RESP_handler):

        while not self.terminateServer:
            try:
                data = socket.recv_multipart() # data : [clientID, msgType, msg]
            except Exception as e:
                continue

            clientID = data[0]
            if b'Connection Request' == data[1]:
                if self.testMode:
                    _globals._print("From server:", self.serverID, "|", data[2])
                # time.sleep(2.1)
                socket.send_multipart([clientID, b'Connection ACK'])
                continue
            else:
                cmd, RESP = RESP_handler(data)

            if cmd == 'reply':
                # delay = random.uniform(0.3, 0.8)
                socket.send_multipart([clientID, RESP])


    def appendEntriesRPC(self, data, socket, destServerID, extra_params, isHeartbeat=False):

        if not isHeartbeat:
            msgType = b'AppendEntries'
        else:
            msgType = b'Heartbeat'

        if type(data) is shared_memory.SharedMemory:
            msg = data.buf
        else:
            msg = data

        resend = True
        success = None
        followerTerm = None
        extra_params_bin = msgpack.packb(extra_params, use_bin_type=True)
        while not self.terminateClient:
            if resend: 
                socket.send_multipart([msgType, msg, extra_params_bin])
            
            try:
                RESP_bin = socket.recv()
                resend = False
            except Exception as e:
                if self.testMode: 
                    _globals._print("Retrying AppendRPC to server: {0} at address: {1}".format(destServerID, socket.LAST_ENDPOINT))
                    resend = True
                continue

            if RESP_bin[:14] == b'Connection ACK':
                continue

            RESP = msgpack.unpackb(RESP_bin)
            success = RESP[0]
            followerTerm = RESP[1]
            break

            if self.testMode:
                _globals._print("Received RESP from server: {0}, address: {1}. Success: {2}, followerTerm: {3}".format(destServerID, socket.LAST_ENDPOINT, success, followerTerm))

        if not self.terminateClient:
            return success, followerTerm
        else:
            return None, None


    def sendHeartbeats(self):
        pass


class CommunicationManager:

    def __init__(self, serverID, testMode):

        self.state = None
        self.servers = {}
        self.serverSockets = {}
        self.iteratorThreads = {}
        self.protocol = 'tcp'
        self.serverID = serverID
        self.testkit = None
        self.testMode = testMode
        self.log = Log(None,None,None,True)
        self.connector = Connector(serverID, self.protocol, testMode=testMode)

        if testMode:
            self.testkit = TestKit(None, ComMan=self)


    # def appendEntriesRPC(self, shm, socket):
    #     binary_data = shm.buf
    #     socket.send(binary_data)
    #     RESP_bin = socket.recv()
    #     RESP = msgpack.unpackb(RESP_bin)
    #     success = RESP[0]
    #     followerTerm = RESP[1]
    #     # The leader should send the RPC indefinitely till it receives a "proper" ACK.
    #     return success, followerTerm


    # def createSocket(self, serverID):
    #     IP, port = self.servers[serverID]
    #     context = zmq.Context()
    #     socket = context.socket(zmq.DEALER)
    #     serverID_bin = str.encode(chr(self.serverID))
    #     socket.setsockopt(zmq.IDENTITY, serverID_bin)
    #     addr = "{0}://{1}:{2}".format(self.protocol, IP, port)
    #     socket.connect(addr)
    #     socket.send_string('Connected! From server {0}'.format(serverID))
    #     _globals._print("socket connected: ", socket.LAST_ENDPOINT)
    #     return socket


    def start(self, msgQueue, pipe, threadPipes, procIdx):

        # _globals._print("GOT to ComMan start")
        self.mainMsgQueue = msgQueue
        self.mainPipe = pipe
        self.threadPipes = threadPipes
        self.procIdx = procIdx
        self.listen_loop()


    def listen_loop(self):

        while True:
            # _globals._print("GOT to ComMan listen_loop")
            msg = self.mainPipe.recv()
            # _globals._print("from comMan: ")
            # _globals._print(msg)
            if msg[0] == 'updateServer':
                data = msg[1]
                # self.updateServer(data)
                self.updateServerIterator(data)

            elif msg[0] == 'terminate':
                self.terminate()
                _globals._print("From CommunicationManager class: Terminating process")
                # time.sleep(10)
                break

            elif msg[0] == 'testkit' and self.testkit is not None:
                self.testkit.processRequest(msg[1], {'procIdx':self.procIdx, 'mode':0})

            else:
                _globals._print("Wrong command - {0}: From CommunicationManager.listen_loop".format(msg[0]))


    
    def terminate(self):

        for _, pipe in self.iteratorThreads.values():
            self.mainMsgQueue.put(['stopThread', pipe.name])

        # _globals._print(3)
        for thread, _ in self.iteratorThreads.values():
            thread.join()
            # _globals._print(4)


    def updateServerIterator(self, servers):

        serverIDs = servers.keys()
        self.updateServer(servers)
        self.updateIterator(serverIDs)
        # _globals._print("Done")


    def updateServer(self, servers):

        for serverID in servers:
            if serverID in self.servers:
                if not self.servers[serverID] == servers[serverID]:
                    oldSocket = self.serverSockets[serverID][0]
                    oldSocket.close()
                    self.servers[serverID] = servers[serverID]
                    socket, success, addr, destServerID = self.connector.createClientSocket(self.servers, serverID)
                    self.serverSockets[serverID] = [socket, success, addr, destServerID] 
            else:
                self.servers[serverID] = servers[serverID]
                socket, success, addr, destServerID = self.connector.createClientSocket(self.servers, serverID)
                self.serverSockets[serverID] = [socket, success, addr, destServerID] 

        toDelete = set(self.servers.keys()) - set(servers.keys())
        for serverID in toDelete:
            socket = self.serverSockets.pop(serverID)[0]
            socket.close()
            del self.servers[serverID]


    def updateIterator(self, iteratorIDs):

        toDelete = set(self.iteratorThreads.keys()) - set(iteratorIDs)
        toAdd = set(iteratorIDs) - set(self.iteratorThreads.keys())

        for ID in toDelete:
            pipe = self.iteratorThreads[ID][1]
            self.threadPipes.append(pipe)
            self.mainMsgQueue.put(['stopThread', pipe.name])
            del self.iteratorThreads[ID]
            # RaftServer Class must send a terminate command through 

        for ID in toAdd:
            pipe = self.threadPipes.pop()
            t = threading.Thread(target=self.replicateLogEntry, args=(ID,pipe,),)
            self.iteratorThreads[ID] = [t, pipe]
            t.start()


    def fetchLogEntryData(self, iteratorID, pipe, params):
        """
        params[0] : 'next' or 'prev'
        params[0] : By default : 1 . Could be more than 1 if multiple log entries are needed
        """

        if pipe.flags[0] > 0:
            data = pipe.recv()
            if data == ['terminate']:
                return 'terminate'
            else:
                SM_names, logIndices, terms, flags = data
            pipe.flags = flags

        else: 
            code = 'fetchLogEntry'
            command = params[0]

            if command == 'next' or command == 'prev':
                # pipe.send([code, command, iteratorID, params[1]])
                self.mainMsgQueue.put([code, command, iteratorID, params[1], pipe.name])
                data = pipe.recv()
                if data == ['terminate']:
                    return 'terminate'
                else:
                    SM_names, logIndices, terms, flags = data
                pipe.flags = flags
            else:
                return None

        data = []
        for name, logIndex, term in zip(SM_names, logIndices, terms):
            # _globals._print('shm name:', name)
            # with _globals.shm_lock:
            # time.sleep(2)
            shm = shared_memory.SharedMemory(name=name)
            # _globals._print(self.log.get_entry_data(shm.buf))
            data.append([shm, logIndex, term])
            # data.append([name, logIndex, term])

        return data


    # def clearLogEntry(self, shm_objects):

    #     for shm in shm_objects:
    #         utils.freeSharedMemory(shm, clear=False)


    def replicateLogEntry(self, iteratorID, pipe):
        """
        Currently the logic that it uses works correctly for fetching 1 log entry at a time, ie ['next', 1]
        """

        motion = 'forward'
        pipe.flags = [0]

        while True:

            if iteratorID not in self.iteratorThreads:
                break

            if motion == 'forward':
                data = self.fetchLogEntryData(iteratorID, pipe, ['next', 1])
            elif motion == 'backward':
                data = self.fetchLogEntryData(iteratorID, pipe, ['prev', 1])
                # datum will be None if the iterator is pointing to the first Log Entry.
                # Perhaps use that information???
            if data == 'terminate':
                _globals._print("From CommunicationManager class: Terminating thread with iteratorID:", iteratorID)
                break

            # Later verify whether there is a valid connection using Connector.connect and Connector.sendHeartbeats.
            # Keep trying till you get a valid connection before sending data via appendRPC msgs.
            socket = self.serverSockets[iteratorID][0] 
            for datum in data:
                shm, logIndex, term = datum
                # name, logIndex, term = datum
                # with _globals.shm_lock:
                # shm = shared_memory.SharedMemory(name=name)
                # _globals._print(self.log.get_entry_data(buf))


                successful, followerTerm = self.connector.appendEntriesRPC(shm, socket, iteratorID)
                self.mainMsgQueue.put(['followerTerm', followerTerm, iteratorID])

                if successful:
                    motion = 'forward'
                    self.mainMsgQueue.put(['successfulReplication', logIndex, iteratorID])
                else:
                    motion = 'backward'
                        
                utils.freeSharedMemory(shm, clear=False)
            # self.clearLogEntry(data)



class Log:

    def __init__(self, maxSize, maxLength, clearLog_event, testMode):

        self.log = llist.dllist([0])
        self.maxSize = maxSize
        self.maxLength = maxLength
        self.ptrs = {}
        self.ref_point = 0
        self.nodeSizes = [0] # Use two dictionaries to optimize the delete operation! Wrap in a class.
        self.logSize = 0
        self.clearLog_event = clearLog_event
        self.matchIndex = None
        self.params_size = 48
        self.testMode = testMode


    # def initIterators(self, numNewIterators, obsoleteIterators):

    #     for iteratorID in obsoleteIterators:
    #         if iteratorID in self.ptrs:
    #             del self.ptrs[iteratorID]

    #     totalIterators = len(self.ptrs) + numNewIterators
    #     newIDs = []

    #     if len(obsoleteIterators) >= numNewIterators:
    #         newIDs = sorted(obsoleteIterators)[:numNewIterators]
    #         for iteratorID in newIDs:
    #             self.ptrs[iteratorID] = [self.log.first, self.ref_point]
    #     else:
    #         allIDs = set(range(totalIterators))
    #         usedIDs = set(self.ptrs.keys())
    #         newIDs = list(allIDs - usedIDs) 
    #         for iteratorID in newIDs:
    #             self.ptrs[iteratorID] = [self.log.first, self.ref_point]

    #     return newIDs


    def initIterators(self, newIterators, obsoleteIterators):

        for ID in obsoleteIterators:
            if ID in self.ptrs:
                del self.ptrs[ID]

        for ID in newIterators:
            self.ptrs[ID] = [self.log.first, self.ref_point]


    def findSmallestPtr(self):

        ref_points = list(zip(*self.ptrs.values()))[1]
        smallestPtrIdx = ref_points.index(min(ref_points))
        ptr, idx = list(self.ptrs.values())[smallestPtrIdx]
        idx -= self.ref_point
        return ptr, idx


    def findLargestPtr(self):

        ref_points = list(zip(*self.ptrs.values()))[1]
        largestPtrIdx = ref_points.index(max(ref_points))
        ptr, idx = list(self.ptrs.values())[largestPtrIdx]
        idx -= self.ref_point
        return ptr, idx


    def addNodeSize(self, size, idx=None):

        if idx is None:
            if self.logSize + size > self.maxSize:
                return False
            else:
                self.nodeSizes.append(size)
                self.logSize += size
        else:
            if self.logSize - self.nodeSizes[idx] + size > self.maxSize:
                return False
            else:
                self.logSize = self.logSize + size - self.nodeSizes[idx]
                self.nodeSizes[idx] = size

        return True


    def deleteNodeSize(self, idxs):

        sizes = self.nodeSizes[idxs]
        if type(sizes) is list:
            sizeSum = sum(sizes)
        else:
            sizeSum = sizes

        self.logSize -= sizeSum
        del self.nodeSizes[idxs]
        return


    def clearLog(self, reduceBy):
        """
        This function will communicate with workers/actors that are responsible for replicating to each follower
        and then use the self.ptrs location to pinpoint the part of the log that can be safely deleted. 
        Add code to save state machine state uptil the point in the log that will be deleted.

        Think more on how to handle this!
        
        """

        while (self.logSize + reduceBy) > self.maxSize or self.log.size >= self.maxLength:
            smallestPtr, smallestIdx = self.findSmallestPtr()
            # if smallestIdx == 0:
            #   self.clearLog_event.wait()
            #   self.clearLog_event.clear()
            self.deleteAllPrevEntries(smallestPtr, smallestIdx)


    def serializeEntry(self, data):
        
        binary_data = msgpack.packb(data, use_bin_type=True)
        return binary_data


    def deserializeEntry(self, binary_data):

        data = msgpack.unpackb(binary_data, use_list=True)
        return data



    def get_entry_term(self, entry):
        if type(entry) is llist.dllistnode:     
            term = entry.value[2]

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            term = utils.decode_int_bytearray(entry, buf_len-8, buf_len-1)

        return term


    def set_entry_term(self, entry, term):
        if type(entry) is llist.dllistnode:     
            entry.value[2] = term

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-1, term)

    
    def get_entry_logIndex(self, entry):
        if type(entry) is llist.dllistnode:
            logIndex = entry.value[1]
        
        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            logIndex = utils.decode_int_bytearray(entry, buf_len-16, buf_len-9)
        
        return logIndex


    def set_entry_logIndex(self, entry, logIndex):
        if type(entry) is llist.dllistnode:
            entry.value[1] = logIndex
        
        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-9, logIndex)


    def get_entry_isCommitted(self, entry):
        if type(entry) is llist.dllistnode:
            return entry.value[3]


    def set_entry_isCommited(self, entry, isCommitted):
        if type(entry) is llist.dllistnode:
            entry.value[3] = isCommitted
        

    def get_entry_prevLogIndex(self, entry):
        if type(entry) is llist.dllistnode:     
            prevLogIndex = entry.value[4]

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            prevLogIndex = utils.decode_int_bytearray(entry, buf_len-24, buf_len-17)

        return prevLogIndex


    def set_entry_prevLogIndex(self, entry, prevLogIndex):
        if type(entry) is llist.dllistnode:     
            entry.value[4] = prevLogIndex

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-17, prevLogIndex)


    def get_entry_prevLogTerm(self, entry):
        if type(entry) is llist.dllistnode:     
            prevLogTerm = entry.value[5]

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            prevLogTerm = utils.decode_int_bytearray(entry, buf_len-32, buf_len-25)

        return prevLogTerm


    def set_entry_prevLogTerm(self, entry, prevLogTerm):
        if type(entry) is llist.dllistnode:     
            entry.value[5] = prevLogTerm

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-25, prevLogTerm)


    def get_entry_commitIndex(self, entry):
        if type(entry) is llist.dllistnode:     
            commitIndex = entry.value[6]

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            commitIndex = utils.decode_int_bytearray(entry, buf_len-40, buf_len-33)

        return commitIndex


    def set_entry_commitIndex(self, entry, commitIndex):
        if type(entry) is llist.dllistnode:     
            entry.value[6] = commitIndex

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-33, commitIndex)


    def get_entry_leaderID(self, entry):
        if type(entry) is llist.dllistnode:     
            leaderID = entry.value[7]

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            leaderID = utils.decode_int_bytearray(entry, buf_len-48, buf_len-41)

        return leaderID


    def set_entry_leaderID(self, entry, leaderID):
        # leaderID is just serverID
        if type(entry) is llist.dllistnode:     
            entry.value[7] = leaderID

        elif type(entry) is memoryview or type(entry) is bytes:
            buf_len = len(entry)
            utils.encode_int_bytearray(entry, buf_len-41, leaderID)


    def set_entry_params(self, shm, params):

        logIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID = params
        
        if type(shm) is memoryview or type(shm) is llist.dllistnode or type(shm) is bytes:
            buf = shm
        else:
            buf = shm.buf

        self.set_entry_logIndex(buf, logIndex)
        self.set_entry_term(buf, term)
        self.set_entry_prevLogIndex(buf, prevLogIndex)
        self.set_entry_prevLogTerm(buf, prevLogTerm)
        self.set_entry_commitIndex(buf, commitIndex)
        self.set_entry_leaderID(buf, leaderID)


    def get_entry_params(self, shm):

        if type(shm) is memoryview or type(shm) is llist.dllistnode or type(shm) is bytes:
            buf = shm
        else:
            buf = shm.buf

        params = []
        params.append(self.get_entry_logIndex(buf))
        params.append(self.get_entry_term(buf))
        params.append(self.get_entry_prevLogIndex(buf))
        params.append(self.get_entry_prevLogTerm(buf))
        params.append(self.get_entry_commitIndex(buf))
        params.append(self.get_entry_leaderID(buf))

        return params


    def get_entry_data(self, shm, deserialize=True):

        if type(shm) is shared_memory.SharedMemory:
            buf = shm.buf
        elif type(shm) is memoryview or type(shm) is bytes:
            buf = shm

        if deserialize:
            data = self.deserializeEntry(buf[:-self.params_size])
        else:
            data = buf[:-self.params_size]

        return data


    def createLogEntry(self, func, args, params):
        """
        params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]

        """

        logEntry = [func.__name__, args]
        binaryLogEntry = self.serializeEntry(logEntry)
        shm_size = len(binaryLogEntry) + self.params_size
        shm = shared_memory.SharedMemory(create=True, size=shm_size)
        shm.buf[:len(binaryLogEntry)] = binaryLogEntry
        self.set_entry_params(shm, params)      
        # logIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID = params

        utils.freeSharedMemory(shm, clear=False)
        total_size = shm_size + 120

        if self.testMode:
            # _globals._print("Created SharedMemory:", shm.name)
            pass

        return shm.name, total_size


    def addEntry(self, shm_name, params):

        logIndex, currentTerm, prevLogIndex, prevLogTerm, commitIndex, leaderID, shm_size, isCommitted = params
        node = llist.dllistnode((shm_name, logIndex, currentTerm, isCommitted, prevLogIndex, prevLogTerm, commitIndex, leaderID, shm_size))
        toClear = (not self.addNodeSize(shm_size)) or (self.log.size >= self.maxLength)

        if toClear:         
            self.clearLog()
            self.addNodeSize(shm_size)

        self.log.append(node)

        if self.testMode:
            logger.log_addEntry(node, logIndex)


    def addEntry_follower(self, msg, commitIndex):

        params = self.get_entry_params(msg)
        logIdx = params[0]
        shm_size = len(msg)
        total_size = shm_size + 120
        isCommitted = commitIndex >= params[0]
        params += [total_size, isCommitted]

        shm = shared_memory.SharedMemory(create=True, size=shm_size)
        shm.buf[:] = msg

        self.addEntry(shm.name, params)
        utils.freeSharedMemory(shm, clear=False)

        return shm.name, logIdx

    # def addEntryRaw(self, logIndex, term, func, args, isCommitted):
    #   """
    #   To add : prevLogIndex, prevLogTerm, commitIndex, leaderID
    #   """


    #   logEntry = [func, args]
    #   binaryLogEntry = self.serializeEntry(logEntry)
    #   shm_size = len(binaryLogEntry) + self.params_size
    #   shm = shared_memory.SharedMemory(create=True, size=shm_size)
    #   shm.buf[:len(binaryLogEntry)] = binaryLogEntry
    #   utils.encode_int_bytearray(shm.buf, shm_size-1, term)
    #   utils.encode_int_bytearray(shm.buf, shm_size-1-8, logIndex)
    #   utils.encode_int_bytearray(shm.buf, shm_size-1-16, prevLogIndex)
    #   utils.encode_int_bytearray(shm.buf, shm_size-1-24, prevLogTerm)
    #   utils.encode_int_bytearray(shm.buf, shm_size-1-32, commitIndex)
    #   utils.encode_int_bytearray(shm.buf, shm_size-1-40, leaderID)

    #   node = llist.dllistnode(shm.buf)
    #   node_size = utils.getObjectSize(node, set())
    #   toClear = (not self.addNodeSize(node_size)) or (self.log.size >= self.maxLength)

    #   if toClear:         
    #       self.clearLog()
    #       self.addNodeSize(node_size)

    #   self.log.append(node)




    def addEntrySerialized(self, binaryLogEntry, logIndex, term, isCommitted):
        """
        !!!CHANGE THIS LATER!!! This function will be called on the follower's side for adding a serialized log entry.
        """

        shm_size = len(binaryLogEntry) + self.params_size
        shm = shared_memory.SharedMemory(create=True, size=shm_size)
        shm.buf[:len(binaryLogEntry)] = binaryLogEntry
        utils.encode_int_bytearray(shm.buf, shm_size-1, term)
        utils.encode_int_bytearray(shm.buf, shm_size-1-8, logIndex)
        shm.buf[shm_size-17] = isCommitted
        node = llist.dllistnode(shm.buf)
        node_size = utils.getObjectSize(node, set())
        toClear = (not self.addNodeSize(node_size)) or (self.log.size >= self.maxLength)

        if toClear:         
            self.clearLog()
            self.addNodeSize(node_size)

        self.log.append(node)



    def deleteEntry(self, entry, idx=None):

        if type(entry) is llist.dllistnode:
            if idx is None:
                idx = self.log.indexOf(entry)
            _, smallestPtrIdx = self.findSmallestPtr()
            if idx < smallestPtrIdx:
                if isinstance(entry.value, Iterable):
                    shm_name = entry.value[0]
                    utils.freeSharedMemory(shm_name)

                    if self.testMode:
                        _globals._print("Deleted Entry with logIdx:", self.get_entry_logIndex(entry))

                self.log.remove(entry)
                self.ref_point += 1
                self.deleteNodeSize(idx)

                return True
            else:
                return False
        else:
            return False


    def deleteAllPrevEntries(self, entry, idx=None):

        if type(entry) is not llist.dllistnode:
            return False
        else:
            if type(entry.prev) is llist.dllistnode:
                if idx is None:
                    idx = self.log.indexOf(entry)
                _, smallestPtrIdx = self.findSmallestPtr()
                if idx <= smallestPtrIdx:
                    self.ref_point += idx
                    self.deleteNodeSize(slice(0, idx))
                    while entry is not self.log.first:
                        if isinstance(self.log.first.value, Iterable):
                            shm_name = self.log.first.value[0]
                            utils.freeSharedMemory(shm_name) 

                            if self.testMode:
                                _globals._print("Deleted Entry with logIdx:", self.get_entry_logIndex(self.log.first))

                        self.log.remove(self.log.first)

        return True



    def deleteAllEntries(self):

        node = self.log.first
        while node is not None:
            if isinstance(node.value, Iterable):
                shm_name = node.value[0]
                utils.freeSharedMemory(shm_name) 

                if self.testMode:
                    _globals._print("Deleted Entry with logIdx:", self.get_entry_logIndex(node))

            next_node = node.next
            self.log.remove(node)
            node = next_node

        self.nodeSizes = []
        self.logSize = 0

    # def deleteAllPrevEntries(self, entry, idx=None):

    #   if type(entry) is not llist.dllistnode:
    #       return False
    #   else:
    #       if type(entry.prev) is llist.dllistnode:
    #           if idx is None:
    #               idx = self.log.indexOf(entry)
    #           _, smallestPtrIdx = self.findSmallestPtr()
    #           if idx <= smallestPtrIdx:
    #               self.log.clearLeft(entry.prev)
    #               self.ref_point += idx
    #               self.deleteNodeSize(slice(0, idx))

    #   return True


    # def deleteAllNextEntries(self, entry):
    #   # TODO. Don't update ref_point

    #   if type(entry) is not llist.dllistnode:
    #       return False
    #   else:
    #       if type(entry.next) is llist.dllistnode:
    #           self.log.clearRight(entry.next)

    #   return True


    def __len__(self):

        return self.log.size


    def jump(self, iteratorID, jump_steps, saveState=True, getNode=False):

        if iteratorID not in self.ptrs:
            return None

        if self.ptrs[iteratorID][0] is None:
            return None

        iteratorIdx = self.ptrs[iteratorID][1] - self.ref_point
        jumpIdx = iteratorIdx + jump_steps

        if not 0 <= jumpIdx <= self.log.size - 1:
            return None

        if jump_steps == 0:
            if getNode:
                return self.ptrs[iteratorID][0]
            else:
                return self.ptrs[iteratorID][0].value

        if saveState:
            self.ptrs[iteratorID][0] = self.log.nodeat(jumpIdx)
            self.ptrs[iteratorID][1] += jump_steps
            if getNode:
                return self.ptrs[iteratorID][0]
            else:
                return self.ptrs[iteratorID][0].value
        else:
            if getNode:
                return self.log.nodeat(jumpIdx)
            else:
                return self.log.nodeat(jumpIdx).value


    def next(self, iteratorID, saveState=True, getNode=False):

        if iteratorID not in self.ptrs:
            return None

        if self.ptrs[iteratorID][0] is None:
            return None

        next_node = self.ptrs[iteratorID][0].next
        if next_node is None:
            return None

        if saveState:
            self.ptrs[iteratorID][0] = next_node
            self.ptrs[iteratorID][1] += 1

        if getNode:
            return next_node
        else:
            return next_node.value

    
    def current(self, iteratorID, getNode=False):

        if iteratorID in self.ptrs:
            if self.ptrs[iteratorID][0] is None:
                return None
            else:
                if getNode:
                    return self.ptrs[iteratorID][0]
                else:
                    return self.ptrs[iteratorID][0].value
        else:
            return None

    
    def prev(self, iteratorID, saveState=True, getNode=False):

        if iteratorID not in self.ptrs:
            return None

        if self.ptrs[iteratorID][0] is None:
            return None

        prev_node = self.ptrs[iteratorID][0].prev

        if prev_node is None or prev_node.value == 0:
            return None

        if saveState:
            self.ptrs[iteratorID][0] = prev_node
            self.ptrs[iteratorID][1] -= 1

        if getNode:
            return prev_node
        else:
            return prev_node.value

    
    def get_lastEntry(self):

        if type(self.log.last) is llist.dllistnode:
            return self.last.log
        else:
            return None

    
    def get_prevEntry(self, entry):

        if type(entry) is not llist.dllistnode:
            return None

        prev_entry = entry.prev
        if type(prev_entry) is llist.dllistnode:
            return prev_entry
        else:
            return None


    def iteratorHalted(self, iteratorID):

        if iteratorID not in self.ptrs:
            return None

        if self.ptrs[iteratorID][0] is None:
            return True

        if self.ptrs[iteratorID][0].next is None:
            return True
        else:
            return False


    # def twoWay_Iterator(self):

    #   run = True
    #   while run:
    #       # run, forward = yield
    #       forward = yield
    #       if forward:
    #           if 0 <= (self.ptr+1) <= len(self.log) - 1:
    #               self.ptr += 1
    #               yield self.log[self.ptr]
    #           else:
    #               yield False
    #       else:
    #           if 0 <= (self.ptr-1) <= len(self.log) - 1:
    #               self.ptr -= 1
    #               yield self.log[self.ptr]
    #           else:
    #               yield False


