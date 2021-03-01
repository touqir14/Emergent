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
from _globals import NO_OP
import logger





def dummy_stateNotifier(state, serverID=None):
    if serverID is not None:
        _globals._print("Server:", serverID, ".State changed to", state)        
    else:
        _globals._print("State changed to", state)

def dummy_executeNotifier(cmdID, serverID=None):
    if serverID is not None:
        _globals._print("Server:", serverID, ".Executed: CommandID -", cmdID)        
    else:
        _globals._print("Executed: CommandID -", cmdID)


class RaftLauncher:

    def __init__(self, param_path, testMode=False):

        pipe_a, pipe_b = multiprocessing.Pipe()
        msgQueue = multiprocessing.Queue()
        self.testMode = testMode
        self.raft = RaftServer(pipe_a, msgQueue, testMode)
        self.loadParams(param_path)
        self.raft.initLog()
        # self.raft.initEverything(testMode)
        notifiers = {'stateNotifier':dummy_stateNotifier, 'executeNotifier':dummy_executeNotifier}
        self.stateMachine = StateMachine(pipe_b, msgQueue, notifiers, self.raft.serverID, testMode)
        # self.launchProcesses()

    def loadParams(self, param_path):
        
        raftParams = Namespace(**run_path(param_path)).raftParams
        for k in raftParams:
            if k in self.raft.__dict__:
                self.raft.__dict__[k] = raftParams[k]


    def launchProcesses(self, testkit=None):
        
        self.raftProcess = multiprocessing.Process(target=self.raft.start, args=(testkit,),)
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
        self.leaderID = None
        self.port = None
        self.timeoutRange = [1000000,2000000]
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
        self.electionTimer = None
        self.recentVotes = {} # Indexed by electionID
        self.currentElectionID = 0
        self.hasVoted = set()
        self.termination = False
        self.nextTimeout = None
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
        self.log = Log(self.logMaxSize, self.logMaxLength, self.clearLog_event, self.testMode, self.serverID)
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

        appendEntries_event = multiprocessing.Event()
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
            p = multiprocessing.Process(target=manager.start, args=(self.listenMsgQueue, pipe_b, pipes_out, i, appendEntries_event,),)
            p.name = processName
            self.workerProcesses.append(p)
            self.processPipes[p.name].append(pipe_a)
            p.start()

        self.assignIteratorToWorker()


    def initServers(self):

        params = {'commitIndex':self.commitIndex, 'currentTerm':self.currentTerm, 'state':self.state}

        for process_name in self.processPipes:
            mainPipe = self.processPipes[process_name][-1]
            iteratorIDs = self.processToIteratorIDs[process_name]
            servers = {}
            for iteratorID in iteratorIDs:
                servers[iteratorID] = self.servers[iteratorID]
            mainPipe.send(['updateParams', params])
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


    def initState(self):

        params = {}
        params['lastLogIndex'] = self.lastLogIndex
        params['lastLogTerm'] = self.lastLogTerm
        params['currentTerm'] = self.currentTerm
        params['prevLogIndex'] = self.prevLogIndex
        params['prevLogTerm'] = self.prevLogTerm
        params['commitIndex'] = self.commitIndex
        params['leaderID'] = self.leaderID
        self.stateMachinePipe.send(['newState', self.state, params])


    def start(self, testkit):

        if type(testkit) is TestKit:
            self.testkit = testkit
            self.testkit.initListenThread()
        else:
            self.testkit = None

        self.initEverything()
        self.initState()
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
                if followerTerm > self.currentTerm:
                    self.leaderToFollower(followerTerm) 

            elif msg[0] == 'successfulReplication':
                logIndex, term, iteratorID = msg[1], msg[2], msg[3]
                self.successfulReplication_handler(logIndex, term, iteratorID)

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
                pipe_name = msg[1]
                processName, pipe_idx = pipe_name[0], pipe_name[1]
                pipe = self.processPipes[processName][pipe_idx]
                pipe.send(['terminate'])

            elif msg[0] == 'RequestVote':
                numVotesReceived, numVotesRequested, term, electionID = msg[1], msg[2], msg[3], msg[4]
                self.requestVoteRPC_handler(numVotesReceived, numVotesRequested, term, electionID)

            else:
                _globals._print("Wrong command - {0}: From RaftServer.listen_loop".format(msg[0]))



    def successfulReplication_handler(self, logIndex, term, iteratorID):

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
            if self.currentTerm == term and self.commitIndex < logIndex: 
                self.updateParams({'commitIndex' : logIndex})
                self.log.updateCommitIndex(logIndex)
                self.stateMachinePipe.send(['executeLogEntry', logIndex])
                if self.testMode:
                    logger.log_committed(logIndex, self.replicationHistory[logIndex])

            self.replicationHistory[logIndex] = None # Make sure to clean up self.replicationHistory variable!




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
            
                    if self.testMode:
                        _globals._print("fetchLogEntry...", "iteratorID:",iteratorID,"command:",command)
            
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

                    if self.testMode:
                        _globals._print("fetchLogEntry...", "iteratorID:",iteratorID,"command:",command)

                else:
                    shm_names.append(None)
                    break

        if len(shm_names) > 0:
            processName, pipe_idx = pipe_name[0], pipe_name[1]
            pipe = self.processPipes[processName][pipe_idx]
            pipe.send([shm_names, logIndices, terms, flags])

        if flags[0] > 0:
            self.fetchPendingLogQueue.put([command, iteratorID, 1, pipe_name])



    def entryAdd_loop(self):

        while True:
            msg = self.entryQueue.get()
            if msg == ['terminate']:
                _globals._print("From Raft Class: entryAdd_loop terminating")
                break
            
            shm_name, params, entry_size = msg
            if params[1] < self.currentTerm or self.state != 'leader': # Not adding an entry from earlier term
                utils.freeSharedMemory(shm_name)
                self.stateMachinePipe.send(['addLogEntry-ACK'])
            else:
                self.addLogEntry(shm_name, params, entry_size)
                self.stateMachinePipe.send(['addLogEntry-ACK'])
                self.processPendingEntries()

    
    def addLogEntry(self, shm_name, params, entry_size):

        params += [entry_size, False] # Adding isCommitted=False.
        new_params = {}
        new_params['lastLogIndex'] = params[0]
        new_params['lastLogTerm'] = params[1]
        new_params['prevLogIndex'] = params[2]
        new_params['prevLogTerm'] = params[3]
        self.updateParams(new_params)
        self.log.addEntry(shm_name, params)


    def processPendingEntries(self):

        while not self.fetchPendingLogQueue.empty():
            command, iteratorID, numEntries, pipe_name = self.fetchPendingLogQueue.get()
            self.fetchLogEntry(command, iteratorID, numEntries, pipe_name)


    def start_server(self):

        self.connector = Connector(self.serverID, self.protocol, None, testMode=self.testMode)
        socket = self.connector.createServerSocket(self.port)
        self.connector.server_listen(socket, self.serverRequest_handler)


    def updateParams(self, params):

        sendToComs = {}
        options = {'commitIndex', 'currentTerm', 'state'}
        addToComs = True

        for k in params:
            addToComs = True
            if k == 'currentTerm': 
                if self.currentTerm < params[k]:
                    self.currentTerm = params[k]
                else:
                    addToComs = False

            elif k in self.__dict__:
                self.__dict__[k] = params[k]

            if k in options and addToComs:
                sendToComs[k] = params[k]

        if sendToComs != {}:
            for p_name in self.processPipes:
                mainPipe = self.processPipes[p_name][-1]
                mainPipe.send(['updateParams', sendToComs])


    def serverRequest_handler(self, data):
        """
        For AppendEntriesRPC replies, RESP is expected to be ['appendEntriesRPC_reply', Success, Term, transactionID]:
        For RequestVoteRPC replies, RESP is expected to be ['requestVoteRPC_reply', term, voteGranted, electionID] 
        """

        clientID, msgType, msg = data[:3]

        if msgType == b'AppendEntries':
            extra_params = msgpack.unpackb(data[3])
            cmd, RESP = self.appendEntriesReq_handler(msg, extra_params)
            if extra_params['term'] >= self.currentTerm and extra_params['leaderCommitIndex'] > self.commitIndex:
                self.executeCommittedEntries(self.commitIndex, extra_params['leaderCommitIndex'])
            return cmd, RESP

        elif msgType == b'Heartbeat':
            extra_params = msgpack.unpackb(data[3])
            self.heartBeatReq_handler(extra_params)    
            return 'no_reply', None

        elif msgType == b'RequestVote':
            cmd, RESP = self.requestVoteReq_handler(msg)
            return cmd, RESP

        else:
            return 'no_reply', None


    def requestVoteReq_handler(self, msg_bin):
        """
        RESP is expected to be ['requestVoteRPC_reply', term, voteGranted, electionID]
        """

        msg = msgpack.unpackb(msg_bin)
        term_, candidateID_, lastLogIndex_, lastLogTerm_, electionID = msg['term'], msg['candidateID'], msg['lastLogIndex'], msg['lastLogTerm'], msg['electionID']
        prevTerm = None

        if term_ < self.currentTerm:
            RESP = msgpack.packb(['requestVoteRPC_reply', self.currentTerm, False, electionID])
            return 'reply', RESP
        else:
            prevTerm = self.currentTerm
            if self.state == 'leader' and self.currentTerm == term_:
                self.hasVoted.add(term_)
            if self.state == 'leader' and self.currentTerm < term_:
                # self.leaderToFollower(term_)
                self.leaderToFollower(self.currentTerm)
            # if self.state == 'candidate' and self.currentTerm < term_:
            #     self.updateParams({'state' : 'follower'})
            #     self.reset_electionTimer()
            #     if self.testMode:
            #         _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower","currentTerm:",self.currentTerm,"Incoming Term:", term_)


        if term_ in self.hasVoted:
            RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])
            return 'reply', RESP

        if self.testMode:
            _globals._print("From requestVoteReq_handler. self.lastLogTerm:",self.lastLogTerm,"..self.lastLogIndex:",self.lastLogIndex,"..candidateID:", candidateID_,"..ElectionID:",electionID)

        if self.lastLogTerm < lastLogTerm_:
            RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, True, electionID])
            self.hasVoted.add(term_)
            if self.state == 'candidate':
                self.updateParams({'state' : 'follower'})                            
                # self.reset_electionTimer()
                if self.testMode:
                    _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower","currentTerm:",self.currentTerm,"Incoming Term:", term_)
            self.reset_electionTimer()
            self.updateParams({'currentTerm' : term_})    
            _globals._print("server:", self.serverID, "voting for candidate:", candidateID_)

        elif self.lastLogTerm == lastLogTerm_:
            if self.lastLogIndex <= lastLogIndex_:
                RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, True, electionID])
                self.hasVoted.add(term_)
                if self.state == 'candidate':
                    self.updateParams({'state' : 'follower'})                            
                    # self.reset_electionTimer()
                    if self.testMode:
                        _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower","currentTerm:",self.currentTerm,"Incoming Term:", term_)
                self.reset_electionTimer()
                self.updateParams({'currentTerm' : term_})    
                _globals._print("server:", self.serverID, "voting for candidate:", candidateID_)
            else:
                RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])
        
        else:
            RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])

        return 'reply', RESP


    # def requestVoteReq_handler(self, msg_bin):
    #     """
    #     RESP is expected to be ['requestVoteRPC_reply', term, voteGranted, electionID]
    #     """

    #     msg = msgpack.unpackb(msg_bin)
    #     term_, candidateID_, lastLogIndex_, lastLogTerm_, electionID = msg['term'], msg['candidateID'], msg['lastLogIndex'], msg['lastLogTerm'], msg['electionID']
    #     prevTerm = None

    #     if term_ < self.currentTerm:
    #         RESP = msgpack.packb(['requestVoteRPC_reply', self.currentTerm, False, electionID])
    #         return 'reply', RESP
    #     else:
    #         prevTerm = self.currentTerm
    #         if self.state == 'leader' and self.currentTerm == term_:
    #             self.hasVoted.add(term_)
    #         if self.state == 'leader' and self.currentTerm < term_:
    #             # self.leaderToFollower(term_)
    #             self.leaderToFollower(self.currentTerm)
    #         if self.state == 'candidate' and self.currentTerm < term_:
    #             self.updateParams({'state' : 'follower'})
    #             self.reset_electionTimer()
    #             if self.testMode:
    #                 _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower","currentTerm:",self.currentTerm,"Incoming Term:", term_)

    #         # self.updateParams({'currentTerm' : term_})    

    #     if term_ in self.hasVoted:
    #         RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])
    #         return 'reply', RESP

    #     if self.testMode:
    #         _globals._print("From requestVoteReq_handler. self.lastLogTerm:",self.lastLogTerm,"..self.lastLogIndex:",self.lastLogIndex,"..candidateID:", candidateID_,"..ElectionID:",electionID)

    #     if self.lastLogTerm < lastLogTerm_:
    #         RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, True, electionID])
    #         self.hasVoted.add(term_)
        
    #     elif self.lastLogTerm == lastLogTerm_:
    #         if self.lastLogIndex <= lastLogIndex_:
    #             RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, True, electionID])
    #             self.hasVoted.add(term_)
    #         else:
    #             RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])
        
    #     else:
    #         RESP = msgpack.packb(['requestVoteRPC_reply', prevTerm, False, electionID])

    #     return 'reply', RESP


    def heartBeatReq_handler(self, msg):

        if msg['term'] < self.currentTerm:
            return
        else:
            if self.state == 'leader':
                # by default leaderTerm > self.currentTerm since there can be only 1 leader in each term
                self.leaderToFollower(msg['term'])
            else:
                self.updateParams({'currentTerm' : msg['term']})

        if self.state == 'follower':
            self.reset_electionTimer()

        if self.state == 'candidate': 
            # Automatically implies that extra_params['term'] >= self.currentTerm.
            # So, transition to the follower state
            self.updateParams({'state' : 'follower'})
            self.reset_electionTimer()
            if self.testMode:
                _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower")


        leaderID_ = msg['leaderID']
        if msg['leaderCommitIndex'] > self.commitIndex and self.leaderID == leaderID_:
            # Only execute this if heartbeat does not come from a brand new leader
            self.executeCommittedEntries(self.commitIndex, msg['leaderCommitIndex'])

        if leaderID_ != self.leaderID:
            self.updateParams({'leaderID' : leaderID_})
            self.hasVoted.clear()
            if self.testMode:
                _globals._print("Server:", self.serverID,"Updated to LeaderID:",leaderID_)


        return


    def appendEntriesReq_handler(self, msg, extra_params):

        leaderCommitIndex, leaderTerm, transactionID = extra_params['leaderCommitIndex'], extra_params['term'], extra_params['transactionID']
        prevTerm = None
        if leaderTerm < self.currentTerm:
            RESP = msgpack.packb(['appendEntriesRPC_reply', False, self.currentTerm, transactionID])
            return 'reply', RESP
        else:
            prevTerm = self.currentTerm
            if self.state == 'leader':
                # by default leaderTerm > self.currentTerm since there can be only 1 leader in each term
                self.leaderToFollower(leaderTerm) 
            else:
                self.updateParams({'currentTerm' : leaderTerm})

        if self.state == 'follower':
            self.reset_electionTimer()

        if self.state == 'candidate': 
            # Automatically implies that extra_params['term'] >= self.currentTerm.
            # So, transition to the follower state
            self.updateParams({'state' : 'follower'})
            self.reset_electionTimer()
            if self.testMode:
                _globals._print("Server:", self.serverID,"Transitioned Candidate -> Follower")


        if extra_params['leaderID'] != self.leaderID:
            self.updateParams({'leaderID' : extra_params['leaderID']})
            self.hasVoted.clear()
            if self.testMode:
                _globals._print("Server:", self.serverID,"Updated to LeaderID:", extra_params['leaderID'])

        lastEntry = self.log.get_lastEntry()
        if lastEntry is not None:
            lastLogIdx, lastLogTerm = self.log.get_entry_logIndex(lastEntry), self.log.get_entry_term(lastEntry) 
            lastLogTerm_ , lastLogIdx_ = self.log.get_entry_term(msg), self.log.get_entry_logIndex(msg) 

            if lastLogTerm == lastLogTerm_ and lastLogIdx == lastLogIdx_:
                _globals._print('ignore')
                RESP = msgpack.packb(['appendEntriesRPC_reply', True, prevTerm, transactionID])
                return 'reply', RESP

            prevLogTerm_ , prevLogIdx_ = self.log.get_entry_prevLogTerm(msg), self.log.get_entry_prevLogIndex(msg)
            result, node = self.log.findEntry(prevLogIdx_, prevLogTerm_, 'Index&Term')

            if result == 'found_last':
                _globals._print('found_last')
                shm_name, logIdx, params = self.log.addEntry_follower(msg, leaderCommitIndex)
                new_params = {}
                new_params['lastLogIndex'] = params[0]
                new_params['lastLogTerm'] = params[1]
                new_params['prevLogIndex'] = params[2]
                new_params['prevLogTerm'] = params[3]
                self.updateParams(new_params)
                RESP = msgpack.packb(['appendEntriesRPC_reply', True, prevTerm, transactionID])
                return 'reply', RESP

            elif result == 'found':
                _globals._print('found')
                self.log.deleteNextEntries_follower(node)
                shm_name, logIdx, params = self.log.addEntry_follower(msg, leaderCommitIndex)
                new_params = {}
                new_params['lastLogIndex'] = params[0]
                new_params['lastLogTerm'] = params[1]
                new_params['prevLogIndex'] = params[2]
                new_params['prevLogTerm'] = params[3]
                self.updateParams(new_params)
                RESP = msgpack.packb(['appendEntriesRPC_reply', True, prevTerm, transactionID])
                return 'reply', RESP

            elif result == 'not_found':
                _globals._print('not_found')
                RESP = msgpack.packb(['appendEntriesRPC_reply', False, prevTerm, transactionID])
                return 'reply', RESP

        else:
            prevLogTerm_ , prevLogIdx_ = self.log.get_entry_prevLogTerm(msg), self.log.get_entry_prevLogIndex(msg)
            if self.lastLogIndex == prevLogIdx_ and self.lastLogTerm == prevLogTerm_:
                # _globals._print(4)
                shm_name, logIdx, params = self.log.addEntry_follower(msg, leaderCommitIndex)
                new_params = {}
                new_params['lastLogIndex'] = params[0]
                new_params['lastLogTerm'] = params[1]
                new_params['prevLogIndex'] = params[2]
                new_params['prevLogTerm'] = params[3]
                self.updateParams(new_params)
                RESP = msgpack.packb(['appendEntriesRPC_reply', True, prevTerm, transactionID])
                return 'reply', RESP

            else:
                # _globals._print(5)
                RESP = msgpack.packb(['appendEntriesRPC_reply', False, prevTerm, transactionID])
                return 'reply', RESP



    def executeCommittedEntries(self, old_commitIndex, leaderCommitIndex):
        # This function is called when leaderCommitIndex is greater than self.commitIndex

        sendForExecution = []
        new_commitIndex = min(self.lastLogIndex, leaderCommitIndex)
        iteratorID = 'commitIndex'

        if new_commitIndex > old_commitIndex:        
            while True:
                node = self.log.next(iteratorID, getNode=True)
                logIdx = self.log.get_entry_logIndex(node)
                sendForExecution.append(node.value[0])
                if logIdx >= new_commitIndex:
                    break

            self.updateParams({'commitIndex' : new_commitIndex})
            self.stateMachinePipe.send(['executeLogEntry', self.commitIndex, sendForExecution])


    def candidateToLeader(self):
        # Set ptrs for all followers to the last log entry.
        # Make sure after that the COMs objects send 'next' requests to the raft servers
        # Then create and append a no_op log entry
        # Then reactivate the SM object by changing its state and updating the params.
        # The first appendEntriesRPC after changing to a leader won't be a heartbeat - it will append no-op entry

        self.log.set_followerPtrsToLast()

        params = {}
        params['state'] = 'leader'
        params['leaderID'] = self.serverID
        self.updateParams(params)

        params = {}
        params['lastLogIndex'] = self.lastLogIndex
        params['lastLogTerm'] = self.lastLogTerm
        params['currentTerm'] = self.currentTerm
        params['prevLogIndex'] = self.prevLogIndex
        params['prevLogTerm'] = self.prevLogTerm
        params['commitIndex'] = self.commitIndex
        params['leaderID'] = self.leaderID
        self.stateMachinePipe.send(['newState', 'leader', params])

        if self.testMode:
            _globals._print("Server:", self.serverID,"Transitioned Candidate -> Leader. currentTerm:", self.currentTerm)

 
    def leaderToFollower(self, followerTerm):

        params = {}
        params['currentTerm'] = followerTerm
        params['state'] = 'follower'
        self.updateParams(params)
        self.stateMachinePipe.send(['newState', 'follower', {}])
        self.reset_electionTimer(startTimer=True)

        if self.testMode:
            _globals._print("Server:", self.serverID,"Transitioned Leader -> Follower")


    def candidateRunner(self):

        if self.termination:
            return

        params = {}
        params['currentTerm'] = self.currentTerm + 1
        params['currentElectionID'] = self.currentElectionID + 1
        if self.state != 'candidate':
            params['state'] = 'candidate'
            self.reset_electionTimer(startTimer=False)

        self.updateParams(params)
        self.hasVoted.add(self.currentTerm)
        self.recentVotes[self.currentElectionID] = [0, self.numServers-1]        
        electionTimeout = random.uniform(self.timeoutRange[0], self.timeoutRange[1])
        self.nextTimeout = electionTimeout / 1000 + time.time()
        msg = {}
        msg['term'] = self.currentTerm
        msg['candidateID'] = self.serverID
        msg['lastLogIndex'] = self.lastLogIndex
        msg['lastLogTerm'] = self.lastLogTerm

        for process_name in self.processPipes:
            mainPipe = self.processPipes[process_name][-1]
            mainPipe.send(['requestVoteRPC', msg, electionTimeout, self.currentElectionID])

        if self.testMode:
            _globals._print_lines(["Server:", self.serverID, ".Started Election:", self.currentElectionID],[msg])



    def requestVoteRPC_handler(self, numVotesReceived, numVotesRequested, term, electionID):

        # if self.testMode:
        #     _globals._print("Server:", self.serverID, "requestVoteRPC_handler is invoked")

        params = {}
        params['currentTerm'] = term
        self.updateParams(params)
        if electionID not in self.recentVotes:
            return
        if self.state != 'candidate':
            del self.recentVotes[electionID]
            return

        self.recentVotes[electionID][0] += numVotesReceived
        self.recentVotes[electionID][1] -= numVotesRequested

        if self.recentVotes[electionID][1] == 0: # When complete election result is available
            if self.testMode:
                _globals._print("Server:", self.serverID,"Vote List with electionID:",electionID,"is",self.recentVotes[electionID])

            if self.recentVotes[electionID][0] >= self.numMajorityServers:
                self.candidateToLeader()
            else:
                time.sleep(max(0, self.nextTimeout - time.time()))
                self.candidateRunner()

            del self.recentVotes[electionID]

        return 


    def reset_electionTimer(self, startTimer=True):
        """
        Use threading.Timer for setting a timer. 
        See https://stackoverflow.com/questions/24968311/how-to-set-a-timer-clear-a-timer-in-python
        threading.Timer.interval gives the duration.
        """
        if self.electionTimer is not None:
            self.electionTimer.cancel()

        if not self.termination and startTimer:
            timerDuration = random.uniform(self.timeoutRange[0], self.timeoutRange[1])
            self.electionTimer = threading.Timer(timerDuration/1000, self.candidateRunner)        
            self.electionTimer.start()

        return 


    def terminate(self):

        self.termination_event.wait()
        self.termination = True
        self.connector.terminateServer = True
        for p in self.workerProcesses:
            self.processPipes[p.name][-1].send(['terminate'])

        for p in self.workerProcesses:
            p.join()

        self.stateMachinePipe.send(['terminate'])
        self.listenMsgQueue.put(['terminate', False])
        self.entryQueue.put(['terminate'])
        self.listenerThread.join()
        self.entryAdderThread.join()
        self.serverThread.join()
        self.reset_electionTimer(startTimer=False)
        with _globals.print_lock:
            print("Printing Log Chain for Server:", self.serverID)
            logger.print_log_datachain(self.log)
        self.log.deleteAllEntries()

        _globals._print("Exiting RaftServer instance")
        return


class StateMachine:

    def __init__(self, pipe, msgQueue, notifiers, serverID, testMode=False):


        # params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]
        self.params = {}
        self.commandBuffer = {}
        self.msgQueue = msgQueue
        self.pipe = pipe
        self.log = Log(None, None, None, testMode)
        self.isLeader = None
        self.serverID = serverID
        self.testMode = testMode
        self.globalState = []
        # self.stateNotifier needs to be called to notify the native server that the state \in \{leader, follower, candidate\} has changed.
        self.loopThreadKill = False
        self.initNotifiers(notifiers)

        self.addEntryEvent = threading.Event()
        self.addEntryEvent.set()

        self.paramsLock = threading.Lock()
        self.executerQueue = queue.Queue()

        self.loopThread = threading.Thread(target=self.listen_loop,)
        self.loopThread.start()

        self.entryExecuterThread = threading.Thread(target=self.run_entryExecution)
        self.entryExecuterThread.start()


    def initNotifiers(self, notifiers):

        if 'stateNotifier' in notifiers:
            self.stateNotifier = notifiers['stateNotifier']
        else:
            self.stateNotifier = None

        if 'executeNotifier' in notifiers:
            self.executeNotifier = notifiers['executeNotifier']
        else:
            self.executeNotifier = None


    def terminate(self):

        self.loopThreadKill = True
        self.msgQueue.put(['terminate', True])
        self.executerQueue.put(['terminate'])
        self.loopThread.join()

        if self.testMode:
            _globals._print_lines(['SM of server:', self.serverID], ['globalState:', self.globalState])


    def updateLogParams(self, params):

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
            if self.isLeader == False or self.isLeader is None:
                self.isLeader = True
                self.addEntryEvent.set()
                self.executeCommand(NO_OP, [], None)
                self.stateNotifier(newState, self.serverID)

        elif newState == 'follower' or newState == 'candidate':
            if self.isLeader == True or self.isLeader is None:
                self.isLeader = False
                self.commandBuffer = {}
                self.stateNotifier(newState, self.serverID)
        # elif newState == 'candidate':
        #   pass


    def executeCommand(self, func, args, commandID):
        """
        When a Raft instance shifts transforms into a leader, how should the native server know it 
        """

        self.addEntryEvent.wait()
        self.addEntryEvent.clear()

        if not self.isLeader:
            self.addEntryEvent.set()
            return False

        if self.loopThreadKill:
            return False

        with self.paramsLock:
            self.params['prevLogIndex'] = self.params['lastLogIndex']
            prevLogIndex = self.params['prevLogIndex']
            self.params['lastLogIndex'] += 1 
            lastLogIndex = self.params['lastLogIndex']
            self.params['prevLogTerm'] = self.params['lastLogTerm'] 
            prevLogTerm = self.params['prevLogTerm']
            self.params['lastLogTerm'] = self.params['currentTerm']
            term = self.params['lastLogTerm']
            commitIndex = self.params['commitIndex']
            leaderID = self.params['leaderID']

        #params[1] must be lastLogTerm
        params = [lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID]
        self.commandBuffer[lastLogIndex] = [func, args, commandID]
        shm_name, total_size = self.log.createLogEntry(func, args, params)
        # time.sleep(12)
        self.msgQueue.put(('addLogEntry', shm_name, params, total_size))

        return True


    def run_entryExecution(self):

        while True:
            msg = self.executerQueue.get() # 1 second timeout
            
            if msg[0] == 'terminate':
                break

            if len(msg) <= 2:
                self.executeLogEntry()
            else:
                self.loadExecuteLogEntry(msg[2]) # msg[2]: a list of shm_names                    



    def executeLogEntry(self):

        if self.commandBuffer == {}:
            return

        keys = sorted(self.commandBuffer.keys())
        for k in keys:
            if k > self.params['commitIndex']:
                break

            func, args, commandID = self.commandBuffer.pop(k)
            result = func(*args)
            
            if self.executeNotifier is not None:
                if commandID is not None:
                    self.executeNotifier(commandID, self.serverID)

            if self.testMode:
                logger.log_entryExecution_leader(k)
                self.globalState.append(result)


    def loadExecuteLogEntry(self, shm_names):

        for name in shm_names:
            shm = utils.loadSharedMemory(name)
            if shm == False:
                continue
            func_name, args = self.log.get_entry_data(shm)
            logIdx = self.log.get_entry_logIndex(shm.buf)
            execute_str = '{0}(*args)'.format(func_name)
            result = eval(execute_str)
            utils.freeSharedMemory(shm, clear=False) 

            if self.testMode:
                logger.log_entryExecution_follower(logIdx)
                self.globalState.append(result)
                if result is not None:
                    self.executeNotifier(logIdx, self.serverID)

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
                self.updateLogParams({'commitIndex':commitIndex})
                self.executerQueue.put(msg)

            elif msg[0] == 'addLogEntry-ACK':
                self.addEntryEvent.set()

            elif msg[0] == 'terminate':
                _globals._print("From StateMachine class: listen_loop terminating")
                break

            elif msg[0] == 'resetParams':
                new_params = msg[1]
                self.updateLogParams(new_params)

            elif msg[0] == 'newState':
                new_state, new_params = msg[1], msg[2]
                self.updateLogParams(new_params)
                self.processStateChange(new_state)

            else:
                _globals._print("Wrong command - {0}: From StateMachine.listen_loop".format(msg[0]))



class Connector:

    def __init__(self, serverID, protocol, comm, curve_auth=False, cert_path=None, testMode=False):
        
        self.serverID = serverID
        self.protocol = protocol
        self.connectionTimeout = 1000 # in milliseconds. Maybe have different timeouts for connection and AppendRPCs ??????????
        self.connectionRetries = 3
        self.curve_auth = curve_auth
        self.cert_path = cert_path
        self.terminateServer = False
        self.terminateClient = False
        self.testMode = testMode
        self.heartbeat_interval = 1000 # in milliseconds
        self.socket_timers = {}
        self.client_socketLocks = {}
        self.client_transactionIDs = {}
        self.log = Log(None,None,None,True)

        if comm is not None:
            self.get_extraParams = comm.get_extraParams
            self.execute_appendEntriesRPC = comm.execute_appendEntriesRPC

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
        msgType = bytes('Connection Request', 'ascii')
        msg = bytes('Connection Request from Server: {0}'.format(self.serverID), 'ascii')
        resend = True
        i = 0

        with self.client_socketLocks[destServerID]:
            while i < trials:
                if resend:
                    socket.connect(addr)
                    socket.send_multipart([msgType, msg])

                try:
                    received = socket.recv()
                except Exception as e:
                    resend = True
                    i += 1
                    continue

                if received == b'Connection ACK':
                    success = True
                    break
                else:
                    resend = False

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
        socket.setsockopt(zmq.RCVHWM, 10**6)
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
        socket.setsockopt(zmq.RCVHWM, 10**6)
        socket.bind(addr)
        
        return socket


    def _createServerSocket_CURVE(self, servers, destServerID):

        """
        Uses Ironhouse encryption based on CURVE
        """
        pass


    def loadCertificates(self):
        pass


    def flushSocketBuffers(self, serverSockets):

        for v in serverSockets.values():
            socket, destServerID = v[0], v[3]
            with self.client_socketLocks[destServerID]:
                socket.RCVTIMEO = 0
                while True:
                    try:
                        socket.recv_multipart()
                    except Exception as e:
                        break
                socket.RCVTIMEO = self.connectionTimeout


    def server_listen(self, socket, RESP_handler):

        while not self.terminateServer:
            try:
                data = socket.recv_multipart() # data : [clientID, msgType, msg, extra_params]
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


    def requestVoteRPC_sendAll(self, params, serverSockets):

        msg = msgpack.packb(params, use_bin_type=True)
        msgType = b'RequestVote'
        numVotesRequested = len(serverSockets)
        for v in serverSockets.values():
            socket, destServerID = v[0], v[3]
            with self.client_socketLocks[destServerID]:
                socket.send_multipart([msgType, msg])

        return numVotesRequested


    def requestVoteRPC_recvAll(self, serverSockets, electionTimeout, electionID):

        endTime = (time.time() + electionTimeout / 1000)
        recv_all = {}
        for v in serverSockets.values():
            socket, destServerID = v[0], v[3]
            with self.client_socketLocks[destServerID]:
                delta = max(0, endTime - time.time())
                socket.RCVTIMEO = int(delta * 1000)
                while True:
                    try:
                        RESP_bin = socket.recv()
                    except Exception as e:
                        recv_all[destServerID] = None
                        break

                    if b'Connection ACK' in RESP_bin:
                        continue

                    # RESP : [MSG_TYPE, term, voteGranted, electionID]
                    RESP = msgpack.unpackb(RESP_bin)

                    if 'requestVoteRPC_reply' == RESP[0] and electionID == RESP[3]: 
                        recv_all[destServerID] = RESP
                        break

                socket.RCVTIMEO = self.connectionTimeout

        return recv_all


    def appendEntriesRPC(self, data, socket, destServerID, isHeartbeat=False, repetitions=True):

        # _globals._print("Within appendEntriesRPC")

        if type(data) is shared_memory.SharedMemory:
            msg = data.buf
            # _globals._print("Type of Msg:",type(msg))
        # else:
        #     _globals._print("Printing Alternative msg:", data)

        if not isHeartbeat:
            msgType = b'AppendEntries'
            self.client_transactionIDs[destServerID] += 1
            # _globals._print("prevLogTerm:", self.log.get_entry_prevLogTerm(msg), "prevLogIdx:", self.log.get_entry_prevLogIndex(msg))
            # if self.testMode:
            #     _globals._print("AppendRPC to server:",destServerID)
        else:
            msgType = b'Heartbeat'
            msg = bytes()
            # if self.testMode:
            #     _globals._print("Heartbeat to server:",destServerID)

        resend = True
        success = None
        followerTerm = None
        while (not self.terminateClient) and repetitions > 0:

            if type(repetitions) == int:
                repetitions =- 1

            if resend: 
                with self.client_socketLocks[destServerID]:
                    extra_params = self.get_extraParams(['leaderCommitIndex', 'term', 'leaderID'])
                    extra_params['transactionID'] = self.client_transactionIDs[destServerID]
                    extra_params_bin = msgpack.packb(extra_params, use_bin_type=True)
                    socket.send_multipart([msgType, msg, extra_params_bin])
                    self.reset_heartbeatTimer(destServerID)
                    if isHeartbeat:
                        break

            with self.client_socketLocks[destServerID]:
                try:
                    RESP_bin = socket.recv()
                    resend = False
                except Exception as e:
                    if self.testMode and not self.terminateClient: 
                        _globals._print("Retrying AppendRPC to server: {0} at address: {1}".format(destServerID, socket.LAST_ENDPOINT))
                    resend = True
                    continue

            if b'Connection ACK' in RESP_bin:
                continue

            RESP = msgpack.unpackb(RESP_bin)
            if 'appendEntriesRPC_reply' != RESP[0] : 
                continue

            success = RESP[1]
            followerTerm = RESP[2]
            RESP_transactionID = RESP[3]

            if RESP_transactionID < self.client_transactionIDs[destServerID]:
                _globals._print("skipping transactionID:", RESP_transactionID)
                continue

            if self.testMode:
                _globals._print("Received RESP from server: {0}, address: {1}. Success: {2}, followerTerm: {3}".format(destServerID, socket.LAST_ENDPOINT, success, followerTerm))

            break

        if not self.terminateClient:
            return success, followerTerm
        else:
            return None, None


    def reset_heartbeatTimer(self, destServerID):

        # _globals._print("within reset_heartbeatTimer for server:", destServerID)

        if destServerID not in self.socket_timers:
            return
            
        socket = self.socket_timers[destServerID][0]
        if self.socket_timers[destServerID][1] is not None:
            self.socket_timers[destServerID][1].cancel()
        args = [None, socket, destServerID, True, 1]
        timer = threading.Timer(self.heartbeat_interval/1000, self.execute_appendEntriesRPC, args)
        self.socket_timers[destServerID][1] = timer
        timer.start()



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
        self.connector = Connector(serverID, self.protocol, self, testMode=testMode)
        self.commitIndex = None
        self.currentTerm = None
        self.reconnect_interval = 60 * 10 * 1000 # 10 mins interval in milliseconds
        self.terminateAll = False
        self.reconnectTimer = None

        if testMode:
            self.testkit = TestKit(None, ComMan=self)



    def updateParams(self, params):

        for k in params:
            if k in self.__dict__:
                if k == 'state':
                    if params[k] == 'leader':
                        # self.connector.flushSocketBuffers(self.serverSockets)
                        self.appendEntries_event.set()
                        self.connector.terminateClient = False
                    else:
                        self.appendEntries_event.clear()
                        self.connector.terminateClient = True
                self.__dict__[k] = params[k]



    def start(self, msgQueue, pipe, threadPipes, procIdx, appendEntries_event):

        self.mainMsgQueue = msgQueue
        self.mainPipe = pipe
        self.threadPipes = threadPipes
        self.procIdx = procIdx
        self.appendEntries_event = appendEntries_event
        self.reset_reconnectTimer()
        self.listen_loop()


    def listen_loop(self):

        while True:
            msg = self.mainPipe.recv()
            if msg[0] == 'updateServer':
                data = msg[1]
                self.updateServerIterator(data)

            elif msg[0] == 'terminate':
                self.terminate()
                _globals._print("From CommunicationManager class: Terminating process")
                break

            elif msg[0] == 'testkit' and self.testkit is not None:
                self.testkit.processRequest(msg[1], {'procIdx':self.procIdx, 'mode':0})

            elif msg[0] == 'updateParams':
                params = msg[1]
                self.updateParams(params)

            elif msg[0] == 'requestVoteRPC':
                params, electionTimeout, electionID = msg[1], msg[2], msg[3]
                self.execute_requestVoteRPC(params, electionTimeout, electionID)

            else:
                _globals._print("Wrong command - {0}: From CommunicationManager.listen_loop".format(msg[0]))


    
    def terminate(self):

        self.terminateAll = True
        self.connector.terminateClient = True
        self.appendEntries_event.set()
        self.reset_reconnectTimer(startTimer=False)

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


    def reset_reconnectTimer(self, startTimer=True):

        if self.reconnectTimer is not None:
            self.reconnectTimer.cancel()

        if startTimer:
            self.reconnectTimer = threading.Timer(self.reconnect_interval/1000, self.reconnect)
            self.reconnectTimer.start()


    def reconnect(self):

        for v in self.serverSockets.values():
            socket, addr, destServerID = v[0], v[2], v[3]
            self.connector.connect(socket, addr, destServerID)
            v[4] = time.time()

        self.reset_reconnectTimer()


    def updateServer(self, servers):

        for serverID in servers:
            if serverID in self.servers:
                if not self.servers[serverID] == servers[serverID]:
                    oldSocket = self.serverSockets[serverID][0]
                    oldSocket.close()
                    self.servers[serverID] = servers[serverID]
                    self.connector.client_socketLocks[serverID] = threading.Lock()
                    self.connector.client_transactionIDs[serverID] = 0
                    socket, success, addr, destServerID = self.connector.createClientSocket(self.servers, serverID)
                    prevConnectionTime = time.time() 
                    self.serverSockets[serverID] = [socket, success, addr, destServerID, prevConnectionTime]
                    self.connector.socket_timers[serverID] = [socket, None]
            else:
                self.servers[serverID] = servers[serverID]
                self.connector.client_socketLocks[serverID] = threading.Lock()
                self.connector.client_transactionIDs[serverID] = 0
                socket, success, addr, destServerID = self.connector.createClientSocket(self.servers, serverID)
                prevConnectionTime = time.time() 
                self.serverSockets[serverID] = [socket, success, addr, destServerID, prevConnectionTime]
                self.connector.socket_timers[serverID] = [socket, None]

        toDelete = set(self.servers.keys()) - set(servers.keys())
        for serverID in toDelete:
            socket = self.serverSockets.pop(serverID)[0]
            socket.close()
            del self.servers[serverID]
            del self.connector.socket_timers[serverID]
            del self.connector.client_socketLocks[serverID]
            del self.connector.client_transactionIDs[serverID]


    def updateIterator(self, iteratorIDs):

        toDelete = set(self.iteratorThreads.keys()) - set(iteratorIDs)
        toAdd = set(iteratorIDs) - set(self.iteratorThreads.keys())

        for ID in toDelete:
            pipe = self.iteratorThreads[ID][1]
            self.threadPipes.append(pipe)
            self.mainMsgQueue.put(['stopThread', pipe.name])
            del self.iteratorThreads[ID]

        for ID in toAdd:
            pipe = self.threadPipes.pop()
            t = threading.Thread(target=self.replicateLogEntry, args=(ID,pipe,),)
            self.iteratorThreads[ID] = [t, pipe]
            t.start()



    def get_extraParams(self, args):

        extra_params = {}
        if 'leaderCommitIndex' in args:
            extra_params['leaderCommitIndex'] = self.commitIndex
        if 'term' in args:
            extra_params['term'] = self.currentTerm
        if 'leaderID' in args:
            extra_params['leaderID'] = self.serverID
        if 'candidateID' in args:
            extra_params['candidateID'] = self.serverID
        return extra_params


    def execute_requestVoteRPC(self, params, electionTimeout, electionID):

        """
        Whatever the intended electionTimeout is, subtract a small amount like 10ms to 
        take care of the overhead due of executing code for sending and receiving
        RPC messages mostly given below (and few other places).
        Eg. if intended timeout is 500ms, then set electionTimeout: 490ms.
        """

        self.reset_reconnectTimer()
        params['electionID'] = electionID
        numVotesRequested = self.connector.requestVoteRPC_sendAll(params, self.serverSockets)
        # time.sleep(electionTimeout/1000)
        recv_data = self.connector.requestVoteRPC_recvAll(self.serverSockets, electionTimeout, electionID)
        numVotesReceived = 0
        term = 0

        for serverID, RESP in recv_data.items():
            if RESP is not None:
                if RESP[2]: # if voteGranted is True
                    numVotesReceived += 1
                term = max(term, RESP[1])

        _globals._print("server:", self.serverID, "execute_requestVoteRPC.", "Voted:", numVotesReceived, "term:", term)
        self.mainMsgQueue.put(['RequestVote', numVotesReceived, numVotesRequested, term, electionID])



    def execute_appendEntriesRPC(self, datum, socket, iteratorID, isHeartbeat=False, repetitions=True):

        if type(datum) == list:
            shm, logIndex, term = datum

        else:
            shm, logIndex, term = None, None, None

        if self.state == 'leader':
            if self.testMode:
                _globals._print("AppendEntries to server:",iteratorID,"with logIdx:",logIndex,",term:",term)
            successful, followerTerm = self.connector.appendEntriesRPC(shm, socket, iteratorID, isHeartbeat, repetitions)

            if successful is None:
                return

            if isHeartbeat:
                return

            if self.currentTerm < followerTerm:
                self.updateParams({'state':'follower'})
                motion = 'forward'
            self.mainMsgQueue.put(['followerTerm', followerTerm, iteratorID])
            
            # elif self.currentTerm >= followerTerm and successful:
            #     motion = 'forward'
            #     self.mainMsgQueue.put(['successfulReplication', logIndex, term, iteratorID])

            # elif self.currentTerm >= followerTerm and not successful:
            #     motion = 'backward'

            if successful:
                motion = 'forward'
                self.mainMsgQueue.put(['successfulReplication', logIndex, term, iteratorID])
            else:
                motion = 'backward'
            return motion

        return 'forward'


    def replicateLogEntry(self, iteratorID, pipe):
        """
        Currently the logic that it uses works correctly for fetching 1 log entry at a time, ie ['next', 1]
        """

        motion = 'forward'
        pipe.flags = [0]

        while True:

            self.appendEntries_event.wait()

            if self.terminateAll or iteratorID not in self.iteratorThreads:
                break

            if motion == 'forward':
                if self.testMode:
                    _globals._print("Motion:"+motion,"iteratorID:",iteratorID)
                data = self.fetchLogEntryData(iteratorID, pipe, ['next', 1])
            elif motion == 'backward':
                if self.testMode:
                    _globals._print("Motion:"+motion,"iteratorID:",iteratorID)
                data = self.fetchLogEntryData(iteratorID, pipe, ['prev', 1])
                # datum will be None if the iterator is pointing to the first Log Entry.
                # Perhaps use that information???
            elif motion is None:
                motion = 'forward'
                continue

            if data == 'terminate':
                _globals._print("From CommunicationManager class: Terminating thread with iteratorID:", iteratorID)
                break

            # Later verify whether there is a valid connection using Connector.connect and Connector.sendHeartbeats.
            # Keep trying till you get a valid connection before sending data via appendRPC msgs.
            socket = self.serverSockets[iteratorID][0]
            for datum in data:
                motion = self.execute_appendEntriesRPC(datum, socket, iteratorID, isHeartbeat=False)                        
                utils.freeSharedMemory(datum[0], clear=False)
                # _globals._print("From replicateLogEntry with serverID:", self.serverID)

            del data



    def fetchLogEntryData(self, iteratorID, pipe, params):
        """
        params[0] : 'next' or 'prev'
        params[1] : By default : 1 . Could be more than 1 if multiple log entries are needed
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
            shm = utils.loadSharedMemory(name)
            data.append([shm, logIndex, term])

        if self.testMode:
            _globals._print("From server:", self.serverID, "fetchLogEntryData:", data)

        return data




class Log:

    def __init__(self, maxSize, maxLength, clearLog_event, testMode, serverID=None):

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
        self.logIndexTerm_record = {}
        self.serverID = serverID

        self.ptrs['commitIndex'] = [self.log.first, self.ref_point]



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


    def updateCommitIndex(self, new_commitIndex):

        while True:
            node = self.next('commitIndex', getNode=True)
            logIdx = self.get_entry_logIndex(node)
            if logIdx >= new_commitIndex:
                break


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

        if self.testMode:
            with _globals.print_lock:
                print("Server:",self.serverID,".Log Before addEntry:")
                logger.print_log_datachain(self)

        logIndex, currentTerm, prevLogIndex, prevLogTerm, commitIndex, leaderID, shm_size, isCommitted = params
        node = llist.dllistnode([shm_name, logIndex, currentTerm, isCommitted, prevLogIndex, prevLogTerm, commitIndex, leaderID, shm_size])
        toClear = (not self.addNodeSize(shm_size)) or (self.log.size >= self.maxLength)

        if toClear:         
            self.clearLog()
            self.addNodeSize(shm_size)

        self.log.append(node)
        self.logIndexTerm_record[(logIndex, currentTerm)] = self.log.last

        if self.testMode:
            with _globals.print_lock:
                logger.log_addEntry(node, logIndex, currentTerm, useLock=False)
                print("Server:",self.serverID,".Log After addEntry:")
                logger.print_log_datachain(self)


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
        # _globals._print("From addEntry_follower")

        return shm.name, logIdx, params


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
                    logIdx, term = self.get_entry_logIndex(entry), self.get_entry_term(entry)
                    self.logIndexTerm_record.pop((logIdx, term))

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
                            logIdx, term = self.get_entry_logIndex(self.log.first), self.get_entry_term(self.log.first)
                            self.logIndexTerm_record.pop((logIdx, term))

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
                logIdx, term = self.get_entry_logIndex(node), self.get_entry_term(node)
                self.logIndexTerm_record.pop((logIdx, term), None)

                if self.testMode:
                    _globals._print("Deleted Entry with logIdx:", self.get_entry_logIndex(node))

            next_node = node.next
            self.log.remove(node)
            node = next_node

        self.nodeSizes = []
        self.logSize = 0


    def deleteNextEntries_follower(self, node, inclusive=False):

        if self.testMode:
            with _globals.print_lock:
                print("Server:",self.serverID,".Log Before deleteNextEntries_follower:")
                logger.print_log_datachain(self)
                # print(node.value)

        if node is not None and not inclusive:
            node = node.next
            # _globals._print(node.value)

        while node is not None:
            temp = node.next
            shm = shared_memory.SharedMemory(node.value[0])
            utils.freeSharedMemory(shm)
            # _globals._print("From deleteNextEntries_follower")

            logIdx, term = self.get_entry_logIndex(node), self.get_entry_term(node)
            self.logIndexTerm_record.pop((logIdx, term))
            self.log.remove(node)
            del node
            node = temp

            if self.testMode:
                _globals._print("Removed Entry with logIdx:", logIdx, "and term:", term)

        if self.testMode:
            with _globals.print_lock:
                print("Server:",self.serverID,".Log After deleteNextEntries_follower:")
                logger.print_log_datachain(self)



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

        if prev_node is None or type(prev_node.value) != list:
            return None

        if saveState:
            self.ptrs[iteratorID][0] = prev_node
            self.ptrs[iteratorID][1] -= 1

        if getNode:
            return prev_node
        else:
            return prev_node.value


    def set_followerPtrsToLast(self):
        """
        Update the follower pointers to point to the last log entry.
        """
        for iteratorID in self.ptrs:
            if iteratorID == 'commitIndex':
                continue

            while True:
                nextNode = self.next(iteratorID, getNode=True)
                if nextNode is None:
                    break


    def get_lastEntry(self):

        if type(self.log.last) is llist.dllistnode:
            if type(self.log.last.value) == list:
                return self.log.last
            else:
                # print(6)
                return None
        else:
            return None

    
    def get_prevEntry(self, entry):

        if type(entry) is not llist.dllistnode:
            return None

        prev_entry = entry.prev
        if type(prev_entry) is llist.dllistnode:
            if type(prev_entry.value) == list:
                return prev_entry
            else:
                return None
        else:
            return None


    def findEntry(self, logIdx, logTerm, mode):

        if mode == 'Index&Term':
            node = self.logIndexTerm_record.get((logIdx, logTerm))
            if node is not None:
                # print(node.value)
                # print(self.log.last.value)
                if node.value is self.log.last.value:
                    return 'found_last', node
                else:
                    return 'found', node
            else:
                # with _globals.print_lock:
                #     logger.print_Log_attributes(self)
                #     print(self.logIndexTerm_record)
                #     print("logIdx:", logIdx, "term:", logTerm)
                return 'not_found', None


    def iteratorHalted(self, iteratorID):

        if iteratorID not in self.ptrs:
            return None

        if self.ptrs[iteratorID][0] is None:
            return True

        if self.ptrs[iteratorID][0].next is None:
            return True
        else:
            return False


