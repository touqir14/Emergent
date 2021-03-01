import threading
import logger
import _globals

class TestKit:

    def __init__(self, raft=None, ComMan=None):

        if raft is not None:
            self.raft = raft
            self.log = raft.log
        else:
            self.raft = None
            self.log = None
        
        self.ComMan = ComMan
        self.stateMachine = None
        self.name = ''

    def setStateMachine(self, stateMachine):

        self.stateMachine = stateMachine

    def setPipes(self, pipe_request, pipe_fetch):

        self.pipe_request = pipe_request
        self.pipe_fetch = pipe_fetch


    def initListenThread(self):

        self.runThread = True
        self.listenThread = threading.Thread(target=self.listen_loop,)
        self.listenThread.start()


    def listen_loop(self):

        while self.runThread:
            msg = self.pipe_fetch.recv()

            if msg[0] == 'fetchRaftAttributes':
                attributes = msg[1]
                attribute_dict = self.fetchRaftAttributes(attributes)
                self.pipe_fetch.send(attribute_dict)

            elif msg[0] == 'fetchLogAttributes':
                attributes = msg[1]
                attribute_dict = self.fetchLogAttributes(attributes)
                self.pipe_fetch.send(attribute_dict)

            elif msg[0] == 'printRaftAttributes':
                self.print_RaftServer_attributes(mode=0)
                self.pipe_fetch.send('ACK')

            elif msg[0] == 'printLogAttributes':
                self.print_Log_attributes(mode=0)
                self.pipe_fetch.send('ACK')

            elif msg[0] == 'printComManAttributes':
                self.print_ComMan_attributes(procIdx=msg[1], mode=2)
                self.pipe_fetch.send('ACK')

            elif msg[0] == 'terminate':
                _globals._print("From TestKit class: terminating")
                break

            else:
                _globals._print("Wrong command - {0}: From TestKit.listen_loop".format(msg[0]))


    def processRequest(self, req, attr):

        mode = attr['mode']
        # _globals._print("HELLLL_1")
        if req == 'printRaftAttributes':
            self.print_RaftServer_attributes(mode=mode)

        elif req == 'printLogAttributes':
            self.print_Log_attributes(mode=mode)

        elif req == 'printComManAttributes':
            # _globals._print("HELLLL_2")
            procIdx = attr['procIdx']
            self.print_ComMan_attributes(procIdx, mode=mode)

        else:
            _globals._print("Wrong command - {0}: From TestKit.processRequest".format(req))



    def print_RaftServer_attributes(self, mode=0):

        if mode == 0:
            with _globals.processForkLock and _globals.print_lock:
                logger.print_RaftServer_attributes(self.raft)

        elif mode == 1:
            self.pipe_request.send(['printRaftAttributes'])
            self.pipe_request.recv()

        elif mode == 2:
            self.pipe_request.send(['fetchRaftAttributes'])
            Raft_attributes = self.pipe_request.recv()
            logger.print_RaftServer_attributes(Raft_attributes)


    def print_Log_attributes(self, mode=0):

        if mode == 0:
            with _globals.processForkLock and _globals.print_lock:
                logger.print_Log_attributes(self.log)

        elif mode == 1:
            self.pipe_request.send(['printLogAttributes'])
            self.pipe_request.recv()

        elif mode == 2:
            self.pipe_request.send(['fetchLogAttributes'])
            Log_attributes = self.pipe_request.recv()
            logger.print_Log_attributes(Log_attributes)


    def print_ComMan_attributes(self, procIdx, mode=0):

        if mode == 0:
            with _globals.processForkLock and _globals.print_lock:
                logger.print_ComMan_attributes(self.ComMan, procIdx)


        elif mode == 1:
            self.pipe_request.send(['printComManAttributes', procIdx])
            self.pipe_request.recv()

        elif mode == 2:
            proc = self.raft.workerProcesses[procIdx]
            pipe = self.raft.processPipes[proc.name][-1]
            pipe.send(['testkit', 'printComManAttributes'])


    def fetchRaftAttributes(self, attributes):

        attribute_dict = {}
        for attr in attributes:
            if attr in self.raft.__dict__:
                attribute_dict[attr] = self.raft.__dict__[attr]

        return attribute_dict


    def fetchLogAttributes(self, attributes):

        attribute_dict = {}
        for attr in attributes:
            if attr in self.log.__dict__:
                attribute_dict[attr] = self.log.__dict__[attr]

        return attribute_dict


    def print_SM_attributes(self, mode=0):

        if mode == 0:
            with _globals.processForkLock and _globals.print_lock:
                logger.print_StateMachine_attributes(self.stateMachine)


    def print_SM_globalState(self, mode=0):

        if mode == 0:
            with _globals.processForkLock and _globals.print_lock:
                print()
                print('From TestKit-', self.name)
                logger.print_StateMachine_globalState(self.stateMachine)


    def exit(self):

        self.pipe_request.send(['terminate'])
