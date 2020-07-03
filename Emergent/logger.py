import json
from queue import Queue
from collections.abc import Iterable
import _globals
import utils

def print_RaftServer_attributes(raft):

    if raft is None:
        print('raft == None')
        print('Exiting from print_RaftServer_attributes')
        return


    raft_dict = None

    if type(raft) == dict:
        raft_dict = raft
    else:
        raft_dict = raft.__dict__


    print("*********** RaftServer Instance ************")
    attributes = ['state', 'commitIndex', 'currentTerm', 'lastLogIndex', 'lastLogTerm', 'prevLogIndex', 
                'prevLogTerm', 'lastApplied', 'matchIndex', 'numServers', 'servers', 'serverID', 'timeoutRange', 
                'protocol', 'logMaxSize', 'logMaxLength', 'workerProcesses', 'numProcesses', 'maxThreadPerProcess', 
                'processToIteratorIDs', 'iteratorIDs', 'replicationHistory', 'numMajorityServers',
                'entryQueue', 'fetchPendingLogQueue']


    for attr in attributes:
        if attr not in raft_dict:
            print("Error!: Attribute ", attr, " not present in RaftServer object")
        else:
            value = raft_dict[attr]
            print("***", attr, "***")
            if type(value) == dict:
                print(json.dumps(value, indent=3))
            elif type(value) == Queue:
                print(value.queue)
            else:
                print(value)


    print("*** processPipes ***")
    for k,v in raft_dict['processPipes'].items():
        print("processName: ", k)
        for pipe in v:
            print('  ', pipe.name)

    print("***************")


def print_entry_buffer(log, buf):

    func_name, args = log.get_entry_data(buf)
    lastLogIndex, term, prevLogIndex, prevLogTerm, commitIndex, leaderID = log.get_entry_params(buf)
    print("*********** New Buffer ***********")
    print("func_name:", func_name)
    print("args:", args)
    print("lastLogIndex:", lastLogIndex)
    print("term:", term)
    print("prevLogIndex:", prevLogIndex)
    print("prevLogTerm:", prevLogTerm)
    print("commitIndex:", commitIndex)
    print("leaderID:", leaderID)
    print("***************")


def print_Log_attributes(log):

    if log is None:
        print('log == None')
        print('Exiting from print_Log_attributes')
        return

    log_dict = None

    if type(log) == dict:
        log_dict = log
    else:
        log_dict = log.__dict__


    print("********* Log Instance ********")

    attributes = ['logSize', 'nodeSizes', 'matchIndex', 'ref_point']
    for attr in attributes:
        if attr not in log_dict:
            print("Error!: Attribute ", attr, " not found in Log object")
        else:
            value = log_dict[attr]
            print("***", attr, "***")
            if type(value) == dict:
                print(json.dumps(value, indent=3))
            elif type(value) == Queue:
                print(value.queue)
            else:
                print(value)

    print("*** ptrs ***")
    for k,v in log_dict['ptrs'].items():
        print("Iterator ID: ", k)
        print("  iterator ref_point: ", v[1])
        if v[0] is not None:
            print_node(v[0])

    node = log_dict['log'].first
    if node == None:
        print("log is empty")

    print("Printing the Log...")
    while node is not None:
        print_node(node)
        node = node.next

    print("***************")


def print_node(node):

    if node is None:
        return

    print("*********** New Log Entry ***********")

    if not isinstance(node.value, Iterable):
        print(node.value)
        return

    shm_name, logIndex, currentTerm, isCommitted, prevLogIndex, prevLogTerm, commitIndex, leaderID, shm_size = node.value
    print('shm_name:', shm_name)
    print('logIndex:', logIndex)
    print('term:', currentTerm)
    print('isCommitted:', isCommitted)
    print('prevLogIndex:', prevLogIndex)
    print('prevLogTerm:', prevLogTerm)
    print('commitIndex:', commitIndex)
    print('leaderID:', leaderID)
    print('shm_size:', shm_size)

    print("***************")


def print_log_datachain(log):

    if log is None:
        print('log == None')
        print('Exiting from print_log_chain')
        return

    log_dict = None

    if type(log) == dict:
        log_dict = log
    else:
        log_dict = log.__dict__

    node = log_dict['log'].first
    if node == None:
        print("log is empty")

    log_chain = []
    while node is not None:
        if isinstance(node.value, Iterable):
            shm_name = node.value[0]
            shm = utils.loadSharedMemory(shm_name)
            func_name, args = log.get_entry_data(shm.buf)
            log_chain.append([func_name, args])
        node = node.next

    print(log_chain)
    return log_chain


def print_ComMan_attributes(comMan, procIdx):

    if comMan is None:
        print('comMan == None')
        print('Exiting from print_ComMan_attributes')
        return

    comMan_dict = None

    if type(comMan) == dict:
        comMan_dict = comMan
    else:
        comMan_dict = comMan.__dict__

    print("*********** CommunicationManager Instance, procIdx:{0} ************".format(str(procIdx)))

    print("*** servers ***")
    print(json.dumps(comMan_dict['servers'], indent=3))

    print("*** iteratorThreads ***")
    for k,v in comMan_dict['iteratorThreads'].items():
        print('IteratorID: ', k)
        print('  thread:', v[0])
        print('  pipe name:', v[1].name)

    print("*** threadPipes ***")
    for pipe in comMan_dict['threadPipes']:
        print(pipe.name)

    print("*** serverSockets ***")
    serverSockets = comMan_dict['serverSockets']
    for serverID in serverSockets:
        print("serverID: ", serverID)
        socket = serverSockets[serverID]
        print("  Connected Endpoint: ", socket.LAST_ENDPOINT)

    print("***************")


def print_StateMachine_attributes(stateMachine):

    if stateMachine is None:
        print('stateMachine == None')
        print('Exiting from print_StateMachine_attributes')
        return

    sm_dict = None

    if type(stateMachine) == dict:
        sm_dict = stateMachine
    else:
        sm_dict = stateMachine.__dict__


    print("********* StateMachine Instance ********")
    attributes = ['params', 'commandBuffer', 'isLeader', 'loopThreadKill']

    for attr in attributes:
        if attr not in sm_dict:
            print("Error!: Attribute ", attr, " not found in StateMachine object")
        else:
            value = sm_dict[attr]
            print("***", attr, "***")
            if type(value) == dict:
                print(json.dumps(value, indent=3))
            elif type(value) == Queue:
                print(value.queue)
            else:
                print(value)


    print("***************")


def print_StateMachine_globalState(stateMachine):

    if stateMachine is None:
        print('stateMachine == None')
        print('Exiting from print_StateMachine_globalState')
        return

    print("Printing StateMachine globalState")
    print(stateMachine.globalState)


# Loggers below

def log_committed(logIdx, rep_history):

    _globals._print_lines(["Committed logIdx:", logIdx], ["rep_history:", rep_history])


def log_entryExecution_leader(logIdx):

    _globals._print_lines(["Leader:Executed logIdx:", logIdx])


def log_entryExecution_follower(logIdx):

    _globals._print_lines(["Follower:Executed logIdx:", logIdx])


def log_successfulReplication(logIdx, iteratorID):

    _globals._print_lines(["Replicated logIdx:", logIdx, "to server:", iteratorID])

def log_addEntry(node, logIdx):

    with _globals.print_lock:
        print("Entry Added with logIdx:", logIdx)
        # print_node(node)

