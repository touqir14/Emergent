import init
from core import RaftLauncher
import time
# import dummy_follower
import multiprocessing
import pathlib
import os
import logging
import _globals
import threading
import utils
import zmq
import msgpack
import router
from runpy import run_path
from argparse import Namespace

Emergent_path = pathlib.Path(__file__).absolute().parent.parent
dummy_params2_follower1_path = os.path.join(Emergent_path, 'tests/dummy_params2_follower1.py')
dummy_params2_follower2_path = os.path.join(Emergent_path, 'tests/dummy_params2_follower2.py')
dummy_params2_leader_path = os.path.join(Emergent_path, 'tests/dummy_params2_leader.py')

IPC_FIFO_NAME = "raft_test_IPC"

def test1():
    """ 
    Tests basic startup and termination.
    """
    rl_leader = RaftLauncher(dummy_params2_leader_path, testMode=True)
    rl_follower1 = RaftLauncher(dummy_params2_follower1_path, testMode=True)
    rl_follower2 = RaftLauncher(dummy_params2_follower2_path, testMode=True)
    stateMachine, testkit_leader = rl_leader.start()
    _, testkit_follower1 = rl_follower1.start()
    _, testkit_follower2 = rl_follower2.start()
    time.sleep(1)
    # testkit.print_RaftServer_attributes(mode=1)
    # testkit.print_Log_attributes(mode=1)
    # testkit.print_SM_attributes(mode=0)
    # # ## 2 CommunicationManager processes
    # testkit.print_ComMan_attributes(0, mode=1)
    # time.sleep(0.5)
    # testkit.print_ComMan_attributes(1, mode=1)
    ##

    func = print
    args = []
    # args = ['Executed: log Entry 1']
    stateMachine.executeCommand(func, args, 1)

    # func = print
    # args = ['Executed: log Entry 2']
    stateMachine.executeCommand(func, args, 2)

    # func = print
    # args = ['Executed: log Entry 3']
    stateMachine.executeCommand(func, args, 3)

    time.sleep(3)
    testkit_leader.exit()
    testkit_follower1.exit()
    testkit_follower2.exit()
    rl_leader.exit()
    # _globals._print('exited from rl_leader')
    rl_follower1.exit()
    rl_follower2.exit()


def test2():

    rl_leader = RaftLauncher(dummy_params2_leader_path, testMode=True)
    rl_follower1 = RaftLauncher(dummy_params2_follower1_path, testMode=True)
    rl_follower2 = RaftLauncher(dummy_params2_follower2_path, testMode=True)
    stateMachine, testkit_leader = rl_leader.start()
    _, testkit_follower1 = rl_follower1.start()
    _, testkit_follower2 = rl_follower2.start()
    time.sleep(1)
    # testkit.print_RaftServer_attributes(mode=1)
    # testkit.print_Log_attributes(mode=1)
    # testkit.print_SM_attributes(mode=0)
    # # ## 2 CommunicationManager processes
    # testkit.print_ComMan_attributes(0, mode=1)
    # time.sleep(0.5)
    # testkit.print_ComMan_attributes(1, mode=1)
    ##

    func = eval
    args = ['1']
    stateMachine.executeCommand(func, args, 1)

    args = ['2']
    stateMachine.executeCommand(func, args, 2)

    args = ['3']
    stateMachine.executeCommand(func, args, 3)

    time.sleep(35)
    # print("All threads:", threading.enumerate())
    testkit_leader.exit()
    testkit_follower1.exit()
    testkit_follower2.exit()
    rl_leader.exit()
    rl_follower1.exit()
    rl_follower2.exit()

    # time.sleep(2)
    # print("Total number of threads running:", threading.active_count())
    # print("All threads:", threading.enumerate())


def test3():
    all_raftParams = []
    all_raftParams.append(Namespace(**run_path(dummy_params2_follower1_path)).raftParams)
    all_raftParams.append(Namespace(**run_path(dummy_params2_follower2_path)).raftParams)
    all_raftParams.append(Namespace(**run_path(dummy_params2_leader_path)).raftParams)
    router_instance = router.createRouter(all_raftParams)
    router_instance.start()

    rl_leader = RaftLauncher(dummy_params2_leader_path, testMode=True)
    rl_follower1 = RaftLauncher(dummy_params2_follower1_path, testMode=True)
    rl_follower2 = RaftLauncher(dummy_params2_follower2_path, testMode=True)

    SM_leader, testkit_leader = rl_leader.start()
    testkit_leader.setStateMachine(SM_leader)
    SM_follower1, testkit_follower1 = rl_follower1.start()
    testkit_follower1.setStateMachine(SM_follower1)
    SM_follower2, testkit_follower2 = rl_follower2.start()
    testkit_follower2.setStateMachine(SM_follower2)

    all_stateMachines = [SM_leader, SM_follower1, SM_follower2]

    testkit_leader.name = 'leader'
    testkit_follower1.name = 'follower1'
    testkit_follower2.name = 'follower2'
    time.sleep(1)
    _globals._print("*** RAFT_TEST: Raft System Initialized")
    
    # PORT = utils.findFreePort()
    PORT = 6000
    print("*** RAFT_TEST: Listening on Port:", PORT)
    addr = "tcp://*:" + str(PORT)
    socket = zmq.Context().socket(zmq.ROUTER)
    socket.bind(addr)
    cmd_id = 0

    while True:
        _, received_bin = socket.recv_multipart()
        received = msgpack.unpackb(received_bin)
        data = received.split('-')
        cmd_type = data[0]
        
        if cmd_type == 'exit':
            break

        elif cmd_type == 'printGlobalStates':
            testkit_leader.print_SM_globalState()
            testkit_follower1.print_SM_globalState()
            testkit_follower2.print_SM_globalState()
        
        elif cmd_type == 'entry':
            try:
                print(data)
                func, args = eval(data[1]), eval(data[2]) 
                cmd_id += 1
                for SM in all_stateMachines:
                    if SM.executeCommand(func, args, cmd_id):
                        break

            except Exception as e:
                print("Failed to receive from testClient")
                print(str(e))
                continue
        
        else:
            print("Invalid cmd_type:",received)
            continue

    testkit_leader.exit()
    testkit_follower1.exit()
    testkit_follower2.exit()
    rl_leader.exit()
    rl_follower1.exit()
    rl_follower2.exit()
    router_instance.terminate()


def run():
    # test1()
    # test2()
    test3()

