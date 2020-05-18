from core import RaftLauncher
import time
import dummy_follower
import multiprocessing
import pathlib
import os
import logging

Emergent_path = pathlib.Path(__file__).absolute().parent.parent
dummy_params1_path = os.path.join(Emergent_path, 'tests/dummy_params1.py')


def test1():
    """ 
    Tests basic startup and termination.
    """
    rl = RaftLauncher(dummy_params1_path, testMode=True)
    stateMachine, testkit = rl.start()
    testkit.setStateMachine(stateMachine)
    time.sleep(1)
    testkit.print_RaftServer_attributes(mode=1)
    testkit.print_Log_attributes(mode=1)
    testkit.print_SM_attributes(mode=0)
    # ## 2 CommunicationManager processes
    testkit.print_ComMan_attributes(0, mode=1)
    time.sleep(0.5)
    testkit.print_ComMan_attributes(1, mode=1)
    ##
    testkit.exit()
    rl.exit()


def test2():

    follower_process = launch_dummy_followers()
    time.sleep(1)
    rl = RaftLauncher(dummy_params1_path, testMode=True)
    stateMachine, testkit = rl.start()
    testkit.setStateMachine(stateMachine)
    time.sleep(1)

    # testkit.print_ComMan_attributes(0, mode=1)
    # time.sleep(0.5)
    # testkit.print_ComMan_attributes(1, mode=1)
    # time.sleep(0.5)

    func = print
    args = ['Executed: log Entry 1']
    stateMachine.executeCommand(func, args)

    func = print
    args = ['Executed: log Entry 2']
    stateMachine.executeCommand(func, args)

    func = print
    args = ['Executed: log Entry 3']
    stateMachine.executeCommand(func, args)

    time.sleep(20)
    testkit.exit()
    rl.exit()
    follower_process.kill()


def launch_dummy_followers():
    p = multiprocessing.Process(target=dummy_follower.run,)
    p.start()
    return p
    # time.sleep(2)
    # p.kill()

def run():
    # test1()
    test2()

    # launch_dummy_followers()
