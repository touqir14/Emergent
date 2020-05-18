import init
from core import Log
import unittest
import multiprocessing
import utils

"""
Main Log APIs:

    * initIterators
    * set_entry_params
    * get_entry_params
    * get_entry_data
    * createLogEntry
    * addEntry
    * deleteEntry
    * deleteAllPrevEntries
    * jump
    * next
    * current
    * prev
    * iteratorHalted

"""
class LogTest(unittest.TestCase):

    def testInit(self):

        log = Log(10**8, 10**8, None)
        iteratorIDs = [0, 1]
        log.initIterators(iteratorIDs, [])
        
        self.assertEqual(list(log.ptrs.keys()), [0, 1], "Iterator IDs do not match within log.ptrs")
        self.assertEqual(log.ptrs[0][0].value, 0, "log.first is not None from log.ptrs[0][0]")
        self.assertEqual(log.ptrs[1][0].value, 0, "log.first is not None from log.ptrs[1][0]")
        self.assertEqual(log.ptrs[0][1], 0, "log.ref_point is not 0 from log.ptrs[0][0]")
        self.assertEqual(log.ptrs[1][1], 0, "log.ref_point is not 0 from log.ptrs[1][0]")

    def testAddEntry(self):

        log = Log(10**8, 10**8, None)
        iteratorIDs = [0, 1]
        log.initIterators(iteratorIDs, [])

        func = print
        args = [1, 2]
        # params = [0, 1, -1, 1, 0, 0]
        params = [0, 1, 2, 3, 4, 5]
        shm_name, entry_size = log.createLogEntry(func, args, params)
        reference = {}
        reference['func'] , reference['args'] = func, args
        reference['logIndex'], reference['currentTerm'], reference['prevLogIndex'] = params[0], params[1], params[2]
        reference['prevLogTerm'], reference['commitIndex'], reference['leaderID'] = params[3], params[4], params[5]
        reference['shm_name'], reference['shm_size'], reference['isCommitted'] = shm_name, entry_size, False
        params_extended = params + [entry_size, False]
        log.addEntry(shm_name, params_extended)

        self.assertEqual(len(log), 2, "len(log) should be 2")
        self.assertEqual(log.current(0), 0, "log.first should be 0 by default")
        next_entry = log.next(0, getNode=True)
        self.assertEqualEntry(log, next_entry, reference)
        self.assertEqualEntry(log, log.current(0, getNode=True), reference)
        self.assertEqual(log.ptrs[0][1], 1, 'index should be 1')
        self.assertEqual(len(log.nodeSizes), 2, 'log.nodeSizes should have 2 entries')
        self.assertEqual(log.nodeSizes[1], reference['shm_size'], 'shm_size does not match for reference')
        
        shm = multiprocessing.shared_memory.SharedMemory(name=next_entry.value[0])
        self.assertEqualEntryBuffer(log, shm.buf, reference)
        utils.freeSharedMemory(shm)


    def testDeleteEntry(self):

        log = Log(10**8, 10**8, None)
        iteratorIDs = [0, 1]
        log.initIterators(iteratorIDs, [])

        func = print
        args = [1, 2]
        params_1 = [0, 1, 2, 3, 4, 5]
        shm_name, entry_size = log.createLogEntry(func, args, params_1)
        reference_1 = {}
        reference_1['func'] , reference_1['args'] = func, args
        reference_1['logIndex'], reference_1['currentTerm'], reference_1['prevLogIndex'] = params_1[0], params_1[1], params_1[2]
        reference_1['prevLogTerm'], reference_1['commitIndex'], reference_1['leaderID'] = params_1[3], params_1[4], params_1[5]
        reference_1['shm_name'], reference_1['shm_size'], reference_1['isCommitted'] = shm_name, entry_size, False
        params_1_extended = params_1 + [entry_size, False]
        log.addEntry(shm_name, params_1_extended)

        func = print
        args = [2, 3, 4]
        params_2 = [1, 2, 3, 4, 5, 6]
        shm_name, entry_size = log.createLogEntry(func, args, params_2)
        reference_2 = {}
        reference_2['func'] , reference_2['args'] = func, args
        reference_2['logIndex'], reference_2['currentTerm'], reference_2['prevLogIndex'] = params_2[0], params_2[1], params_2[2]
        reference_2['prevLogTerm'], reference_2['commitIndex'], reference_2['leaderID'] = params_2[3], params_2[4], params_2[5]
        reference_2['shm_name'], reference_2['shm_size'], reference_2['isCommitted'] = shm_name, entry_size, False
        params_2_extended = params_2 + [entry_size, False]
        log.addEntry(shm_name, params_2_extended)

        self.assertEqual(len(log), 3, "len(log) should be 2")
        self.assertEqual(log.current(0), 0, "log.first should be 0 by default")
        next_entry = log.next(0, getNode=True)
        next_entry = log.next(0, getNode=True)
        self.assertEqualEntry(log, next_entry, reference_2)
        self.assertEqualEntry(log, log.current(0, getNode=True), reference_2)
        self.assertEqual(log.ptrs[0][1], 2, 'index should be 2')
        
        shm = multiprocessing.shared_memory.SharedMemory(name=next_entry.value[0])
        self.assertEqualEntryBuffer(log, shm.buf, reference_2)
        utils.freeSharedMemory(shm, clear=False)

        prev_entry = log.prev(0, saveState=False, getNode=True)
        self.assertEqualEntry(log, prev_entry, reference_1)
        self.assertEqual(log.ptrs[0][1], 2, 'index should be 2')

        log.deleteEntry(prev_entry) # At this stage prev_entry cannot be deleted because one of the pointers is at idx=0
        self.assertEqual(len(log), 3, 'len(log) should be 3')


        next_entry = log.next(1, getNode=True)
        next_entry = log.next(1, getNode=True)
        log.deleteEntry(prev_entry) # At this stage prev_entry can be deleted because both of the pointers is at idx=2
        self.assertEqual(len(log), 2, 'len(log) should be 2')
        self.assertEqualEntry(log, log.current(0, getNode=True), reference_2)
        self.assertEqual(len(log.nodeSizes), 2, 'len(log.nodeSizes) should be 2')
        self.assertEqual(log.nodeSizes[1], reference_2['shm_size'], 'shm_size does not match for reference')

        log.deleteAllEntries()
        self.assertEqual(len(log), 0, 'log should be empty')
        self.assertEqual(sum(log.nodeSizes), 0, 'nodeSizes should be empty')
        self.assertEqual(log.logSize, 0, 'logSize should be 0')


    def testDeletePrevEntries(self):

        log = Log(10**8, 10**8, None)
        iteratorIDs = [0, 1]
        log.initIterators(iteratorIDs, [])

        func = print
        args = [1, 2]
        params_1 = [0, 1, 2, 3, 4, 5]
        shm_name, entry_size = log.createLogEntry(func, args, params_1)
        reference_1 = {}
        reference_1['func'] , reference_1['args'] = func, args
        reference_1['logIndex'], reference_1['currentTerm'], reference_1['prevLogIndex'] = params_1[0], params_1[1], params_1[2]
        reference_1['prevLogTerm'], reference_1['commitIndex'], reference_1['leaderID'] = params_1[3], params_1[4], params_1[5]
        reference_1['shm_name'], reference_1['shm_size'], reference_1['isCommitted'] = shm_name, entry_size, False
        params_1_extended = params_1 + [entry_size, False]
        log.addEntry(shm_name, params_1_extended)

        func = print
        args = [2, 3, 4]
        params_2 = [1, 2, 3, 4, 5, 6]
        shm_name, entry_size = log.createLogEntry(func, args, params_2)
        reference_2 = {}
        reference_2['func'] , reference_2['args'] = func, args
        reference_2['logIndex'], reference_2['currentTerm'], reference_2['prevLogIndex'] = params_2[0], params_2[1], params_2[2]
        reference_2['prevLogTerm'], reference_2['commitIndex'], reference_2['leaderID'] = params_2[3], params_2[4], params_2[5]
        reference_2['shm_name'], reference_2['shm_size'], reference_2['isCommitted'] = shm_name, entry_size, False
        params_2_extended = params_2 + [entry_size, False]
        log.addEntry(shm_name, params_2_extended)

        entry_0 = log.jump(0, 2, getNode=True)
        self.assertEqualEntry(log, entry_0, reference_2)

        log.deleteAllPrevEntries(entry_0)
        self.assertEqual(len(log), 3, "len(log) should be 3")

        entry_1 = log.jump(1, 2, getNode=True)
        log.deleteAllPrevEntries(entry_0)
        self.assertEqual(len(log), 1, "len(log) should be 1")
        self.assertEqual(log.nodeSizes[0], reference_2['shm_size'], 'shm_size does not match for reference')

        log.deleteAllEntries()
        self.assertEqual(len(log), 0, "len(log) should be 0")


    def assertEqualEntry(self, log, entry, reference):

        print("*** assertEqualEntry part1 ***")
        self.assertEqual(entry.value[0], reference['shm_name'], 'shm_name does not match')
        self.assertEqual(entry.value[1], reference['logIndex'], 'logIndex does not match')
        self.assertEqual(entry.value[2], reference['currentTerm'], 'currentTerm does not match')
        self.assertEqual(entry.value[3], reference['isCommitted'], 'isCommitted does not match')
        self.assertEqual(entry.value[4], reference['prevLogIndex'], 'prevLogIndex does not match')
        self.assertEqual(entry.value[5], reference['prevLogTerm'], 'prevLogTerm does not match')
        self.assertEqual(entry.value[6], reference['commitIndex'], 'commitIndex does not match')
        self.assertEqual(entry.value[7], reference['leaderID'], 'leaderID does not match')
        self.assertEqual(entry.value[8], reference['shm_size'], 'shm_size does not match')
        print("*******************")

        params = log.get_entry_params(entry)

        print("*** assertEqualEntry part2 ***")
        self.assertEqual(params[0], reference['logIndex'], 'logIndex does not match')
        self.assertEqual(params[1], reference['currentTerm'], 'currentTerm does not match')
        self.assertEqual(params[2], reference['prevLogIndex'], 'prevLogIndex does not match')
        self.assertEqual(params[3], reference['prevLogTerm'], 'prevLogTerm does not match')
        self.assertEqual(params[4], reference['commitIndex'], 'commitIndex does not match')
        self.assertEqual(params[5], reference['leaderID'], 'leaderID does not match')
        print("*******************")


    def assertEqualEntryBuffer(self, log, buf, reference):

        print("*** assertEqualEntryBuffer ***")
        func_name, args = log.get_entry_data(buf)
        self.assertEqual(reference['func'].__name__, func_name, "func_name does not match")
        self.assertListEqual(reference['args'], args, "args do not match")
        
        params = log.get_entry_params(buf)
        self.assertEqual(params[0], reference['logIndex'], 'logIndex does not match')
        self.assertEqual(params[1], reference['currentTerm'], 'currentTerm does not match')
        self.assertEqual(params[2], reference['prevLogIndex'], 'prevLogIndex does not match')
        self.assertEqual(params[3], reference['prevLogTerm'], 'prevLogTerm does not match')
        self.assertEqual(params[4], reference['commitIndex'], 'commitIndex does not match')
        self.assertEqual(params[5], reference['leaderID'], 'leaderID does not match')
        print("********************")



def run():
    unittest.main()

if __name__ == '__main__':
    run()