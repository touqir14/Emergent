from collections import OrderedDict, Mapping, Container
from sys import getsizeof
import llist
import math
import copy
import multiprocessing
import _globals
import socket
from contextlib import closing

# Use set() for ids when calling this function
def getObjectSize(o, ids):
    """Find the memory footprint of a Python object
    This is a recursive function that rills down a Python object graph
    like a dictionary holding nested ditionaries with lists of lists
    and tuples and sets.
    The sys.getsizeof function does a shallow size of only. It counts each
    object inside a container as pointer only regardless of how big it
    really is.
    :param o: the object
    :param ids:
    :return:
    """
    d = getObjectSize
    if id(o) in ids:
        return 0

    r = getsizeof(o)
    ids.add(id(o))

    if isinstance(o, str):
        return r

    if isinstance(o, Mapping):
        print("Mapping")
        return r + sum(d(k, ids) + d(v, ids) for k, v in o.items())

    if isinstance(o, Container) or isinstance(o, memoryview):
        print("Container")
        return r + sum(d(x, ids) for x in o)

    if isinstance(o, llist.dllistnode):
        print("dllistnode")
        return r + d(o.value, ids)

    return r


def encode_int_bytearray(array, end, a):

    binary_a = bin(a)
    length_a = len(binary_a)
    iterations = math.ceil((length_a - 2) / 8) - 1
    for i in range(iterations):
        array[end-i] = int(binary_a[length_a-(i+1)*8:length_a-i*8], 2)

    array[end-iterations] = int(binary_a[2:length_a-iterations*8], 2)

def decode_int_bytearray(array, start, end):

    byte_str = "0b"
    for i in range(end - start + 1):
        bin_rep = bin(array[start+i])[2:]
        zero_paddings = 8 - len(bin_rep)
        byte_str += (zero_paddings*'0' + bin_rep)

    return int(byte_str, 2)  



def loadSharedMemory(shm_name):

    # if shm_name == _globals.no_op:
    #     return False

    mem = multiprocessing.shared_memory.SharedMemory(name=shm_name)
    return mem


def freeSharedMemory(mem, clear=True):

    if type(mem) is str:
        # if shm_name == _globals.no_op:
        #     return False
        mem = multiprocessing.shared_memory.SharedMemory(name=mem)

    mem.close()

    if clear:
        mem.unlink()    

    # print("Cleared Shared Memory:", mem.name)

def findFreePort():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
