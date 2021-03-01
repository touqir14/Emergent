import pathlib
import os
import sys
from multiprocessing import resource_tracker

def modify_resource_tracker():
	# See discussion: https://bugs.python.org/issue39959
	# See source code: https://github.com/python/cpython/blob/master/Lib/multiprocessing/resource_tracker.py 
	rt = resource_tracker._resource_tracker

	def register(name, rtype):
	    if rtype == 'shared_memory':
	        # print("Do nothing for shared_memory")
	        return
	    else:
	        rt._send('REGISTER', name, rtype)        

	def unregister(name, rtype):
	    if rtype == 'shared_memory':
	        # print("Do nothing for shared_memory")
	        return
	    else:
	        rt._send('UNREGISTER', name, rtype)        


	resource_tracker.register = register
	resource_tracker.unregister = register


def add_Emergent_paths():
	Emergent_path = pathlib.Path(__file__).absolute().parent.parent
	test_path = os.path.join(Emergent_path, 'tests')
	EmergentMain_path = os.path.join(Emergent_path, 'Emergent')
	sys.path.append(test_path)
	sys.path.append(EmergentMain_path)

modify_resource_tracker()
add_Emergent_paths()

