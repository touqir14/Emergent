import multiprocessing

print_lock = multiprocessing.Lock()
processForkLock = multiprocessing.Lock()
shm_lock = multiprocessing.Lock()

def NO_OP(*args):
	return

def _print(*args):
	with print_lock:
		print(*args)

def _print_lines(*args):
	with print_lock:
		print("")
		for i in args:
			print(*i)
		print("")