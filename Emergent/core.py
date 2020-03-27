import random
import msgpack
import utils


class RaftServer:

	"""
	self.state : [0,1,2]. 0 identifies a leader, 1 identifies a follower, 2 identifies a candidate.
	self.currentTerm : a non-negative integer. Denotes the current term of the server.
	self.commitIndex : a non-negative integer. Denotes the highest log entry known to be committed.
	self.lastApplied : a non-negative integer. Denotes the highest log entry applied to the state machine.
	self.nextIndex : (leader) a list of positive integer initialized to 1 for all servers. i'th entry denotes the next log entry to be sent to the i'th follower.
	self.matchIndex : (leader) a list of non-negative integers initialized to 0 for all servers. i'th entry denotes index of highest log entry replicated to i'th follower.
	"""

	def __init__(self):
		
		self.state = 1
		self.log = []
		self.serializedLog = []
		self.currentTerm = 1
		self.commitIndex = 0
		self.lastApplied = 0
		self.nextIndex = []
		self.matchIndex = []
		self.numServers = 1
		self.servers = []
		self.leaderID = None
		self.timeoutRange = [0.1,0.2]
		self.protocol = "tcp"

		random.seed(time.time())



	def initLog(self):
		self.log = [None]
		return

	def sendHeartBeat(self):
		pass

	def sendLogEntries(self):
		pass

	def createLogEntry(self):
		# This function creates a log entry and appends it to the log.
		# First ensure that there is enough RAM to store a new log entry.
		self.log
		pass

	def replicateLogEntry(self):
		pass

	def executeLogEntry(self):
		pass

	def appendRPC_manager(self, e):
		if 
		pass

	def commitLogEntry(self, logEntry, ):
		pass

	def executeCommand(self, func, args, callback):
		logEntry = self.createLogEntry
		self.commitLogEntry
		return


	def socketToServer(self, server_idx):
		IP, port = self.servers[server_idx]
		context = zmq.Context()
		socket = context.socket(zmq.PAIR)
		addr = "{0}://{1}:{2}".format(self.protocol, IP, port)
		socket.connect(addr)
		return socket

	def serializeLogEntry(self, data):
		binary_data = msgpack.packb(data, use_bin_type=True)
		return binary_data

	def appendEntriesRPC(self, binary_data, dest_idx, socket):
		socket.send(binary_data)
		RESP_bin = socket.recv()
		RESP = msgpack.unpackb(ACK_bin)
		success = RESP[0]
		follower_term = RESP[1]
		# The leader should send the RPC indefinitely till it receives a "proper" ACK.
		return success, follower_termee

	def requestVote(self):
		pass

	def followerToCandidate(self):
		pass

	def candidateToFollower(self):
		pass

	def candidateToLeader(self):
		pass

	def leaderToFollower(self):
		pass

	def resetTimer(self, oldTimer, runnable, startTimer=True):
		"""
		Use threading.Timer for setting a timer. 
		See https://stackoverflow.com/questions/24968311/how-to-set-a-timer-clear-a-timer-in-python
		threading.Timer.interval gives the duration.
		"""
		if oldTimer not None:
			oldTimer.cancel()

		timerDuration = random.uniform(self.timeoutRange[0], self.timeoutRange[1])
		newTimer = threading.Timer(timerDuration, runnable)
		
		if startTimer: 
			newTimer.start()

		return newTimer



class Log:

	def __init__(self, maxSize, maxLength):

		self.log = llist.dllist()
		self.maxSize = maxSize
		self.maxLength = maxLength
		self.ptrs = {}
		self.ref_point = 0
		self.nodeSizes = []
		self.logSize = 0
		self.clearLog_event = 


	def initIterators(self, numNewIterators, obsoleteIterators):

		for iteratorID in obsoleteIterators:
			if iteratorID in self.ptrs:
				del self.ptrs[iteratorID]

		totalIterators = len(self.ptrs) + numNewIterators
		newIDs = []

		if len(obsoleteIterators) >= numNewIterators:
			newIDs = sorted(obsoleteIterators)[:numNewIterators]
			for iteratorID in newIDs:
				self.ptrs[iteratorID] = [self.log.first, self.ref_point]
		else:
			allIDs = set(range(totalIterators))
			usedIDs = set(self.ptrs.keys())
			newIDs = list(allIDs - usedIDs) 
			for iteratorID in newIDs:
				self.ptrs[iteratorID] = [self.log.first, self.ref_point]

		return newIDs


	def findSmallestPtr(self):

		smallestPtrIdx = argmin(zip(*self.ptrs.values())[1])
		ptr, idx = self.ptrs.values()[smallestPtrIdx]
		idx -= self.ref_point
		return ptr, idx


	def findLargestPtr(self):

		largestPtrIdx = argmax(zip(*self.ptrs.values())[1])
		ptr, idx = self.ptrs.values()[largestPtrIdx]
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

		sizes = nodeSizes[idxs]
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
		
		"""

		while (self.logSize + reduceBy) > self.maxSize or self.log.size >= self.maxLength:
			smallestPtr, smallestIdx = self.findSmallestPtr()
			if smallestIdx == 0:
				self.clearLog_event.wait()
				self.clearLog_event.clear()
			self.deleteAllPrevEntries(smallestPtr, smallestIdx)


	def addEntry(self, logIndex, term, func, args, isCommitted):

		logEntry = [logIndex, term, func, args, isCommitted]
		node = llist.dllistnode(logEntry)
		node_size = utils.getObjectSize(node)
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
					self.log.clearLeft(entry.prev)
					self.ref_point += idx
					self.deleteNodeSize(slice(0, idx))

		return True


	# def deleteAllNextEntries(self, entry):
	# 	# TODO. Don't update ref_point

	# 	if type(entry) is not llist.dllistnode:
	# 		return False
	# 	else:
	# 		if type(entry.next) is llist.dllistnode:
	# 			self.log.clearRight(entry.next)

	# 	return True


	def __len__(self):

		return self.log.size


	def jump(self, iteratorID, jump_steps, saveState=True):

		if iteratorID not in self.ptrs:
			return None

		iteratorIdx = self.ptrs[iteratorID][1] - self.ref_point
		jumpIdx = iteratorIdx + jump_steps

		if not 0 <= jumpIdx <= self.log.size - 1:
			return None

		if jump_steps == 0:
			return self.ptrs[iteratorID][0].value

		if saveState:
			self.ptrs[iteratorID][0] = self.log.nodeat(jumpIdx)
			self.ptrs[iteratorID][1] += jump_steps
			return self.ptrs[iteratorID][0].value
		else:
			return self.log.nodeat(jumpIdx).value


	def next(self, iteratorID, saveState=True):

		if iteratorID not in self.ptrs:
			return None

		next_node = self.ptrs[iteratorID][0].next
		if saveState:
			if next_node is None:
				return None
			else:
				self.ptrs[iteratorID][0] = next_node
				self.ptrs[iteratorID][1] += 1
				return next_node.value
		else:
			if next_node is None:
				return None
			else:
				return next_node.value

	
	def current(self, iteratorID):

		if iteratorID in self.ptrs:
			return self.ptrs[iteratorID][0].value
		else:
			return None

	
	def prev(self, iteratorID, saveState=True):

		if iteratorID not in self.ptrs:
			return None

		prev_node = self.ptrs[iteratorID][0].prev
		if saveState:
			if prev_node is None:
				return None
			else:
				self.ptrs[iteratorID][0] = prev_node
				self.ptrs[iteratorID][1] -= 1
				return prev_node.value
		else:
			if prev_node is None:
				return None
			else:
				return prev_node.value


	def iteratorHalted(self, iteratorID):

		if iteratorID not in self.ptrs:
			return None

		if self.ptrs[iteratorID][0].next is None:
			return True
		else:
			return False


	# def twoWay_Iterator(self):

	# 	run = True
	# 	while run:
	# 		# run, forward = yield
	# 		forward = yield
	# 		if forward:
	# 			if 0 <= (self.ptr+1) <= len(self.log) - 1:
	# 				self.ptr += 1
	# 				yield self.log[self.ptr]
	# 			else:
	# 				yield False
	# 		else:
	# 			if 0 <= (self.ptr-1) <= len(self.log) - 1:
	# 				self.ptr -= 1
	# 				yield self.log[self.ptr]
	# 			else:
	# 				yield False


