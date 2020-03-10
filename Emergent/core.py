import random
import msgpack


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

		self.log = []
		self.maxSize = maxSize
		self.maxLength = maxLength
		self.ptr = 0
		self.iter = None



	def addEntry(self, logIndex, term, func, args, isCommitted):

		logEntry = [logIndex, term, func, args, isCommitted]
		self.log.append(logEntry)
		# Add code for detecting whether Log has run out of memory. If so, this function should block
		# until there is an empty slot for a new entry.

	def deleteEntry(self):
		pass

	def next(self, iteratorID, saveState=True):

		next_node = self.ptrs[iteratorID].next
		if saveState:
			if next_node is None:
				return None
			else:
				self.ptrs[iteratorID] = next_node
				return next_node.value()
		else:
			if next_node is None:
				return None
			else:
				return next_node.value()

	def current(self, iteratorID):

		return self.ptrs[iteratorID]

	def prev(self, iteratorID, saveState=True):

		prev_node = self.ptrs[iteratorID].prev
		if saveState:
			if prev_node is None:
				return None
			else:
				self.ptrs[iteratorID] = prev_node
				return prev_node.value()
		else:
			if prev_node is None:
				return None
			else:
				return prev_node.value()


	def iteratorHalted(self, iteratorID):

		if self.ptrs[iteratorID].next is None:
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


