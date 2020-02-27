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



	def sendHeartBeat(self):
		pass

	def sendLogEntries(self):
		pass

	def socketToServer(self, server_idx):
		IP, port = self.servers[server_idx]
		context = zmq.Context()
		socket = context.socket(zmq.PAIR)
		addr = "{0}://{1}:{2}".format(self.protocol, IP, port)
		socket.connect(addr)
		return socket


	def appendEntriesRPC(self, data, dest_idx, socket):
		binary_data = msgpack.packb(data, use_bin_type=True)
		socket.send(binary_data)
		RESP_bin = socket.recv()
		RESP = msgpack.unpackb(ACK_bin)
		success = RESP[0]
		follower_term = RESP[1]
		# The leader should send the RPC indefinitely till it receives a "proper" ACK.
		return success, follower_term

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
