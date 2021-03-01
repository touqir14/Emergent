

raftParams = {}
raftParams['numServers'] = 3
raftParams['servers'] = {0:('127.0.0.1','5555'), 1:('127.0.0.1','5556')}
raftParams['serverID'] = 2
raftParams['timeoutRange'] = [1500, 2000]
raftParams['protocol'] = 'tcp'
raftParams['logMaxSize'] = 10**6
raftParams['logMaxLength'] = 10**8
raftParams['numProcesses'] = 2
raftParams['maxThreadPerProcess'] = 2
raftParams['numMajorityServers'] = 1
raftParams['iteratorIDs'] = list(raftParams['servers'].keys())
raftParams['leaderID'] = 0
raftParams['state'] = 'follower'
# raftParams['port'] = '5557'
raftParams['port'] = '6557'

settings = {}
# settings['certificate_directory'] = 