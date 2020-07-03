

raftParams = {}
raftParams['numServers'] = 3
raftParams['servers'] = {1:('127.0.0.1','5556'), 2:('127.0.0.1','5557')}
raftParams['serverID'] = 0
raftParams['timeoutRange'] = [1500, 2000]
raftParams['protocol'] = 'tcp'
raftParams['logMaxSize'] = 10**6
raftParams['logMaxLength'] = 10**8
raftParams['numProcesses'] = 2
raftParams['maxThreadPerProcess'] = 2
raftParams['numMajorityServers'] = 1
raftParams['iteratorIDs'] = list(raftParams['servers'].keys())
raftParams['leaderID'] = 0
raftParams['state'] = 'leader'
# raftParams['port'] = '5555'
raftParams['port'] = '6555'

settings = {}
# settings['certificate_directory'] = 