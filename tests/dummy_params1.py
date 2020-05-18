

raftParams = {}
raftParams['numServers'] = 5
raftParams['servers'] = {1:('127.0.0.1','5556'), 2:('127.0.0.1','5557'), 3:('127.0.0.1','5558'), 4:('127.0.0.1','5559')}
raftParams['serverID'] = 0
raftParams['timeoutRange'] = [0.1, 0.2]
raftParams['protocol'] = 'tcp'
raftParams['logMaxSize'] = 10**6
raftParams['logMaxLength'] = 10**8
raftParams['numProcesses'] = 2
raftParams['maxThreadPerProcess'] = 2
raftParams['numMajorityServers'] = 2
raftParams['iteratorIDs'] = list(raftParams['servers'].keys())