'''
Created on Jan 9, 2013

@author: mfrincu
'''

#only for AllPar1LnSdyn
desiredfast='m1.small'
#for CPU intensive set it to false
dataintensive=False

longfast=False
#used by experimentComplex to change the instance type based on vmtypeprops below
typeindex=0
#length of the btu in seconds
btu=3600
#the instance types
vmtypeprops=(('m1.small',0.080,0.120,1,'US_East_Virginia'),('m1.medium',0.160,0.120,1.6,'USEast_Virginia'),('m1.large',0.320,0.120,2.1,'USEast_Virginia'),('m1.xlarge',0.640,0.120,2.7,'USEast_Virginia'))