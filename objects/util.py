'''
Created on Jan 9, 2013

@author: mfrincu
'''

import config
import algorithms.util

"""
Computes the properties of a new VM for a given task 
params: the task, the VM list, the task data dependency list, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: the list of properties for the new VM
""" 
def allocatevm(t,vmlist,taskdeplist,tasksinfo):
    if t>len(taskdeplist):
        return config.vmtypeprops[0]
    (datainloc,dataoutloc)=taskdeplist[t]
    transferoutcost=vmlist[datainloc[0]+'-'+config.desiredfast][1]
    destination=['',10000000]
    for key in vmlist:
            vmprops=vmlist[key]
            cost=0
            if datainloc[0]==key.split('-')[0] or tasksinfo[1][t-1]<1024:
                cost=vmprops[0]
            else:
                cost=vmprops[0]+transferoutcost
            if cost<destination[1] and config.desiredfast in key:
                destination[0]=key
                destination[1]=cost  
    return (config.desiredfast,vmlist[destination[0]][0],vmlist[destination[0]][1],vmlist[destination[0]][2],destination[0].split('-')[0])        

"""
Computes the properties of a new VM for a given task but assigns a faster machine if the task is large and the 'longfast' flag is true
params: the task, the VM property list, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: the list of properties for the new VM
"""
def allocatevm2(t,vmtypeprops,tasksinfo):
    if config.longfast and algorithms.util.islarge(t, tasksinfo):
        return vmtypeprops[len(vmtypeprops)-1]
    else:
        return vmtypeprops[config.typeindex]

"""     
computes the diversity between two locations
params: first location, second location    
return: an n-bit value containing the diversity. The most significant bit corresponds to the continent, 
         the next one to the country, and so forth
"""         
def computediversity(location1,location2):
    diversity='111111'
    assert (len(location1) == len(location2))
    i=0
    found = False
    while i < len(location1) and not found:
        if location1[i] != location2[i]:
            found = True
            for j in range(i,len(location1)):
                diversity=diversity[0:j] + '0' + diversity[j+1:]     
        i=i+1   
    return int('111111',2)^int(diversity,2)   

"""
reads the VM types from the file. A file line contains:
location-VMtype    rentprice    transferoutprice    speedup
Example: NorthAmerica_USEast_Virginia-m1.small    0.080    0.120    1
params: filename
return: a list containing the VMs as a dictionary with the key being the VM location and type and 
        the value a list  (rent price, transfer out price, speedup) and the second list item being
        the list of locations
"""
def readVMtypes(name):
    locations = []
    vms = {} 
    f = open(name, 'r')
    for line in f.readlines():
        (key,price,outboundcost,speedup)=line.split()
        vms[key] = (float(price),float(outboundcost),float(speedup))
        (location, trash) = key.split('-')
        if location not in locations:
            locations.append(location)

    return (vms, locations)

"""
Outputs a file in the format used by SimSchlouder
params: a list of time lists (the actual execution time, the inbound transfer, the estimated execution time, the outbound transfer), the name of the DAG and the path where the file should be saved
"""
def toSimSchlouderFormat(dag,name,path):
    s=''
    #print len(dag.tasksinfo), len(dag.dag)
    for t in range(1,len(dag.dag)+1):
        s=s+str(t)+'\t'+'0'+'\t'+str(dag.tasksinfo[0][t-1])+'\t'+'~'+'\t'+str(dag.tasksinfo[2][t-1])+'\t'+str(dag.tasksinfo[1][t-1])+'\t'+str(dag.tasksinfo[3][t-1])
        if len(dag.pred(t))>0:
            s=s+'\t'+'->'+'\t'
        for p in dag.pred(t):
            s=s+str(p)+' '
        s=s+'\n'
    if (not path.endswith('/')):
        path=path+'/'
    with open(path+'simschlouder-'+name+'.tasks','w') as f:
        f.write(s)