'''
Created on Jan 9, 2013

@author: mfrincu
'''

import json
import numpy
import math
import uuid
import datetime

import objects.vms as vms
from config import *

"""
Determines the tasks situated on the critical path, i.e., the longest path in the task DAG
params: the task DAG, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: the list of tasks on the critical path
"""
def computeCPtasks(dag,tasksinfo):
    paths=[]
    for t in dag.startnodes:
        paths=__findpath(t,dag,tasksinfo,tasksinfo[0][t-1],[t])
    #print paths
    cp=[]
    cpval=0
    for i in range(0,len(paths)-1):
        if i%2==0:
            if paths[i]>cpval:
                cpval=paths[i]
                cp=paths[i+1]
    return (cpval,cp)

"""
Finds the path from a given task to the final task
params: the task, the task DAG, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the cost so far, the path so far 
return: the list of tasks on the path
"""
def __findpath(t,dag,tasksinfo,cost,path):    
    path=path+[t]
    if dag.suc(t)==[]:
        return [cost,path]
    paths=[]
    for s in dag.suc(t):
        childpaths=__findpath(s,dag,tasksinfo,cost+tasksinfo[0][s-1],path)
        for cp in childpaths:
            paths.append(cp)
    return paths    

"""
Sorts a list of tasks based on their ET
params: the task list, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: the list of tasks sorted
"""
def sorttasksbyetasc(tasks,tasksinfo):
    dic={}
    for i in range(0,len(tasksinfo[0])):
        dic[i+1]=tasksinfo[0][i]  
    alltasks=[]
    for k,v in sorted(dic.iteritems(), key=lambda (k,v): (v,k)):
        alltasks.append(k)
    otasks=[]
    for t in alltasks:
        if t in tasks:
            otasks.append(t)
    return otasks

"""
Sorts a list of tasks based on their rank
params: the task list, the task DAG
return: the list of tasks sorted
""" 
def sorttasksbyrank(tasks,dag):
    alltasks=dag.computepriorityallandsortdesc()
    otasks=[]
    for t in alltasks:
        if t in tasks:
            otasks.append(t)
    return otasks 

"""
Computes the transfer time needed by a task on a 1Gb link. The simple method uses a store and forward technique
params: the task, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
"""    
def computetransfertime(t,tasksinfo):
    size=tasksinfo[1][t-1]
    return 1/(size/1000*8)
    
"""
Checks whether or not the BTU by adding a new task is exceeded
params: maxeft (a list (VM ID, EFT)), the VM list, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the task 
return: true of BTU is exceeded, false otherwise
"""         
def exceedsbtu(maxeft, vmlist, tasksinfo, t):
    if len(vmlist[maxeft[0]].tasks)==0:
        return False
    starttime=vmlist[maxeft[0]].tasks[0][1]
    endtime=vmlist[maxeft[0]].tasks[len(vmlist[maxeft[0]].tasks)-1][2]
    res=vmlist[maxeft[0]].computeesteft(t,tasksinfo[0][t-1],maxeft[1])
    taskruntime=res[2]-res[1]
    if taskruntime+endtime-starttime>btu:
        return True
    else:
        return False
    
    #NOTE: uncomment the following and comment the previous for the initial version
    #res=vmlist[maxeft[0]].computeesteft(t,tasksinfo[0][t-1],maxeft[1])
    #r=res[1]%btu
    #assign task to VM with largest EFT if runtime not exceeding BTU
    #if r+res[2]-res[1]<btu:
    #    return False
    #else:
    #    return True    

"""
Checks to see whether a VM has finished its BTU or not
params: the VM list, the index of the VM in the list and the current time (the finish time of a task's predecessors)
return: True if VM has reached its BTU; false otherwise
"""
def hasfinished(vmlist, index, crttime):
    vm = vmlist[index]
    if len(vm.tasks)>0:
        if crttime - vmlist[index].tasks[0][1] > btu:
            return True
        else:
            return False

"""
Determines the large task list by ET in the overall task list
params: the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return the list containing the large task
"""
def __computelargetasklist(tasksinfo):
    dic={}
    for i in range(0,len(tasksinfo[0])):
        dic[i+1]=tasksinfo[0][i]  
    stval=sorted(tasksinfo[0])
    stindex=[]
    for k,v in sorted(dic.iteritems(), key=lambda (k,v): (v,k)):
        stindex.append(k)
    std=numpy.std(stval)
    for i in range(i, len(stval)):
        if stval[i] - stval[i-1] > std:
            large=[]         
            for j in range(i,len(stindex)):
                large.append(stindex[j])
            return large 
    if std<numpy.median(stval):
        return stindex 

"""
Checks whether or not a task is large (by ET)
params: the task, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: true if task is large, false otherwise
"""        
def islarge(t, tasksinfo):
    large=__computelargetasklist(tasksinfo)     
    try:
        if t in large:
            return True
        else:
            return False
    except ValueError:
        return False    

"""
Computes the gain obtained by allocating a faster VM to a task
params: the task, the VM list, the VM type list, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance
return: the gain value
"""    
def computegain(t,vmlist,vmtype,tasksinfo):    
    for vm in vmlist:
        if vm.tasks[0][0]==t:
            vmnew=vms.VM(vmtype)
            etnew=vmnew.computeesteft(t, tasksinfo[0][t-1], 0)[2]-vmnew.computeesteft(t, tasksinfo[0][t-1], 0)[1]
            return (vm.tasks[0][2]-vm.price*vm.tasks[0][1])/vm.speedup - etnew/vmnew.speedup

"""
Computes the rent costs for the entire schedule
params: the VM list
return: the total rent
"""
def computerentcosts(vmlist):
    ct=0
    for vm in vmlist:
        ft=0
        if len(vm.tasks)==0:            
            continue
        t=vm.tasks[len(vm.tasks)-1][2]-vm.tasks[0][1]
        for i in range(1,len(vm.tasks)):
            diff=(vm.tasks[i][1]-vm.tasks[i-1][2])            
            #print vm.tasks[i][1], vm.tasks[i-1][2], (vm.tasks[i][1]-vm.tasks[i-1][2]), diff, t
            if diff>=btu:
                ft+=diff
        c=math.ceil(float(t)/btu-float(ft)//btu)
        ct+=c*vm.price
        #print c*vm.price, vm.tasks
    return ct

"""
Computes the transfer costs for the entire schedule
params: the VM list
return: the total transfer cost
"""   
def computetransfercosts(vmlist,tasksinfo,taskdeplist,vms):   
    tc=0
    for vm in vmlist:
        ft=0
        if len(vm.tasks)==0:
            continue
        t=vm.tasks[len(vm.tasks)-1][2]-vm.tasks[0][1]
        for i in range(1,len(vm.tasks)):
            if vm.location != taskdeplist[vm.tasks[i-1][0]][1][0] and tasksinfo[1][vm.tasks[i-1][0]]>=1024:
                tc+= vms[taskdeplist[vm.tasks[i-1][0]][1][0]+'-m1.small'][1]
        if vm.location != taskdeplist[vm.tasks[len(vm.tasks)-1][0]][1][0] and tasksinfo[1][vm.tasks[len(vm.tasks)-1][0]]>=1024:
                tc+= vms[taskdeplist[vm.tasks[len(vm.tasks)-1][0]][1][0]+'-m1.small'][1]     
    return tc

"""
Computes the idle time for the entire schedule
params: the VM list
return: the total idle time
"""   
def computeidletime(vmlist):
    tidle=0
    for vm in vmlist:
        if len(vm.tasks)>0:
            time=vm.tasks[len(vm.tasks)-1][2]-vm.tasks[0][1]
        else:
            time=btu
        crttime=0
        maxslot=time//btu
        inslot=False
        idle=0
        if time%btu>0:
            maxslot+=1
        if len(vm.tasks)==1:
            idle = btu-vm.tasks[0][2]%(vm.tasks[0][1]+0.00000000000000001)
        else:
            for i in range(1,len(vm.tasks)):
                btuindexprev=vm.tasks[i-1][2]//btu+1
                btuindexcrt=vm.tasks[i][1]//btu+1
                if btuindexprev!=btuindexcrt:
                    idle+=btuindexprev*btu-vm.tasks[i-1][2]
                else:
                    idle+=vm.tasks[i][1]-vm.tasks[i-1][2]
        #print 'VM',idle,crttime
        tidle+=idle
    return tidle    

"""
Computes the schedule makespan
params: the VM list
return: the total idle time
""" 
def computemakespan(vmlist):
    mk=0
    for vm in vmlist:
        if (len(vm.tasks) >0):
            if mk<vm.tasks[len(vm.tasks)-1][2]:
                mk=vm.tasks[len(vm.tasks)-1][2]
    
    return mk

"""
Outputs the result
params: the VM list, JSON output flag, the name of the scheduling strategy, the name of the VM provisioning method
"""
def printvmlist(vmlist,tojson,schedulingstrategy,provisioningstrategy):
    scale=1
    dict={}
    print 'EXPERIMENT RESULTS'
    print 'Scheduling strategy:', schedulingstrategy
    print 'VM provisioning method:', provisioningstrategy 
    if tojson==True:
        dicttmp={'version':''+ str(uuid.uuid4())+'','start_date':''+str(datetime.datetime.now())+'','description':''}
        dict['info']=dicttmp
        #sj+='{"info":{"version":"'+ str(uuid.uuid4())+'","start_date":"'+str(datetime.datetime.now())+'","description":""},'
        dict['nodes']=[]
        #sj+='"nodes":['
    for vm in vmlist:
        if tojson==True:  
            dicttmp={}          
            dicttmp['instance_id']=str(vm.id)
            dicttmp['host']=vm.typer
            dicttmp['scheduling_strategy']=schedulingstrategy
            dicttmp['provisioning_strategy']=provisioningstrategy
            dicttmp['start_date']=vm.tasks[0][1]/scale
            lasttaskusage=((vm.tasks[len(vm.tasks)-1][2]-vm.tasks[0][1]))%btu
            if lasttaskusage>0:
                stoptime=((vm.tasks[len(vm.tasks)-1][2]-vm.tasks[0][1])//btu/+btu)/scale
            dicttmp['stop_date']=stoptime
            dicttmp['boot_time']=0
            dicttmp['predicted_boot_time']=0
            dicttmp['user']='mfrincu'
            dicttmp['jobs']=[]
            
            dict['nodes'].append(dicttmp)            
        print vm.typer, vm.location
        s=''        
        for t in vm.tasks:
            if tojson==True:
                dicttmp={}          
                dicttmp['submission_date']=0
                dicttmp['duration_application']=0
                dicttmp['duration_management']=0
                dicttmp['duration_communication']=0
                dicttmp['real_duration']=(t[2]-t[1])/scale
                dicttmp['id']=t[0]
                dicttmp['duration']='0'
                dicttmp['command_line']='n/a'
                dicttmp['start_date']=t[1]/scale
                dict['nodes'][-1]['jobs'].append(dicttmp)
            s=s+' | '+str(t[0])+' : '+str(t[1])+'->'+str(t[2])
        print s
        
    if tojson==True:
        sj=json.dumps(dict,sort_keys=True,indent=4, separators=(',', ': '))  
        with open(schedulingstrategy+'-'+provisioningstrategy+'.json','w') as f:
            f.write(sj+'\n')
