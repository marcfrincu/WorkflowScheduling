'''
Created on Dec 12, 2012

@author: mfrincu
'''

import objects.tasks as tasks
from objects.util import allocatevm
from objects.util import allocatevm2
from util import *
from config import *
import math as math

from time import clock,localtime


"""
Implementation of the OneVMperTask algorithm: each task is scheduled to a new VM
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""
def onevmpertask(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]
    #create a VM for each parallel start task and assign the tasks to them
    for i in range(0,len(dag.startnodes)):
        if dataintensive:
            vm=vms.VM(allocatevm(i+1,vml,taskdeplist,tasksinfo))
        else:
            vm=vms.VM(allocatevm2(i+1,vmtypeprops,tasksinfo))
        l=vm.assigntask(i+1,tasksinfo[0][i],computetransfertime(i+1,tasksinfo))
        vmlist.append(vm)
    #for the rest of the tasks schedule all tasks based on rank
    if layers is None:
        tasks=dag.computepriorityallandsortdesc()
    else:
        tasks=layers
    for t in tasks:
        if t not in dag.startnodes:
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,eft)
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            if dataintensive:
                vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            else:
                vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
            vm.assigntask(t,tasksinfo[0][t-1],maxeft[1])
            vmlist.append(vm)
    return vmlist

"""
Implementation of the OneVMforAll algorithm: All tasks are ordered and scheduled on a single VM
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""
def onevmforall(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]
    #create one VM
    if dataintensive:
        vm=vms.VM(allocatevm(1,vml,taskdeplist,tasksinfo))
    else:
        vm=vms.VM(allocatevm2(1,vmtypeprops,tasksinfo))
    l=vm.assigntask(1,tasksinfo[0][0],computetransfertime(1,tasksinfo))
    vmlist.append(vm)
    #for the rest of the tasks schedule all tasks based on rank
    if layers is None:
        tasks=dag.computepriorityallandsortdesc()
    else:
        tasks=layers    
    for t in tasks:        
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,eft)
            for tp in pred:                
                for l in vmlist[0].tasks:
                    if tp==l[0] and maxeft[1]<l[2]:
                        maxeft=[vmlist[0],l[2]]
            #assign task to VM with largest EFT
            if dataintensive:
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            vmlist[0].assigntask(t,tasksinfo[0][t-1],maxeft[1])
    return vmlist

"""
Implementation of the StartParExceed algorithm: each initial parallel task is scheduled to a new VM. If a task's EET exceeds the BTU then the existing VM's rent is extended by another BTU
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""
def startparexceed(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]
    #create a VM for each parallel start task and assign the tasks to them
    for i in range(0,len(dag.startnodes)):
        if dataintensive:
            vm=vms.VM(allocatevm(i+1,vml,taskdeplist,tasksinfo))
        else:
            vm=vms.VM(allocatevm2(i+1,vmtypeprops,tasksinfo))
        l=vm.assigntask(i+1,tasksinfo[0][i],computetransfertime(i+1,tasksinfo))
        vmlist.append(vm)
    #for the rest of the tasks schedule all tasks based on rank
    if layers is None:
        tasks=dag.computepriorityallandsortdesc()
    else:
        tasks=layers
    for t in tasks:
        if t not in dag.startnodes:
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,eft)
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            #assign task to VM with largest EFT
            if dataintensive:
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            vmlist[maxeft[0]].assigntask(t,tasksinfo[0][t-1],maxeft[1])
    return vmlist

"""
Implementation of the StartParExceed algorithm: each initial parallel task is scheduled to a new VM. If a task's EET exceeds the BTU then a new VM is rented
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""
def startparnotexceed(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]
    #create a VM for each parallel start task and assign the tasks to them
    for i in range(0,len(dag.startnodes)):       
        if dataintensive:
            vm=vms.VM(allocatevm(i+1,vml,taskdeplist,tasksinfo))
        else:
            vm=vms.VM(allocatevm2(i+1,vmtypeprops,tasksinfo))
        l=vm.assigntask(i+1,tasksinfo[0][i],computetransfertime(i+1,tasksinfo))
        vmlist.append(vm)
    #for the rest of the tasks schedule all tasks based on rank
    if layers is None:
        tasks=dag.computepriorityallandsortdesc()
    else:
        tasks=layers
    for t in tasks:
        if t not in dag.startnodes:
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,eft)
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]            
            if dataintensive:
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            #assign task to VM with largest EFT if runtime not exceeding BTU                
            if not exceedsbtu(maxeft, vmlist, tasksinfo, t):                
                vmlist[maxeft[0]].assigntask(t,tasksinfo[0][t-1],maxeft[1])
            else:#otherwise allocate task to new VM
                if dataintensive:
                    vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                else:
                    vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                vm.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                vmlist.append(vm)
    return vmlist

"""
Implementation of the AllParExceed algorithm: each parallel task is scheduled to a new VM. If a task's EET exceeds the BTU then the existing VM's rent is extended by another BTU
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def allparexceed(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]    
#    if layers is None:    
    levels=dag.gettasksperlevel()
#    else:
#        levels=layers
    for k in levels.keys():
        #order tasks from largest to smallest
        tasks=sorttasksbyetasc(levels[k],tasksinfo)
        tasks.reverse()
        #tasks=levels[k]
        vmassigned=[]
        longesttask=()
        for t in tasks:
            if len(vmlist)==0:
                if dataintensive:
                    vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))                
                else:
                    vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))            
                vmlist.append(vm)
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,est)            
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            if dataintensive:
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            #assign task to VM with largest EFT
            if maxeft[0] not in vmassigned:         
                if dataintensive:
                    maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)       
                res=vmlist[maxeft[0]].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                vmassigned.append(maxeft[0])    
            else:#search a VM free by task est
                found=False
                #indexvmmin=-1
                #mindiff=1000*btu
                for j in range(0,len(vmlist)):
                    vm=vmlist[j]
                    if j not in vmassigned:
                        eft=vm.tasks[len(vm.tasks)-1][2]
                        est=vm.tasks[0][1]
                        a=math.ceil((eft-est)/btu)
                        b=math.ceil((maxeft[1]-est)/btu)
                        
                        if longesttask!=():
                            data=vmlist[j].computeesteft(t,tasksinfo[0][t-1],eft)
                            if eft+data[2]-data[1]<longesttask[2] and not found:
                                res=vmlist[j].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                                vmassigned.append(j)
                                found=True
                        else:
                            if eft<maxeft[1] and a==b and not found:
                        #diff=eft-est+int(eft-est)/int(btu)*btu  
                        #if eft<maxeft[1] and (diff<btu):
                        #    if diff<mindiff:
                                #mindiff=diff
                                #indexvmmin=j
                                res=vmlist[j].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                                vmassigned.append(j)
                                found=True
                #if indexvmmin!=-1:
                #    vmlist[indexvmmin].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                #    vmassigned.append(indexvmmin)
                if not found:#add new VM
                    if dataintensive:
                        vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                    else:                                 
                        vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                    #if no suitable VM has been found start a new one for the task but as late as possible
                    #the eft of the task should be the same as the eft of the longest task already started  
                    if longesttask!=():
                        data=vm.computeesteft(t,tasksinfo[0][t-1],longesttask[2])
                        res=vm.assigntask(t,tasksinfo[0][t-1],longesttask[2]-(data[2]-data[1]))
                    else:#if the longest task has not been deployed then deploy it                  
                        res=vm.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                    vmlist.append(vm)
            if (longesttask==()):
                longesttask=res
    return vmlist

"""
Implementation of the AllParNotExceed algorithm: each parallel task is scheduled to a new VM. If a task's EET exceeds the BTU then a new VM is rented
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def allparnotexceed(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]
#    if layers is None:    
    levels=dag.gettasksperlevel()
 #   else:
 #       levels=layers   
    for k in levels.keys():        
        tasks=sorttasksbyetasc(levels[k],tasksinfo)  
        vmassigned=[]
        for t in tasks:            
            if len(vmlist)==0:
                if dataintensive:
                    vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))                
                else:
                    vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))            
                vmlist.append(vm)
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)
            maxeft=[0,0]#(id,est)            
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            #assign task to VM with largest EFT
            if dataintensive:
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)      
            if maxeft[0] not in vmassigned and not exceedsbtu(maxeft, vmlist, tasksinfo, t):                
                vmlist[maxeft[0]].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                vmassigned.append(maxeft[0])            
            else:#search a VM free by task est
                indexvmmin=-1
                mindiff=1000*btu
                if not dataintensive:
                    for j in range(0,len(vmlist)):
                        if j not in vmassigned and not hasfinished(vmlist, j, maxeft[1]):
                            vm=vmlist[j]
                            if len(vm.tasks)>0:
                                eft=vm.tasks[len(vm.tasks)-1][2]
                                est=vm.tasks[0][1]
                            else:
                                eft=0
                                est=0
                            diff=eft-est+int(eft-est)/int(btu)*btu
                            if eft<maxeft[1] and not exceedsbtu([j,maxeft[1]], vmlist, tasksinfo, t) and (diff<btu):
                                if diff<mindiff:                                    
                                    mindiff=diff
                                    indexvmin=j              
                                
                else:
                    for j in range(0,len(vmlist)):
                        vmdummy=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                        if j not in vmassigned and vmlist[j].location==vmdummy.location:
                            vmlist[j].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                            vmassigned.append(j)
                            found=True
                if indexvmmin!=-1:
                    vmlist[indexvmmin].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                    vmassigned.append(indexvmmin)
                if indexvmmin==-1 or (exceedsbtu(maxeft, vmlist, tasksinfo, t) and indexvmmin==-1):#add new VM
                    if dataintensive:
                        vm=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                        maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
                    else:                        
                        vm=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                    vm.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                    vmlist.append(vm)
    return vmlist

"""
Implementation of the AllPar1LnS algorithm: parallelism is reduced by executing small parallel tasks sequentially on the same machine which runs parallel to the longest task. 
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def allpar1lns(dag,tasksinfo,vml,taskdeplist,layers=None):
    vmlist=[]    
    #if layers is None:    
    levels=dag.gettasksperlevel()
    #else:
    #    levels=layers
    for k in levels.keys():
        tasks=sorttasksbyetasc(levels[k],tasksinfo)
        #tasks=sorttasksbyrank(levels[k],dag)
        if tasks==[]:
            continue               
        longesttask=tasks[-1]
        if dataintensive:
            vmlong=vms.VM(allocatevm(longesttask,vml,taskdeplist,tasksinfo))            
        else:
            vmlong=vms.VM(allocatevm2(longesttask,vmtypeprops,tasksinfo))
        
        pred=dag.pred(longesttask)
        maxeft=[0,0]#(id,est)            
        for tp in pred:
            for vm in range(0,len(vmlist)):
                for l in vmlist[vm].tasks:
                    if tp==l[0] and maxeft[1]<l[2]:
                        maxeft=[vm,l[2]]                
        if dataintensive:
            maxlongtaskest=maxeft[1]+2*computetransfertime(longesttask,tasksinfo)
        else:
            maxlongtaskest=maxeft[1]
            
        for i in range(0,len(tasks)):            
            #compute largest EFT of parent tasks            
            t=tasks[i]                    
            pred=dag.pred(t)
            #print 't',t, pred
            maxeft=[0,0]#(id,est)            
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]                
            if dataintensive:                
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
            #if i!=0 and (longesttask!=t or len(tasks)==1) and vmlist[-1].computeesteft(t,tasksinfo[0][t-1],maxeft[1])[2]<vmlong.computeesteft(longesttask,tasksinfo[0][longesttask-1],maxlongtaskest)[2]:            
            if (len(vmlist)!=0 and not exceedsbtu([len(vmlist)-1,maxeft[1]], vmlist, tasksinfo, t)) and vmlist[-1].computeesteft(t,tasksinfo[0][t-1],maxeft[1])[2]<=vmlong.computeesteft(longesttask,tasksinfo[0][longesttask-1],maxlongtaskest)[2]:
                #print 'IF',t
                vmlist[-1].assigntask(t,tasksinfo[0][t-1],maxeft[1])
            else:
                #print 'ELSE', t
                #if longesttask!=t or (i==0 and len(tasks)==1):
                if len(vmlist)==0:
                    #print 'NO VMS->ADD 1ST', t                    
                    if dataintensive:
                        vmj=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                    else:
                        vmj=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                    vmj.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                    vmlist.append(vmj)                   
                else:
                    found=False
                    #print 'TRY EXISTING VM', t
                    exceedbtu=False
                    for j in range(0,len(vmlist)):                       
                        vmj=vmlist[j]
                        eft=vmj.tasks[len(vmj.tasks)-1][2]                        
                        exceedbtu=exceedsbtu([j,maxeft[1]], vmlist, tasksinfo, t)
                        if eft<=maxeft[1] and not found and not exceedbtu:
                            vmlist[j].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                            found=True
                            break

                    if not found or exceedbtu:#add new VM
                        #print 'NEW VM', t
                        if dataintensive:
                            vmj=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                        else:
                            vmj=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                        vmj.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                        vmlist.append(vmj)
        #if len(tasks)>1:   
        #    print 'LONGEST',longesttask     
        #    vmlong.assigntask(longesttask,tasksinfo[0][longesttask-1],maxlongtaskest)
        #    vmlist.append(vmlong)    
    return vmlist

"""
Implementation of the AllPar1LnS algorithm: extension of AllPar1LnS in which the speed of the VM holding the longest task is increased to execute it faster. This happens within a budget dictated by the amount spent if scheduling using the AllParNotExceed algorithm   
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def allpar1lnsdyn(dag,tasksinfo,vml,taskdeplist,layers=None):
    global desiredfast    
    vmlist=[]    
    #if layers is None:    
    levels=dag.gettasksperlevel()
    #else:
    #    levels=layers       
    for k in levels.keys():
        #phase 1: schedule using AllParNotExceed version           
        tasks=sorttasksbyetasc(levels[k],tasksinfo)
        #tasks=sorttasksbyrank(levels[k],dag)
        if tasks==[]:
            continue        
                
        crtlevelcost=0    
        maxlevelcost=0   
        longesttask=tasks[-1]
        #print 'LONGEST',longesttask
        if dataintensive:            
            for t in tasks:
                vmlong=vms.VM(allocatevm(longesttask,vml,taskdeplist,tasksinfo))
                print t, vmlong.typer, vmlong.location
                maxlevelcost=maxlevelcost+vmlong.price
        else:
            vmlong=vms.VM(allocatevm2(longesttask,vmtypeprops,tasksinfo))    
            maxlevelcost=len(tasks)*vmlong.price
       
        pred=dag.pred(longesttask)
        maxeft=[0,0]#(id,est)            
        for tp in pred:
            for vm in range(0,len(vmlist)):
                for l in vmlist[vm].tasks:
                    if tp==l[0] and maxeft[1]<l[2]:
                        maxeft=[vm,l[2]]                
        if dataintensive:
            maxlongtaskest=maxeft[1]+2*computetransfertime(longesttask,tasksinfo)
        else:
            maxlongtaskest=maxeft[1]
        
        levelvmslistids=[]
        for i in range(0,len(tasks)):            
            #compute largest EFT of parent tasks            
            t=tasks[i]                    
            pred=dag.pred(t)
            maxeft=[0,0]#(id,est)            
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]                
            if dataintensive:                
                maxeft[1]=maxeft[1]+2*computetransfertime(t,tasksinfo)
                
            #if (len(vmlist)!=0 and vmlist[-1].typer==desiredfast) and i!=0 and (longesttask!=t or len(tasks)==1) and vmlist[-1].computeesteft(t,tasksinfo[0][t-1],maxeft[1])[2]<vmlong.computeesteft(longesttask,tasksinfo[0][longesttask-1],maxlongtaskest)[2]:
            if (len(vmlist)!=0 and not exceedsbtu(maxeft, vmlist, tasksinfo, t)) and vmlist[-1].computeesteft(t,tasksinfo[0][t-1],maxeft[1])[2]<=vmlong.computeesteft(longesttask,tasksinfo[0][longesttask-1],maxlongtaskest)[2]:
                vmlist[-1].assigntask(t,tasksinfo[0][t-1],maxeft[1])
                if vmlist[-1].id not in levelvmslistids:
                    levelvmslistids.append(vmlist[-1].id)
                if t==longesttask:
                    vmlong=vmlist[-1]
            else:
                #print 'ELSE', t
                #if longesttask!=t or (i==0 and len(tasks)==1):
                if len(vmlist)==0:
                    #print 'NO VMS->ADD 1ST', t                    
                    if dataintensive:
                        vmj=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                    else:
                        vmj=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                    vmj.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                    vmlist.append(vmj)
                    levelvmslistids.append(vmlist[-1].id)
                    if t==longesttask:
                        vmlong=vmj          
                else:
                    found=False
                    #print 'TRY EXISTING VM', t
                    exceedbtu=False
                    for j in range(0,len(vmlist)):                       
                        vmj=vmlist[j]
                        eft=vmj.tasks[len(vmj.tasks)-1][2]                        
                        exceedbtu=exceedsbtu([j,maxeft[1]], vmlist, tasksinfo, t)
                        if eft<=maxeft[1] and not found and not exceedbtu and vmj.typer=='m1.small':
                            vmlist[j].assigntask(t,tasksinfo[0][t-1],maxeft[1])   
                            if vmlist[j].id not in levelvmslistids:
                                levelvmslistids.append(vmlist[j].id)
                            found=True
                            if t==longesttask:
                                vmlong=vmlist[j]
                            break
                    #print found, exceedbtu
                    if not found or exceedbtu:#add new VM
                        #print 'NEW VM', t
                        if dataintensive:
                            desiredfast='m1.small'
                            vmj=vms.VM(allocatevm(t,vml,taskdeplist,tasksinfo))
                        else:
                            vmj=vms.VM(allocatevm2(t,vmtypeprops,tasksinfo))
                        vmj.assigntask(t,tasksinfo[0][t-1],maxeft[1])
                        levelvmslistids.append(vmj.id)
                        vmlist.append(vmj)
                        if t==longesttask:
                            vmlong=vmj
                                    
        #print 'RC before',computerentcosts(vmlist)
        #phase 2: reduce parallelism and increase speed
        crtpricelist=[]
        crttypelist=[]
        for idvm in levelvmslistids:
            vm=__getvmbyid(idvm,vmlist)
            crtlevelcost+=vm.price
            
            crtpricelist.append(vm.price)
            crttypelist.append(vm.location)
                    
        #print 'maxprice',maxlevelcost,'crtprice',crtlevelcost, 'VMs on level worst price', crtpricelist, 'VMs on level type', crttypelist
        
        if len(tasks)>1:
            #print 'VM long tasks',vmlong.tasks
            #print 'VM long', vmlong.typer, vmlong.location
                    
            etlongest=0
            for i in range(0, len(vmlong.tasks)):
                if vmlong.tasks[i][0]==longesttask: 
                    etlongest=vmlong.tasks[i][2]-vmlong.tasks[i][1]
                    break
            etsecondlongest=0
            vmsecondlongest=None
            #print 'longest',vmlong.id,etlongest
            for idvm in levelvmslistids:                
                etsum=0            
                vm=__getvmbyid(idvm,vmlist)
                if vm.id == vmlong.id:
                    continue
                for t in vm.tasks:                    
                    for tt in tasks:
                        if tt in t:
                            etsum+=t[2]-t[1]
                if etsecondlongest<=etsum:
                    etsecondlongest=etsum
                    vmsecondlongest=vm
            #print 'second longest', vmsecondlongest.id, etsecondlongest
            
            increasedvm2=None    
            etincrease2=0     
            step=0   
            while maxlevelcost>crtlevelcost and etlongest>=etsecondlongest:# and computerentcosts(vmlist)<0.8:
                #print 'STEP',step
                #print 'TMP RC',computerentcosts(vmlist)
                step+=1
                type=__getnextvmtype(vmlong.typer,vmtypeprops)
                if type==None:
                    break                
                else:
                    if dataintensive:
                        desiredfast=type[0]
                        increasedvm=vms.VM(allocatevm(vmlong.tasks[-1][0],vml,taskdeplist,tasksinfo))
                        #print 'increased first', increasedvm.typer, increasedvm.location

                    else:
                        increasedvm=vms.VM(type)
                    #print 'maxCost', maxlevelcost, 'crtCost', crtlevelcost
                    #print 'ETlongest', etlongest, 'ETsecondlongest', etsecondlongest
                                                      
                    costincrease=increasedvm.price
                    etnew=increasedvm.computeesteft(vmlong.tasks[-1][0],vmlong.tasks[-1][2]-vmlong.tasks[-1][1],vmlong.tasks[-1][1])
                    etincrease=etnew[2]-etnew[1]
                    #print 'trying to speedup ETlongest to', etincrease  
                    #print 'second potential speedup', etincrease2
                    costafter=crtlevelcost+costincrease
                    if len(vmlong.tasks)==1:
                        costafter=costafter-vmlong.price
                    if costafter<=maxlevelcost:
                        # assume second longest ET can be i,proved
                        if (vmsecondlongest == None):
                            break
                        type=__getnextvmtype(vmsecondlongest.typer,vmtypeprops)
                        if type==None:
                            break                                                       
                        if dataintensive:
                            desiredfast=type[0]
                            increasedvm2=vms.VM(allocatevm(vmsecondlongest.tasks[-1][0],vml,taskdeplist,tasksinfo))
                            #print 'increased second', increasedvm2.typer, increasedvm2.location
                        else:
                            increasedvm2=vms.VM(type)
                        costincrease2=increasedvm2.price                                                                                                    
                        for j in range(0,len(vmsecondlongest.tasks)):
                            t=vmsecondlongest.tasks[j]                                
                            if t[0] in tasks and len(increasedvm2.tasks)!=0:                                                                        
                                est=increasedvm2.tasks[len(increasedvm2.tasks)-1][2]
                                increasedvm2.assigntask(t[0],tasksinfo[0][t[0]-1],est)
                                #print 'added task to increased second vm',t[0],est,increasedvm2.tasks[-1][2]-increasedvm2.tasks[-1][1] 
                            else:               
                                est=vmsecondlongest.tasks[j][1]
                                if t[0] in tasks and len(increasedvm2.tasks)==0:
                                    increasedvm2.assigntask(t[0],tasksinfo[0][t[0]-1],est)
                                    #print 'added first task to increased second vm',t[0],est,increasedvm2.tasks[-1][2]-increasedvm2.tasks[-1][1]
                        if len(increasedvm2.tasks)>0:
                            etincrease2=increasedvm2.tasks[len(increasedvm2.tasks)-1][2]-increasedvm2.tasks[0][1]
                        #print 'trying to speedup ETsecondlongest to',  etincrease2
                        
                        #if longest increase better than previous second or current second and resulting price still smaller
                        if etincrease>=etsecondlongest or (etincrease>=etincrease2 and crtlevelcost-vmsecondlongest.price-vmlong.price+costincrease+costincrease2<=maxlevelcost):                        
                            crtlevelcost=costafter
                            etlongest=etincrease
                            tasksvm=__removevm(vmlong.id,vmlist)                            
                            increasedvm.assigntask(tasksvm[0][0],tasksinfo[0][tasksvm[0][0]-1], tasksvm[0][1])
                            vmlist.append(increasedvm)
                            vmlong=increasedvm                            
                            #print 'new crtCost', costafter

                            costafter=crtlevelcost+costincrease2
                            if len(vmsecondlongest.tasks)!=len(increasedvm2.tasks):
                                costafter=costafter-vmsecondlongest.price
                            -vmsecondlongest.price
                            if costafter<=maxlevelcost:
                                #print 'removed tasks from second longest',__removetasksfromvm(vmsecondlongest.id,vmlist,tasks)
                                if len(vmsecondlongest.tasks)==0:
                                    __removevm(vmsecondlongest.id,vmlist)
                                vmlist.append(increasedvm2)   
                                crtlevelcost=costafter
                                vmsecondlongest=increasedvm2
                                etsecondlongest=etincrease2
                                #print 'new crtCost', costafter
                        else:
                            break        
                        if (etincrease<etincrease2 and len(increasedvm2.tasks)>0):
                            break
                        
                    else:
                        break
                #print 'RC after',computerentcosts(vmlist)

    return vmlist   

"""
Implementation of the Gain algorithm: computes a gain matrix for each task on every VM and increases (within a budget) the speed of the task with the biggest value    
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def gain(dag,tasksinfo,vml,taskdeplist,layers=None):
    global desiredfast
    #initial schedule using the OneVMperTask strategy
    vmlist=onevmpertask(dag,tasksinfo,vml,taskdeplist)
    a=[]
    # compute the gain matrix 'a'
    for i in range(0,len(tasksinfo[0])):        
        a.append([])      
        for j in range(0,len(vmtypeprops)):
            a[i].append([])          
            for k in range(0,len(vmlist)):                                
                if vmlist[k].tasks[0][0]==i+1 and vmtypeprops[j][0]==vmlist[k].typer:
                    a[i][j]=0
                else:
                    timeold=0
                    cold=0
                    for k in range(0,len(vmlist)):
                        if vmlist[k].tasks[0][0]==i+1:
                            timeold=vmlist[k].tasks[0][2]-vmlist[k].tasks[0][1]
                            cold=vmlist[k].speedup
                    if dataintensive:
                        desiredfast=vmtypeprops[j][0]
                        vm=vms.VM(allocatevm(i+1, vml, taskdeplist, tasksinfo))
                    else:
                        vm=vms.VM(vmtypeprops[j])                        
                    timenew=vm.computeesteft(i+1, tasksinfo[0][i], 0)[2]-vm.computeesteft(i+1, tasksinfo[0][i], 0)[1]
                    cnew=vm.speedup                    
                    a[i][j]=(timeold-timenew)/(cnew-cold+0.000000000001)
                    
    B=4*computerentcosts(vmlist)
    cost=0
    cont=True
    reassignedtasks=[]
    iter=0
    # attempt to increase speed within budget 'B'
    while cost<B and cont==True and iter<1000:
        iter+=1
        maxtask=-1
        maxvm=-1
        maxa=0
        for i in range(0,len(tasksinfo[0])):  
            if i+1 not in reassignedtasks:
                for j in range(0,len(vmtypeprops)):
                    if a[i][j]>maxa:
                        maxa=a[i][j]
                        maxtask=i+1
                        maxvm=j
        if maxtask != -1:
            for k in range(0,len(vmlist)):  
                if vmlist[k].tasks[0][0]==maxtask:
                    vmlist.pop(k)
                    break
                    
            pred=dag.pred(maxtask)            
            maxeft=[0,0]#(id,est)    
            for tp in pred:
                for vmi in range(0,len(vmlist)):
                    for l in vmlist[vmi].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vmi,l[2]]
            if dataintensive:
                desiredfast=vmtypeprops[maxvm][0]
                vm=vms.VM(allocatevm(maxtask, vml, taskdeplist, tasksinfo))
            else:
                vm=vms.VM(vmtypeprops[maxvm])
            vm.assigntask(maxtask, tasksinfo[0][maxtask-1], maxeft[1])                        
            vmlist.append(vm)
            reassignedtasks.append(maxtask)
            cost=computerentcosts(vmlist)
    
    #update est and eft    
    tasks=dag.computepriorityallandsortdesc()
    for t in tasks:
        if t not in dag.startnodes:
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)            
            maxeft=[0,0]#(id,eft)
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            vmt=vmlist[0]
            for vm in vmlist:
                if vm.tasks[0][0]==t:
                    vmt=vm 
            vmt.updatetask(t, maxeft[1], maxeft[1]+vmt.tasks[0][2]-vmt.tasks[0][1])
    return vmlist
                        
    return vmlist
        
"""
Implementation of the CPA-Eager algorithm: tries to increase the speed of the tasks situated on the critical path, i.e., the path that gives the makespan    
params: the DAG structure, the list of task execution times (tasksinfo[0]) and size (tasksinfo[1]) for the default EC2.small instance, the list of VM properties, the list of task data dependencies
return: the list of VMs containing the assigned tasks
"""  
def cpaeager(dag,tasksinfo,vml,taskdeplist,layers=None):
    global desiredfast
    #initial schedule using the OneVMperTask strategy   
    vmlist=onevmpertask(dag,tasksinfo,vml,taskdeplist)
    #compute cost 'Bi'
    Bi=0
    total=0
    cost=computerentcosts(vmlist)    
    for vm in vmlist:        
        et=vm.tasks[-1][2]-vm.price*vm.tasks[0][1]
        total+=vm.price*(btu+et)/2
    Bi=2*cost#we could use another cost for Bi... ideas?
    
    #get tasks on critical path
    cp=computeCPtasks(dag,tasksinfo)
    Bp=(Bi*total)/(cp[0]*cost)
    
    tcp=cp[0]
    ta=total/Bp
    iter=0
    #attempt to increase speed
    while tcp>ta and cost<=Bi and iter<1000:
        iter+=1        
        gain=[0,[],0]
        for t in cp[1]:
            for vmtype in vmtypeprops:
                if gain[2]<computegain(t,vmlist,vmtype,tasksinfo):
                    gain=[t,vmtype,computegain(t,vmlist,vmtype,tasksinfo)]
                    
        for i in range(0,len(vmlist)+1):
            if vmlist[i].tasks[0][0]==gain[0]:
                vmlist.pop(i)
                if dataintensive:
                    desiredfast=gain[1][0]
                    vm=vms.VM(allocatevm(gain[0], vml, taskdeplist, tasksinfo))
                else:         
                    vm=vms.VM(gain[1])
                pred=dag.pred(gain[0])
                maxeft=[0,0]#(id,est)    
                for tp in pred:
                    for vmi in range(0,len(vmlist)):
                        for l in vmlist[vmi].tasks:
                            if tp==l[0] and maxeft[1]<l[2]:
                                maxeft=[vmi,l[2]]
                vm.assigntask(gain[0],tasksinfo[0][gain[0]-1],maxeft[1])
                vmlist.append(vm)
                break
          
        cp=computeCPtasks(dag,tasksinfo)
        total=0
        cost=computerentcosts(vmlist)
        for vm in vmlist:        
            et=vm.tasks[-1][2]-vm.price*vm.tasks[0][1]
            total+=vm.price*(btu+et)/2
        tcp=cp[0]
        Bp=(Bi*total)/(cp[0]*cost)
        ta=total/Bp      
        break
    
    #update est and eft    
    tasks=dag.computepriorityallandsortdesc()
    for t in tasks:
        if t not in dag.startnodes:
            #compute largest EFT of parent tasks        
            pred=dag.pred(t)            
            maxeft=[0,0]#(id,eft)
            for tp in pred:
                for vm in range(0,len(vmlist)):
                    for l in vmlist[vm].tasks:
                        if tp==l[0] and maxeft[1]<l[2]:
                            maxeft=[vm,l[2]]
            vmt=vmlist[0]
            for vm in vmlist:
                if vm.tasks[0][0]==t:
                    vmt=vm 
            vmt.updatetask(t, maxeft[1], maxeft[1]+vmt.tasks[0][2]-vmt.tasks[0][1])
    return vmlist

"""
Determines the dominant region, i.e., the region occurring most frequently, in the VM list
params: the VM list
return: the region ID
"""
def __finddominantregion(vmlist):
    count={}
    for vm in vmlist:
        if count[vm.location]==None:
            count[vm.location]=0
        count[vm.location]=count[vm.location]+1
    key,value=max(count.iteritems(), key=lambda x:x[1])
    return key

"""
Removes a VM from the VM list
params: the ID of the VM to be removed, the VM list
returns: the task list assigned to the VM if VM exists or None otherwise
"""
def __removevm(idvm,vmlist):
    for i in range(0,len(vmlist)):
        if vmlist[i].id==idvm:
            tasks=vmlist[i].tasks
            vmlist.pop(i)
            return tasks
    return None

"""
Attempts to remove a list of tasks from the ones assigned to a VM
params: the VM ID; the VM list, the list of tasks to be removed
return: the list of tasks successfully removed
"""
def __removetasksfromvm(idvm,vmlist,tasklist):
    tasks=[]
    for vm in vmlist:
        if vm.id==idvm:
            i=0
            while i<len(vm.tasks):
                if vm.tasks[i][0] in tasklist:
                    tasks.append(vm.tasks[i])
                    vm.tasks.pop(i)
                else:
                    i+=1
    return tasks

"""
Determines the next VM type in the VM type list
params: the current VM type, the VM type list
return: the next VM type or None if current type is last
"""
def __getnextvmtype(crt,vmtypeprops):
    for i in range(0,len(vmtypeprops)):
        if vmtypeprops[i][0]==crt and i<len(vmtypeprops)-1:
            return vmtypeprops[i+1]
    return None

"""
Returns a VM object from the VM list
params: the VM ID that is searched for, the VM list
"""
def __getvmbyid(id,vmlist):
    for vm in vmlist:
        if vm.id==id:
            return vm
    return None
