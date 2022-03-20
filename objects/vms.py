'''
Created on Oct 25, 2012

@author: mfrincu
'''

import uuid

"""
holds information and methods for handling a VM
"""
class VM(object):
    
    """
    a VM has ID, location, type, rent price, transfer out the zone costs, speedup (from the reference type), assigned tasks
    """         
    def __init__(self,props):
        self.id=uuid.uuid4()
        self.typer=props[0]
        self.location=props[4]
        self.price=props[1]
        self.transferout=props[2]
        self.speedup=props[3]
        self.tasks=[]   
    
    """
    calculates the execution start and finish times for a task
    params: task ID,, execution time on the reference model, execution start time
    return: a list containing the number of tasks in the VM including the new one, the execution start and finish times
    """          
    def computeesteft(self,taskid,et,est):
        etr=et/self.speedup
        #print taskid, et, etr, self.typer,self.speedup 
        #find first slack that can fit etr and return it
        #also check that our task est is after the finish time of the previous one
        for i in range(1,len(self.tasks)):
            if self.tasks[i][1]-self.tasks[i-1][2]-1>etr and self.tasks[i-1][2]>est:
                return (i,self.tasks[i-1][2]+1,self.tasks[i-1][2]+1+etr)
        #otherwise add task at the end of task list
        if len(self.tasks)==0:
            return(1,est,est+etr)
        if self.tasks[len(self.tasks)-1][2] < est:
            return(len(self.tasks)+1,est,est+etr)
        return (len(self.tasks)+1,self.tasks[len(self.tasks)-1][2]+1, self.tasks[len(self.tasks)-1][2]+etr+1)
    
    """
    assign a task to this VM
    params: task ID, execution time, execution start time
    return: a list containing the number of tasks in the VM including the new one, the execution start and finish times
    """
    def assigntask(self,taskid,et,est):
        res=self.computeesteft(taskid, et, est)
        self.__assigntasktovm(taskid, res[0], res[1], res[2])
        return res
    
    """
    updates a task's start and finish times
    task ID, execution start time; execution finish time
    """    
    def updatetask(self,taskid,est,eft):
        for i in range(0,len(self.tasks)):
            if self.tasks[i][0]==taskid:
                self.tasks[i][1]=est
                self.tasks[i][2]=eft
    
    """     
    assigns a task to a VM
    task ID, position in the task list, execution start time and execution finishing time
    """         
    def __assigntasktovm(self,taskid,position,est,eft):
        l=[taskid,est,eft]
        self.tasks.insert(position,l) 

#print readVMtypes('data/vm-types.txt')

#print computediversity((1,1,1,1,1,1), (1,1,1,1,0,0))

#r=VM(('m1.small',0.080,0.120,1,'US_East_Virginia'))

#r.assigntasktovm(1, 1, 1, 5)
#r.assigntasktovm(2, 2, 6, 8)
#r.assigntasktovm(3, 3, 11, 12)


#print r.computeesteft(4, 3, 1)
#print r.computeesteft(5, 2, 1)