'''
Created on Oct 25, 2012

@author: mfrincu
'''
import sys
import functools

class DAG:
    """
    a DAG has a graph and a list of task information
    params: task information list,the DAG filename (or graph list), (optional) start node list if graph is a list not a filename
    The DAG file contains on the first line the list of start nodes and then on each line
    the dependencies as ones (dependency) or zeros (no dependency) to the other tasks. An
    extra fictive task is added as a last column to link the last task in the DAG to it
    """
    def __init__(self,tasksinfo,graph,startnodes=None):        
        self.tasksinfo=tasksinfo
        
        if startnodes is not None and isinstance (graph, list):
            self.dag=graph
            self.startnodes=startnodes
        else:            
            if isinstance (graph, basestring):
                self.dag=[]        
                fdag = open(graph, 'r')
                self.startnodes = [ int(x) for x in fdag.readline().split() ]
                for line in fdag.readlines():
                    self.dag.append([])
                    l=line.split()
                    for i in range(0,len(l)-1):
                        if int(l[i]) == 1: 
                            self.dag[-1].append(i+1)
        #print self.dag                  
    """   
    determines the successors of a task    
    params: task ID
    return: the empty list if none found or the list otherwise
    """
    def suc(self,taskid):
        return self.dag[taskid-1]

    """
    determines the predecessors of a task    
    params: task ID
    return: the empty list if none found or the list otherwise
    """        
    def pred(self,taskid):
        pred=[]
        for t in range(0,len(self.dag)):
            for tt in self.dag[t]:
                if tt == taskid:
                    pred.append(t+1)
        return pred
    
    """
    computes the priority of each task. Uses a particularity of graphs in which all nodes are connected to every predecessor
    to improve the execution speed for computing the ranks. 
    params: full True if each task is connected to every predecessor, levels the list of levels containing tasks (used if full==True)    
    return: the list of tasks sorted descending by priority
    """     
    def computepriorityallandsortdesc(self, full=False, levels=None):
        priority={}#self.__computepriority4()
        if full:
            priority=self.__computepriorityfast(levels)
        else:        
            for t in range(0,len(self.dag)):
                #priority[t+1] = self.__computepriority3([t+1])
                priority[t+1] = self.__computepriority(t+1)
        l=[]
        for k,v in sorted(priority.iteritems(), key=lambda (k,v): (v,k),reverse=True):
            #l.append((k,v))
            l.append(k)

        return l         

    """
    computes the list of tasks per level. Each level contains a list of parallel tasks
    return: the dictionary having as ID the level and as value the task list
    """   
    def gettasksperlevel(self):
        layers={}
        layers[0]=set(self.startnodes)
        
        alltasks=set([])
        for i in range(1,len(self.dag)+1):
            alltasks.add(i)
        addedtasks=set(self.startnodes)
        
        maxlevel=0
        for l in self.dag:
            for t in l:         
                level=self.__computelevel(t,0)
                i=layers.get(level,set([]))            
                i.add(t)    
                addedtasks.add(t)
                layers[level]=i
                if level>maxlevel:
                    maxlevel=level
                    
        finaltasks=alltasks.difference(addedtasks)
        layers[maxlevel+1]=finaltasks
        
        return layers    
        
    """
    recursive method for computing the priority of a task
    params: the task ID
    return: the priority
    """    
    def __computepriority2(self,taskid):    
        try:    
            if self.suc(taskid) == []:
                return self.tasksinfo[0][taskid-1]
            else:
                maxsuc=0
                for suc in self.suc(taskid):  
                    prioritysuc=self.tasksinfo[1][taskid-1]+self.__computepriority2(suc)
                    if prioritysuc>maxsuc:
                        maxsuc=prioritysuc
                return self.tasksinfo[0][taskid-1]+maxsuc
        except (RuntimeError, TypeError):
            print "__computepriority error", taskid,self.suc(taskid),self.tasksinfo[1][taskid-1]

    """
    recursive method for computing the priority of a task
    params: the task ID
    return: the priority
    """ 
    def __computepriority(self,taskid):
        try:
            rank=functools.partial(self.__computepriority)
            if self.suc(taskid):
                #print taskid, self.suc(taskid)
                return self.tasksinfo[0][taskid-1]+max(self.tasksinfo[1][taskid-1]+rank(suc) for suc in self.suc(taskid))
            else:
                return self.tasksinfo[0][taskid-1]
        except (RuntimeError, TypeError):
            print "__computepriority error", taskid,self.suc(taskid)

    """
    fast method to compute the ranking of tasks if every task is connected to all its predecessors
    params: levels the list of levels containing tasks
    return: the priority
    """
    def __computepriorityfast(self, levels):
        priorities={}
        if levels is None:
            return None
        else:
            for t in levels[len(levels)-1]:
                priorities[t]=self.tasksinfo[0][t-1]
            for l in reversed(range(0,len(levels)-1)):
                for t in levels[l]:
                    priorities[t]=self.tasksinfo[0][t-1]
                    maxsuc=0
                    for s in levels[l+1]:
                        tmp=self.tasksinfo[1][t-1]+priorities[s]
                        if tmp>maxsuc:
                            maxsuc=tmp
                    priorities[t]+=maxsuc
        return priorities

    """
    recursive method for computing the level of a task
    params: task ID, level number
    return: the level of the task
    """    
    def __computelevel(self, t, level):
        pred=self.pred(t)
        if  len(pred)==0:
            return level
        else:
            m=0            
            for i in pred:
                crt=self.__computelevel(i,level+1)                
                if m<crt:
                    m=crt
            return m 

"""                          
holds information regarding the tasks' execution and transfer times on the reference VM
"""
class TasksInfo:
    def __init__(self,namelist):      
        self.data=[] #1st ET, 2nd size
        if isinstance (namelist, list):
            for item in namelist:
                self.data.append(item)            
        else:            
            if isinstance (namelist, tuple):
                for name in namelist:
                    l=[]
                    f = open(name, 'r')
                    line = f.readline()
                    for i in line.split():
                        l.append(float(i))
                    self.data.append(l)     

#t=TasksInfo(('../data/et-tasks-cstem.txt','../data/size-tasks-cstem.txt'))
#print t.data

#dag=DAG(t.data,'../data/dag-cstem.txt')
#print dag.dag
#print dag.startnodes
#print dag.suc(1)
#print dag.pred(4)
#print dag.computepriorityallandsortdesc(True,dag.gettasksperlevel())
#print dag.gettasksperlevel()