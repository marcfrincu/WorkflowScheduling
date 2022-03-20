'''
Created on Feb 21, 2013

@author: mfrincu
'''

import random

"""
Creates a random DAG with a given number of levels and tasks per level.
Levels are linked either fully (each task is linked to all predecessors) or randomly
params: the number of levels, the number of tasks per level as an interval and 
a boolean indicated if linkage between levels is full or not
return: the DAG in a format readable by the algorithms  
"""
def graph(nolevels, tasksperlevelinterval, full):
    #generate tasks on each level
    #we return the layers here since computing the layers recursively using the tasks.gettasksperlevel() method is too expensive
    #the layer list can be passed directly as argument to the scheduling algorithm
    layers={}
    lastindex=0
    lg=[]
    for i in range(0,nolevels):
        #quick hack to avoid ending up with 0 initial tasks due to: j in range(1,1)
        tasksperlevel=1
        while tasksperlevel==1:
            tasksperlevel=int(random.uniform(tasksperlevelinterval[0],tasksperlevelinterval[1]))                    
        lg.append([])
        for j in range(1,tasksperlevel+1):
            lg[-1].append(j+lastindex)
            
            layer=layers.get(i,set([]))
            layer.add(j+lastindex)
            layers[i]=layer
            
        lastindex=tasksperlevel+lastindex

    notasks=lastindex
    # link the levels
    # use the samepred method: http://www.kasahara.elec.waseda.ac.jp/schedule/making_e.html
    avgnopred=3
    g=[ [] for i in range(0,lastindex) ]
    lastindex=0    
    for i in range(1,nolevels):
        #link them to some tasks on the previous level to the crt one
        if len(lg[i-1])<=avgnopred or full:
            for k in range(0,len(lg[i-1])):  
                for j in range (0,len(lg[i])):                           
                    g[lastindex+k].append(lg[i][j])
                    #g[__getindex(lg,i,j)].append(lg[i-1][k])
            lastindex=lastindex+len(lg[i-1])
        else:
            for j in range (0,len(lg[i])):
                picked=[]
                n=random.randint(0, len(lg[i-1])-1)
                while (len(picked)<avgnopred):
                    if n not in picked:
                        picked.append(n)
                    n=random.randint(0, len(lg[i-1])-1)
                for k in picked:
                    g[lastindex+k].append(lg[i][j])
                    #g[__getindex(lg,i,j)].append(lg[i-1][k])
            lastindex=lastindex+avgnopred
    return (g,lg[0],notasks,layers)

"""
Retrieves the index of the task based on the level and the position on the level.
This method assumes that task indices are ordered ascending inside each level and
between levels
params: the graph, the level index, the task index in the level
return: the global index of the task
"""
def __getindex(g,level,item):
    index=0
    for i in range(0,level):
        index=index+len(g[i])
    return index+item

#print graph(5, [1,5], False)
    
    