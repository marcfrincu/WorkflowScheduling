#!/usr/bin/python

'''
Created on May 28, 2013

@author: mfrincu
#!/usr/bin/env python

Script for generating and exporting various DAGs for later use in SimSchlouder
'''

import objects.util as outil
import objects.tasks as tasks
import utils.generation as gen
import utils.graph as graph

import sys

def handlePareto(n):
    aet=gen.generateParetoList(n,2,500)
    tci=gen.generateParetoList(n,1.3,800)
    [head,eet]=gen.generateerrorET(aet,200)
    
    tco=list(tci)
    
    return tasks.TasksInfo([aet,tci,eet,tco])
    
def handleWeibull(n):
    aet=gen.generateWeibullList(n,2,4,500)
    tci=gen.generateWeibullList(n,1.3,4,800)
    [head,eet]=gen.generateerrorET(aet,200)
    
    tco=list(tci)
    
    return tasks.TasksInfo([aet,tci,eet,tco])

def handleRandom(etmethod,path):
    #generate random DAG using samepred
    #change the following to obtain different graphs
    nolevels=10
    mintasksperlevel=5
    full=False
        
    (g,initialtasks,nbtotaltasks,layers)=graph.graph(nolevels, [mintasksperlevel,mintasksperlevel+5], full)
    n=nbtotaltasks
    #generate task related times
    t = optionsEt.get(etmethod, lambda : None)(n)
    #link tasks to DAG
    dag=tasks.DAG(t.data,g,initialtasks)
    #print the results in SimSchlouder format
    outil.toSimSchlouderFormat(dag,'RANDOM_'+str(nolevels)+'_'+str(mintasksperlevel)+'_'+str(full),path)

def handleMontage(etmethod,path):
    t = optionsEt.get(etmethod, lambda : None)(24)    
    dag=tasks.DAG(t.data,'data/dag-montage.txt')
    outil.toSimSchlouderFormat(dag,'MONTAGE',path)
    
def handleMapReduce(etmethod,path):
    t = optionsEt.get(etmethod, lambda : None)(17)
    dag=tasks.DAG(t.data,'data/dag-mapreduce.txt')
    outil.toSimSchlouderFormat(dag,'MAPREDUCE',path)

def handleCstem(etmethod,path):
    t = optionsEt.get(etmethod, lambda : None)(15)
    dag=tasks.DAG(t.data,'data/dag-cstem.txt')
    outil.toSimSchlouderFormat(dag,'CSTEM',path)

def handleSequential(etmethod,path):
    t = optionsEt.get(etmethod, lambda : None)(10)
    dag=tasks.DAG(t.data,'data/dag-sequential.txt')
    outil.toSimSchlouderFormat(dag,'SEQUENTIAL',path)


optionsEt = { 'PARETO' : handlePareto,
              'WEIBULL' : handleWeibull
           }    

optionsGraph = { 'RANDOM' : handleRandom,
                 'MONTAGE' : handleMontage,
                 'MAPREDUCE' : handleMapReduce,
                 'CSTEM' : handleCstem,
                 'SEQUENTIAL' : handleSequential
                }

if (len(sys.argv) != 4):
    print 'SimSchlouder DAG generator usage: ./simschlouder.py GRAPH_TYPE EXECUTION_TIME_DISTRIBUTION OUTPUT_PATH'
    print 'GRAPH_TYPE can be: RANDOM, MONTAGE, MAPREDUCE, CSTEM or SEQUENTIAL'
    print 'EXECUTION_TIME_DISTRIBUTION can be: PARETO, WEIBULL. Others can be added in the utils.generation module'
    sys.exit(0)

optionsGraph.get(sys.argv[1], lambda : None)(sys.argv[2], sys.argv[3])


###for random graphs
#nolevels=10
#tasksperlevel=5
#full=False
#(g,initialtasks,nbtotaltasks,layers)=graph.graph(nolevels, [tasksperlevel,tasksperlevel+5], full)
#n=nbtotaltasks

###for predefined graphs
#n=15#for Pareto
#n=24#for MONTAGE DAG
#n=17#for MAPREDUCE
#n=10#for SEQUENTIAL


#dag=tasks.DAG(t.data,g,initialtasks)
#dag=tasks.DAG(t.data,'data/dag-cstem.txt')
#dag=tasks.DAG(t.data,'data/dag-montage.txt')
#dag=tasks.DAG(t.data,'data/dag-mapreduce.txt')
#dag=tasks.DAG(t.data,'data/dag-sequential.txt')


#outil.toSimSchlouderFormat(dag,'CSTEM')