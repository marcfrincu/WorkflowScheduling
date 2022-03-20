'''
Created on Oct 31, 2012

@author: mfrincu

Module containing some experiments for determining the performance of
the workflow algorithms
'''

import objects.util as utilvms
import objects.tasks as tasks
import algorithms.algs as acpu
import utils.graph as graph
import utils.generation as generation
import config
import random
import math
from collections import defaultdict
from collections import Counter
from time import clock,localtime

"""
Computes the utility of each algorithm against the default one: OneVMperTask
The utility is defined as (makespan_algorithm * cost_reference) / (makespan_reference * cost_algorithm);
or in other words makespanloss_algorithm * $gain_algorithm. The reference one is optimal in terms of
makespan and is the worst in terms of cost, hence any algorithm will cost less but take more time to
execute the tasks. 
"""
def experimentUtility():

    (vmlist, locationlist)=utilvms.readVMtypes('data/vm-types.txt')
    taskdeplist={1: (('NorthAmerica_USWest_Oregon', 2, 1, 1), ('NorthAmerica_USWest_Oregon', 2, 1, 1)), 2: (('Asia_Japan_Tokio', 2, 2, 5), ('Asia_Japan_Tokio', 2, 2, 5)), 3: (('EU_Ireland_Dublin', 1, 2, 4), ('EU_Ireland_Dublin', 1, 2, 4)), 4: (('EU_Ireland_Dublin', 2, 2, 1), ('EU_Ireland_Dublin', 2, 2, 1)), 5: (('NorthAmerica_USWest_California', 1, 1, 5), ('NorthAmerica_USWest_California', 1, 1, 5)), 6: (('NorthAmerica_USWest_California', 1, 2, 1), ('NorthAmerica_USWest_California', 1, 2, 1)), 7: (('NorthAmerica_USWest_Oregon', 2, 2, 3), ('NorthAmerica_USWest_Oregon', 2, 2, 3)), 8: (('EU_Ireland_Dublin', 2, 2, 5), ('EU_Ireland_Dublin', 2, 2, 5)), 9: (('Asia_Singapore_Singapore', 2, 2, 1), ('Asia_Singapore_Singapore', 2, 2, 1)), 10: (('Asia_Japan_Tokio', 1, 2, 3), ('Asia_Japan_Tokio', 1, 2, 3)), 11: (('Asia_Japan_Tokio', 2, 2, 3), ('Asia_Japan_Tokio', 2, 2, 3)), 12: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 5), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 5)), 13: (('Asia_Singapore_Singapore', 1, 1, 2), ('Asia_Singapore_Singapore', 1, 1, 2)), 14: (('Asia_Japan_Tokio', 1, 2, 4), ('Asia_Japan_Tokio', 1, 2, 4)), 15: (('SouthAmerica_Brazil_SaoPaolo', 2, 2, 2), ('SouthAmerica_Brazil_SaoPaolo', 2, 2, 2)), 16: (('NorthAmerica_USWest_California', 1, 2, 4), ('NorthAmerica_USWest_California', 1, 2, 4)), 17: (('NorthAmerica_USWest_California', 1, 1, 3), ('NorthAmerica_USWest_California', 1, 1, 3)), 18: (('NorthAmerica_USWest_California', 1, 1, 2), ('NorthAmerica_USWest_California', 1, 1, 2)), 19: (('NorthAmerica_USWest_California', 2, 2, 1), ('NorthAmerica_USWest_California', 2, 2, 1)), 20: (('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5), ('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5)), 21: (('NorthAmerica_USWest_Oregon', 1, 2, 2), ('NorthAmerica_USWest_Oregon', 1, 2, 2)), 22: (('SouthAmerica_Brazil_SaoPaolo', 2, 2, 4), ('SouthAmerica_Brazil_SaoPaolo', 2, 2, 4)), 23: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 4), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 4)), 24: (('NorthAmerica_USWest_California', 1, 2, 1), ('NorthAmerica_USWest_California', 1, 2, 1))}

    results={
             'onevmforall':[],
             'startparnotexceed':[],
             'startparexceed':[],
             'allparexceed':[],
             'allparnotexceed':[]
             }
    
    dagnamelist=['sequential','cstem','montage','mapreduce']
    notasks=[10,15,24,17]
    maxetvalue=900
    
    i=1
    dagname=dagnamelist[i]
    nt=notasks[i]
    t=tasks.TasksInfo(('data/et-tasks-'+dagname+'.txt','data/size-tasks-'+dagname+'.txt'))
    dag=tasks.DAG(t.data,'data/dag-'+dagname+'.txt')
    
    results={}
    longlist=[]
    utilities=['onevmforall','startparnotexceed','startparexceed','allparexceed','allparnotexceed']
    #for all #long tasks
    nolong=1
    increment=float(nt)/4
    while nolong<nt:
        print nolong
    
        stepresults={
             'onevmforall':[],
             'startparnotexceed':[],
             'startparexceed':[],
             'allparexceed':[],
             'allparnotexceed':[]
             }
        #for all ET times of long tasks
        for v in range(1,maxetvalue):
            t.data[0] = []
            #for all tasks
            for i in range(0,nt):
                #assign ET to task as v if task is long
                if i<nolong:
                    t.data[0].append(v)
                #or 1 if task is short 
                else:
                    t.data[0].append(5)
            
            #we have now all the ETs of our tasks for this scenario
            #start scheduling and compute the utility
                    
            #the OneVMperTask is our reference algorithm
            vml=acpu.onevmpertask(dag,t.data,vmlist,taskdeplist)
            mkref=acpu.computemakespan(vml)
            cref=acpu.computerentcosts(vml)
            
            vml=acpu.onevmforall(dag, t.data,vmlist,taskdeplist)    
            stepresults['onevmforall'].append((acpu.computemakespan(vml)*cref)/(mkref*acpu.computerentcosts(vml)))   
                
            vml=acpu.startparnotexceed(dag, t.data,vmlist,taskdeplist)
            stepresults['startparnotexceed'].append((acpu.computemakespan(vml)*cref)/(mkref*acpu.computerentcosts(vml)))
            
            vml=acpu.startparexceed(dag, t.data,vmlist,taskdeplist)
            stepresults['startparexceed'].append((acpu.computemakespan(vml)*cref)/(mkref*acpu.computerentcosts(vml)))
                    
            vml=acpu.allparexceed(dag, t.data,vmlist,taskdeplist)
            stepresults['allparexceed'].append((acpu.computemakespan(vml)*cref)/(mkref*acpu.computerentcosts(vml)))        
            
            vml=acpu.allparnotexceed(dag, t.data,vmlist,taskdeplist)        
            stepresults['allparnotexceed'].append((acpu.computemakespan(vml)*cref)/(mkref*acpu.computerentcosts(vml)))
           
        results[nolong]=stepresults
        longlist.append(nolong)
        if nolong==1:
            nolong=increment    
        else:
            nolong+=increment
        if nolong==nt:
            nolong=nt-1
            
    out=''
    for key in longlist:
        for key2 in utilities:
            out=out+key2+str(key)+'\t'
    out=out+'\n'
            
    for i in range(0,maxetvalue):
        for key in longlist:
            for key2 in utilities:
                u=results[key][key2][i]
                out=out+str(u)+'\t'
        out=out+'\n'
        
    with open('utility.dat','w') as f:
        f.write(out)

#TODO:
# adjust following code to support multiple workflows and
"""
Runs a wide array of experiments on randomly generated DAGs and writes the outcome in 'results/per-alg/'
""" 
def experimentComplex():    
    
    noutilities=7
    # 7 methods x 3 instance types
    utilities=['OneVMperTask-s','OneVMforAll-s','StartParNotExceed-s','StartParExceed-s','AllParExceed-s','AllparNotExceed-s','Allpar1LnS-s',
               'OneVMperTask-m','OneVMforAll-m','StartParNotExceed-m','StartParExceed-m','AllParExceed-m','AllparNotExceed-m','Allpar1LnS-m',
               'OneVMperTask-l','OneVMforAll-l','StartParNotExceed-l','StartParExceed-l','AllParExceed-l','AllparNotExceed-l','Allpar1LnS-l',
               'Allpar1LnSdyn']
               
    #nolevels=1#random.randint(1,10)
    #NOTE: for the value used for BTU see the config.py file
    maxetvalue=5000
    full=True
    
    #when comparing the results between various algorithms we do not use the exact values but
    #allow for an epsilon variance.
    epsilon=0.10
   
    considerall=False
    
    (vmlist, locations)=utilvms.readVMtypes('data/vm-types.txt')
    
    #results <key>: nolovels#taskperlevel#percentgaenolong
    #results <value>: [bestmethodindexTEST1,methodindexTEST2,methodindexTEST3,...]
    #best time&cost
    resultsmk=defaultdict(list)
    resultsc=defaultdict(list)
    #worst time and cost
    resultswmk=defaultdict(list)
    resultswc=defaultdict(list)
    
    resultsperalgmk=defaultdict(list)    
    resultsperalgc=defaultdict(list)
    
    for nolevels in range(1,2,1):
        #vary number of tasks per level
        #for instance maxtasksperlevel=6 and a start from 5 gives tasksperlevel=7.5
        for tasksperlevel in range(0,15,5):
            noskipped=0
            for experimentdifferentgraph in range(1,5):
                #generate the graph
                (g,initialtasks,nbtotaltasks,layers)=graph.graph(nolevels, [tasksperlevel,tasksperlevel+5], full)
                #update the number of task3s in the workflow 
                nt=nbtotaltasks            
                experimentsamegraph=-1
                skip=False
                while experimentsamegraph<4 and not skip:                
                    experimentsamegraph=experimentsamegraph+1
                    
                    small=random.randint(100,200)
                    fractionsmall=random.randint(1,50)/100.0
                    #fractionlong=random.randint(1,50)/100.0
                                                                  
                    #for all #long tasks
                    #nolong=1
                    #only once for pareto
                    nolong=nbtotaltasks
                    increment=float(nt)/4
                    #step: 0, 25, 50, 75, 100 - percentage of long tasks
                    #we do not use this anymore so set nolong=nt for a single iteration
                    step=-25                
                    while nolong<=nt and not skip:                    
                        step=step+25                     
                        #print 'Skipped', noskipped, 'Experiment',experimentdifferentgraph, '/',experimentsamegraph, '. Tasks per level',tasksperlevel,'/',maxtasksperlevel,'. No long tasks',nolong,'/',nt  
                        #for all ET times of long tasks
                        #for v in range(500,maxetvalue,200):
                        #for v in range(150,maxetvalue,100):
                        stime = clock()
                        key=str(nolevels)+'#'+str((2*tasksperlevel+5)/2.0)+"#"+str(step)#+"#"+str(nt)
                        print key + ' ' + str(experimentdifferentgraph)
                        t = []
                        t.append([])                
                        #randomly pick tasks that are long
                        #picked=[]
                        #tlongindice=random.randint(0, nt)
                        #while (len(picked)<nolong):
                        #    if tlongindice not in picked:
                        #        picked.append(tlongindice)
                        #    tlongindice=random.randint(0, nt-1)
                        
                        #for all tasks
                        for i in range(0,nt):
                            #assign ET to task as v if task is long
                            #if i in picked:
                                #if v==1:
                        #    t[0].append(random.randint(int(small-small*fractionsmall),int(small+small*fractionsmall)))
                            t[0].append(random.gauss(4500,200))
                                #else:
                                    #t[0].append(random.randint(int(v-v*fractionlong),int(v+v*fractionlong)))
                            #or 5 1if task is short 
                            #else:
                            #    t[0].append(random.randint(1,small))
                        
                        #t.append(generation.generateParetoList(nt,2,500))
                        t.append(generation.generateParetoList(nt,1.3,800))
                        
                        t=tasks.TasksInfo(t)
                        dag=tasks.DAG(t.data,g,initialtasks)
                        taskdeplist=generation.generatedatadependecylocations(nt, locations, 2, 2, 5)
                        orderedtasks=dag.computepriorityallandsortdesc(full,layers)                       
                        #print "DAG",dag.dag
                        #print "DAG initial tasks",dag.startnodes
                        #print "Task data dependencies",taskdeplist
                        #print "Task ET", t.data[0]                
                            
                        #we have now all the ETs of our tasks for this scenario
                        #start scheduling and compute the utility
                                                                                           
                        #the OneVMperTask is our reference algorithm
                        bestmk=0
                        bestc=0
                        worsemk=0
                        worsec=0
                        mkref=0
                        cref=0
                        bestmkindex=0
                        bestcindex=0
                        worsemkindex=0
                        worsecindex=0
                        
                        alglistmk=[]
                        alglistc=[]
                        
                        for type in range(0,len(config.vmtypeprops)-1):
                            config.typeindex=type
                            if type==0:
                                vml=acpu.onevmpertask(dag,t.data,vmlist,taskdeplist,orderedtasks)
                                mkref=acpu.computemakespan(vml)
                                cref=acpu.computerentcosts(vml)
                                alglistmk.append((mkref,0))
                                alglistc.append((mkref,0))
                            
                                bestmk=mkref
                                bestc=cref
                                worsemk=mkref
                                worsec=cref
                                    
                                bestmkindex=0
                                bestcindex=0
                                worsemkindex=0
                                worsecindex=0
                                #print utilities[type*noutilities], bestmk, bestc
                                #acpu.printvmlist(vml,False,'*','*')
                            else:                        
                                vml=acpu.onevmpertask(dag,t.data,vmlist,taskdeplist,orderedtasks)
                                crtmk=acpu.computemakespan(vml)
                                crtc=acpu.computerentcosts(vml)
                                #print utilities[type*noutilities], crtmk, crtc
                                #acpu.printvmlist(vml,False,'*','*')

                                if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                                    if crtmk<=bestmk+bestmk*epsilon  :
                                        alglistmk.append((crtmk,type*noutilities))
                                        bestmk=crtmk
                                        bestmkindex=type*noutilities
                                    if crtc<=bestc+bestc*epsilon :
                                        alglistc.append((crtc,type*noutilities))
                                        bestc=crtc
                                        bestcindex=type*noutilities
                                if crtmk>worsemk:
                                    worsemk=crtmk
                                    worsemkindex=type*noutilities
                                if crtc>worsec:
                                    worsec=crtc
                                    worsecindex=type*noutilities
                                
                                    
                            vml=acpu.onevmforall(dag, t.data,vmlist,taskdeplist,orderedtasks)    
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)
                            #print utilities[type*noutilities+1], crtmk, crtc
                            #acpu.printvmlist(vml,False,'*','*')
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                                if crtmk<=bestmk+bestmk*epsilon  :
                                    alglistmk.append((crtmk,type*noutilities+1))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+1
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtc,type*noutilities+1))
                                    bestc=crtc
                                    bestcindex=type*noutilities+1
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+1
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+1
                                
                            vml=acpu.startparnotexceed(dag, t.data,vmlist,taskdeplist,orderedtasks)
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)
                            #print utilities[type*noutilities+2], crtmk, crtc
                            #acpu.printvmlist(vml,False,'*','*')                                                        
                               
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                                if crtmk<=bestmk+bestmk*epsilon  :
                                    alglistmk.append((crtmk,type*noutilities+2))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+2
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtmk,type*noutilities+2))
                                    bestc=crtc
                                    bestcindex=type*noutilities+2
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+2
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+2
                                
                            vml=acpu.startparexceed(dag, t.data,vmlist,taskdeplist,orderedtasks)
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)                            
                            #print utilities[type*noutilities+3], crtmk, crtc
                            #acpu.printvmlist(vml,False,'*','*')
                            
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                                if crtmk<=bestmk+bestmk*epsilon :
                                    alglistmk.append((crtmk,type*noutilities+3))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+3
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtc,type*noutilities+3))
                                    bestc=crtc
                                    bestcindex=type*noutilities+3
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+3
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+3
                                
                            vml=acpu.allparexceed(dag, t.data,vmlist,taskdeplist,layers)
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)
                            #print utilities[type*noutilities+4], crtmk, crtc
                            #acpu.printvmlist(vml,False,'*','*')                            
                            
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or  considerall:
                                if crtmk<=bestmk+bestmk*epsilon  :
                                    alglistmk.append((crtmk,type*noutilities+4))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+4
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtc,type*noutilities+4))
                                    bestc=crtc
                                    bestcindex=type*noutilities+4
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+4
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+4
                                
                            vml=acpu.allparnotexceed(dag, t.data,vmlist,taskdeplist,layers)
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)
                            #print utilities[type*noutilities+5], crtmk, crtc
                            #acpu.printvmlist(vml,False,'*','*')                            
                            
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or  considerall:
                                if crtmk<=bestmk+bestmk*epsilon :
                                    alglistmk.append((crtmk,type*noutilities+5))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+5
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtc,type*noutilities+5))
                                    bestc=crtc
                                    bestcindex=type*noutilities+5
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+5
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+5
                            
                            
                            vml=acpu.allpar1lns(dag, t.data,vmlist,taskdeplist,layers)
                            crtmk=acpu.computemakespan(vml)
                            crtc=acpu.computerentcosts(vml)  
                            #print utilities[type*noutilities+6], crtmk, crtc                        
                                                         
                            #print crtmk, mkref, crtc, cref, 'allparnotexceed'
                            if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                                if crtmk<=bestmk+bestmk*epsilon  :
                                    alglistmk.append((crtmk,type*noutilities+6))
                                    bestmk=crtmk
                                    bestmkindex=type*noutilities+6
                                if crtc<=bestc+bestc*epsilon :
                                    alglistc.append((crtc,type*noutilities+6))
                                    bestc=crtc
                                    bestcindex=type*noutilities+6
                            if crtmk>worsemk:
                                worsemk=crtmk
                                worsemkindex=type*noutilities+6
                            if crtc>worsec:
                                worsec=crtc
                                worsecindex=type*noutilities+6
                            
                        config.typeindex=0    
                        vml=acpu.allpar1lnsdyn(dag, t.data,vmlist,taskdeplist,layers)        
                        crtmk=acpu.computemakespan(vml)
                        crtc=acpu.computerentcosts(vml)
                        #print utilities[len(utilities)-1], crtmk, crtc
                        #acpu.printvmlist(vml,False,'*','*')                                                   
                        if (crtmk<=mkref+mkref*epsilon and crtc<=cref+cref*epsilon) or considerall:
                            if crtmk<=bestmk+bestmk*epsilon  :
                                alglistmk.append((crtmk,len(utilities)-1))
                                bestmk=crtmk
                                bestmkindex=len(utilities)-1
                            if crtc<=bestc+bestc*epsilon :
                                alglistc.append((crtc,len(utilities)-1))
                                bestc=crtc
                                bestcindex=len(utilities)-1
                        if crtmk>worsemk:
                            worsemk=crtmk
                            worsemkindex=len(utilities)-1
                        if crtc>worsec:
                            worsec=crtc
                            worsecindex=len(utilities)-1
                    
                        best=getbestalg(alglistmk,alglistc,utilities)

                        #for k in best[0]
                        #    resultsperalgmk[key].append(k)
                        #for k in best[1]
                        #    resultsperalgc[key].append(k)
                        resultsperalgmk[key].append(best[0])
                        resultsperalgc[key].append(best[1])
                                                            
                        etime=clock()
                        if etime-stime<600: 
                            resultsmk[key].append(bestmkindex)                    
                            resultsc[key].append(bestcindex)
                            resultswmk[key].append(worsemkindex)                    
                            resultswc[key].append(worsecindex)
                        else:
                            skip=True
                            break
                                               
                        if nolong==1:
                            nolong=increment
                        else:
                            nolong+=increment
                        #if nolong==nt:
                        #    nolong=nt-1
                    if skip:
                        noskipped+=1
                        
    print "Write experiment results"    
    out=[]
    percentagesmk=defaultdict(list)
    for k in resultsperalgmk:
        #count=Count(resultsperalgmk[k])
        l=resultsperalgmk[k]
        print l
        count=[0 for i in range(0,len(utilities))]
        for ll in l:
            for i in ll:            
                count[i]=count[i]+1
        size=len(resultsperalgmk[k])
        fields=k.split('#')
        line=''
        for fi in fields:
            line=line+fi+'\t'
        #for k2 in count
        for k2 in range(0,len(count)):
            if (count[k2]!=0):                        
                percent=100*float(count[k2])/size
                print int(math.ceil(percent)), count[k2], k2
                percentagesmk[k].append([utilities[k2],int(math.ceil(percent))])            
                #x=float(fields[0])+1*math.cos(math.radians(k2*16.36))
                #y=float(fields[0])+1*math.sin(math.radians(k2*16.36))
                x=float(fields[0])
                y=float(fields[1])-k2/22.0*10
                out.append(utilities[k2])
                with open('results/per-alg/'+fields[1]+'_makespan-'+utilities[k2]+'.dat','a') as f:
                    f.write(line+str(int(math.ceil(percent)))+'\t'+str(x)+'\t'+str(y)+'\n')

    for i in utilities:
        if i not in out:
            with open('results/per-alg/'+fields[1]+'_makespan-'+i+'.dat','w') as f:
                f.write('')

    out=[]
    percentagesc=defaultdict(list)
    for k in resultsperalgc:
        #count=Count(resultsperalgc[k])
        l=resultsperalgc[k]
        count=[0 for i in range(0,len(utilities))]
        for ll in l:
            for i in ll:            
                count[i]=count[i]+1
        size=len(resultsperalgc[k])
        fields=k.split('#')
        line=''
        for fi in fields:
            line=line+fi+'\t'
        #for k2 in count
        for k2 in range(0,len(count)):
            if (count[k2]!=0):                                                
                percent=100*float(count[k2])/size                
                percentagesc[k].append([utilities[k2],int(math.ceil(percent))])
                #x=float(fields[0])+1*math.cos(math.radians(k2*16.36))
                #y=float(fields[1])+1*math.sin(math.radians(k2*16.36))
                x=float(fields[0])
                y=float(fields[1])-k2/22.0*10
                out.append(utilities[k2])
                with open('results/per-alg/'+fields[1]+'_cost-'+utilities[k2]+'.dat','a') as f:
                    f.write(line+str(int(math.ceil(percent)))+'\t'+str(x)+'\t'+str(y)+'\n')                    

    for i in utilities:
        if i not in out:
            with open('results/per-alg/'+fields[1]+'_cost-'+i+'.dat','w') as f:
                f.write('')

    print "Ended experiment"
    print "Time", localtime()
        
    # !!! IMPORTANT: the following lines will be removed in the near future
                        
    #for k in resultsmk:
    #    count=Counter(resultsmk[k])
    #    maxv=0
    #    max=0
    #    for k2 in count:
    #        
    #        if count[k2] > maxv:
    #            maxv=count[k2]
    #            max=k2
    #    total=len(resultsmk[k])
    #    resultsmk[k]=[maxv*100/total,max]
    #for k in resultswmk:
    #    count=Counter(resultswmk[k])
    #    maxv=0
    #    max=0
    #    for k2 in count:
    #        
    #        if count[k2] > maxv:
    #            maxv=count[k2]
    #            max=k2
    #    total=len(resultswmk[k])
    #    resultswmk[k]=[maxv*100/total,max]
    #        
    #for k in resultsc:
    #    count=Counter(resultsc[k])
    #    maxv=0
    #    max=''
    #    for k2 in count:
    #        if count[k2] > maxv:
    #            maxv=count[k2]
    #            max=k2
    #    total=len(resultsc[k])
    #    resultsc[k]=[maxv*100/total,max]
    
    #for k in resultswc:
    #    count=Counter(resultswc[k])
    #    maxv=0
    #    max=''
    #    for k2 in count:
    #        if count[k2] > maxv:
    #            maxv=count[k2]
    #            max=k2
    #    total=len(resultswc[k])
    #    resultswc[k]=[maxv*100/total,max]
                
    #print "Write experiment results"
    
    # !!! Only if the results are to be analyzed with MEKA/WEKA
    #for normal
    #writeMekaFile(percentagesmk,percentagesc,150,50,utilities)    
    #writeWekaFile(resultsmk,resultsc,(small+1)/2.0,math.sqrt(math.pow(small-1,2)/12.0),utilities)
    #for pareto
    #writeWekaFile(resultsmk,resultsc,150,50,utilities)

    # !!! The files written next are not used anymmore. Please use the ones generated before and put in per-alg/    
    #makespan
    #out=defaultdict(str)
    #for k in resultsmk:
    #    fields=k.split('#')
    #    s=''        
    #    for f in fields:
    #        s=s+f+'\t'
    #    s=s+str(resultsmk[k][0])+"\n"
    #    if utilities[resultsmk[k][1]] not in out:
    #        out[utilities[resultsmk[k][1]]]=s
    #    out[utilities[resultsmk[k][1]]]=out[utilities[resultsmk[k][1]]]+s
    
    #for k in out:         
    #    with open('results/results-makespan-'+k+'.dat','w') as f:
    #        f.write(out[k])

    #for i in utilities:
    #    if i not in out:
    #        with open('results/results-makespan-'+i+'.dat','w') as f:
    #            f.write('')

    #makespan worse case
    #out=defaultdict(str)
    #for k in resultswmk:
    #    fields=k.split('#')
    #    s=''        
    #    for f in fields:
    #        s=s+f+'\t'        
    #    s=s+str(resultswmk[k][0])+"\n"
    #    if utilities[resultswmk[k][1]] not in out:
    #        out[utilities[resultswmk[k][1]]]=s
    #    out[utilities[resultswmk[k][1]]]=out[utilities[resultswmk[k][1]]]+s
    
    #for k in out:         
    #    with open('results/results-makespan-worse-'+k+'.dat','w') as f:
    #        f.write(out[k])

    #for i in utilities:
    #    if i not in out:
    #        with open('results/results-makespan-worse-'+i+'.dat','w') as f:
    #            f.write('')
                    
    #cost
    #out=defaultdict(str)
    #for k in resultsc:
    #    fields=k.split('#')
    #    s=''        
    #    for f in fields:
    #        s=s+f+'\t'
    #    s=s+str(resultsc[k][0])+"\n"
    #    if utilities[resultsc[k][1]] not in out:
    #        out[utilities[resultsc[k][1]]]=s        
    #    out[utilities[resultsc[k][1]]]=out[utilities[resultsc[k][1]]]+s
    #for k in out:
    #    with open('results/results-cost-'+k+'.dat','w') as f:
    #        f.write(out[k])
    
    #generate some empty files. The script requires all filenames to be present
    #for i in utilities:
    #    if i not in out:
    #        with open('results/results-cost-'+i+'.dat','w') as f:
    #            f.write('')

    #cost worse
    #out=defaultdict(str)
    #for k in resultswc:
    #    fields=k.split('#')
    #    s=''        
    #    for f in fields:
    #        s=s+f+'\t'
    #    s=s+str(resultswc[k][0])+"\n"
    #    if utilities[resultswc[k][1]] not in out:
    #        out[utilities[resultswc[k][1]]]=s        
    #    out[utilities[resultswc[k][1]]]=out[utilities[resultswc[k][1]]]+s
    #for k in out:
    #    with open('results/results-cost-worse-'+k+'.dat','w') as f:
    #        f.write(out[k])
    
    #generate some empty files. The script requires all filenames to be present
    #for i in utilities:
    #    if i not in out:
    #        with open('results/results-cost-worse-'+i+'.dat','w') as f:
    #            f.write('')
    
    
import operator

def getbestalg(mk,c,utilities):
    epsilon=0.10
    s_mk = sorted(mk, key=operator.itemgetter(0))
    s_c = sorted(c, key=operator.itemgetter(0))
    
    res_mk=[]
    res_c=[]
    
    max=int(s_mk[0][0])
    res_mk.append(s_mk[0][1])
    for item in range(1,len(s_mk)):
        if int(s_mk[item][0])>max+max*epsilon:
            break
        if int(s_mk[item][0])<=max+max*epsilon:
            res_mk.append(s_mk[item][1])
            
    max=s_c[0][0]
    res_c.append(s_c[0][1])
    for item in range(1,len(s_c)):
        if s_c[item][0]>max+max*epsilon:
            break
        if s_c[item][0]<=max+max*epsilon:
            res_c.append(s_c[item][1])
    
    #print s_mk
    #print map(lambda x: utilities[x], res_mk)
    #print s_c
    #print res_c
    
    return [res_mk,res_c]
    
"""
Generates a Meka compatible file. It can be used to analyze and make predictions 
with the MEKA tool
"""
def writeMekaFile(resultsmk,resultsc,meanshort,stdshort,utilities):
    header='@RELATION \'algorithm: -C 22 -split-percentage 50\'\n'
    header+='@ATTRIBUTE OneVMperTask-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE OneVMforAll-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParNotExceed-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParExceed-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllParExceed-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllparNotExceed-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE Allpar1LnS-s {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE OneVMperTask-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE OneVMforAll-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParNotExceed-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParExceed-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllParExceed-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllparNotExceed-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE Allpar1LnS-m {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE OneVMperTask-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE OneVMforAll-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParNotExceed-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE StartParExceed-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllParExceed-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE AllparNotExceed-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE Allpar1LnS-l {0,1,2,3,4,5,6,7,8,9,10}\n'
    header+='@ATTRIBUTE Allpar1LnSdyn {0,1,2,3,4,5,6,7,8,9,10}\n'

    header+='@ATTRIBUTE nolevels  NUMERIC\n'
    header+='@ATTRIBUTE avgtasksperlevel  NUMERIC\n'
    header+='@ATTRIBUTE numberoftasks  NUMERIC\n'
    header+='@ATTRIBUTE meanshort  NUMERIC\n'
    header+='@ATTRIBUTE stddevshort  NUMERIC\n'    

    header+='@DATA\n'
    
    sep=','
    s=header
    for k in resultsmk:
        r=resultsmk[k]
        sa=''
        for a in utilities:
            found=False
            for item in r:            
                if a in item[0]:
                    #quick hack until I find the error
                    if item[1]>10:
                        item[1]=10
                    sa=sa+str(int(math.floor(item[1]/10.)))+sep
                    found=True
            if not found:
                sa=sa+'0'+sep
        s=s+sa
        fields=k.split('#')
        s=s+fields[0]+sep+fields[1]+sep+fields[3]+sep
        s=s+str(meanshort)+sep+str(stdshort)+'\n'  
    
    with open('results/meka-mk.arff','w') as f:
        f.write(s)
        
    sep=','
    s=header
    for k in resultsc:
        r=resultsc[k]
        sa=''
        for a in utilities:
            found=False
            for item in r:            
                if a in item[0]:
                    sa=sa+str(int(math.floor(item[1]/10.)))+sep
                    found=True
            if not found:
                sa=sa+'0'+sep
        s=s+sa
        fields=k.split('#')
        s=s+fields[0]+sep+fields[1]+sep+fields[3]+sep
        s=s+str(meanshort)+sep+str(stdshort)+'\n'  
    
    with open('results/meka-c.arff','w') as f:
        f.write(s)

"""
Generates a Weka compatible file. It can be used to analyze and make predictions 
with the WEKA tool
"""    
def writeWekaFile(resultsmk,resultsc,meanshort,stdshort,utilities):
    header='@RELATION algorithm\n'
    header+='@ATTRIBUTE nolevels  NUMERIC\n'
    header+='@ATTRIBUTE avgtasksperlevel  NUMERIC\n'
    header+='@ATTRIBUTE numberoftasks  NUMERIC\n'
    header+='@ATTRIBUTE meanshort  NUMERIC\n'
    header+='@ATTRIBUTE stddevshort  NUMERIC\n'    
    header+='@ATTRIBUTE a1 {OneVMperTask-s,OneVMforAll-s,StartParNotExceed-s,StartParExceed-s,AllParExceed-s,AllparNotExceed-s,Allpar1LnS-s,OneVMperTask-m,OneVMforAll-m,StartParNotExceed-m,StartParExceed-m,AllParExceed-m,AllparNotExceed-m,Allpar1LnS-m,OneVMperTask-l,OneVMforAll-l,StartParNotExceed-l,StartParExceed-l,AllParExceed-l,AllparNotExceed-l,Allpar1LnS-l,Allpar1LnSdyn}\n'
    header+='@DATA\n'
      
    sep=','
    s=header
    for k in resultsmk:
        #s=s+str(nolevels)+sep
        fields=k.split('#')                
        s=s+fields[0]+sep+fields[1]+sep+fields[3]+sep
            
#        meanlong=float(fields[len(fields)-1])
#        if fractionlong==0:
#          stdlong=0
#        else:
#          stdlong=math.sqrt(meanlong*fractionlong/6.0)
        s=s+str(meanshort)+sep+str(stdshort)+sep+utilities[resultsmk[k][1]]+'\n'
    with open('results/weka-m.arff','w') as f:
        f.write(s)
    
    s=header
    for k in resultsc:
        #s=s+str(nolevels)+sep
        fields=k.split('#')
        s=s+fields[0]+sep+fields[1]+sep+fields[3]+sep
                        
#        meanlong=float(fields[len(fields)-1])
#       if fractionlong==0:
#          stdlong=0
#       else:
#          stdlong=math.sqrt(meanlong*fractionlong/6.0)
         
        s=s+str(meanshort)+sep+str(stdshort)+sep+utilities[resultsc[k][1]]+'\n'       
    with open('results/weka-c.arff','w') as f:
        f.write(s)

def experimentSingleAlgorithm():
    # !!! see config.py for some settings
    
    #randomly generated data for taskdeplist. depends on the workflow
    #montagne {1: (('NorthAmerica_USWest_Oregon', 2, 1, 1), ('NorthAmerica_USWest_Oregon', 2, 1, 1)), 2: (('Asia_Japan_Tokio', 2, 2, 5), ('Asia_Japan_Tokio', 2, 2, 5)), 3: (('EU_Ireland_Dublin', 1, 2, 4), ('EU_Ireland_Dublin', 1, 2, 4)), 4: (('EU_Ireland_Dublin', 2, 2, 1), ('EU_Ireland_Dublin', 2, 2, 1)), 5: (('NorthAmerica_USWest_California', 1, 1, 5), ('NorthAmerica_USWest_California', 1, 1, 5)), 6: (('NorthAmerica_USWest_California', 1, 2, 1), ('NorthAmerica_USWest_California', 1, 2, 1)), 7: (('NorthAmerica_USWest_Oregon', 2, 2, 3), ('NorthAmerica_USWest_Oregon', 2, 2, 3)), 8: (('EU_Ireland_Dublin', 2, 2, 5), ('EU_Ireland_Dublin', 2, 2, 5)), 9: (('Asia_Singapore_Singapore', 2, 2, 1), ('Asia_Singapore_Singapore', 2, 2, 1)), 10: (('Asia_Japan_Tokio', 1, 2, 3), ('Asia_Japan_Tokio', 1, 2, 3)), 11: (('Asia_Japan_Tokio', 2, 2, 3), ('Asia_Japan_Tokio', 2, 2, 3)), 12: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 5), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 5)), 13: (('Asia_Singapore_Singapore', 1, 1, 2), ('Asia_Singapore_Singapore', 1, 1, 2)), 14: (('Asia_Japan_Tokio', 1, 2, 4), ('Asia_Japan_Tokio', 1, 2, 4)), 15: (('SouthAmerica_Brazil_SaoPaolo', 2, 2, 2), ('SouthAmerica_Brazil_SaoPaolo', 2, 2, 2)), 16: (('NorthAmerica_USWest_California', 1, 2, 4), ('NorthAmerica_USWest_California', 1, 2, 4)), 17: (('NorthAmerica_USWest_California', 1, 1, 3), ('NorthAmerica_USWest_California', 1, 1, 3)), 18: (('NorthAmerica_USWest_California', 1, 1, 2), ('NorthAmerica_USWest_California', 1, 1, 2)), 19: (('NorthAmerica_USWest_California', 2, 2, 1), ('NorthAmerica_USWest_California', 2, 2, 1)), 20: (('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5), ('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5)), 21: (('NorthAmerica_USWest_Oregon', 1, 2, 2), ('NorthAmerica_USWest_Oregon', 1, 2, 2)), 22: (('SouthAmerica_Brazil_SaoPaolo', 2, 2, 4), ('SouthAmerica_Brazil_SaoPaolo', 2, 2, 4)), 23: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 4), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 4)), 24: (('NorthAmerica_USWest_California', 1, 2, 1), ('NorthAmerica_USWest_California', 1, 2, 1))}
    #cstem {1: (('EU_Ireland_Dublin', 2, 2, 5), ('EU_Ireland_Dublin', 2, 2, 5)), 2: (('Asia_Japan_Tokio', 2, 2, 5), ('Asia_Japan_Tokio', 2, 2, 5)), 3: (('EU_Ireland_Dublin', 2, 1, 5), ('EU_Ireland_Dublin', 2, 1, 5)), 4: (('EU_Ireland_Dublin', 1, 2, 5), ('EU_Ireland_Dublin', 1, 2, 5)), 5: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 1), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 1)), 6: (('Asia_Singapore_Singapore', 1, 1, 2), ('Asia_Singapore_Singapore', 1, 1, 2)), 7: (('EU_Ireland_Dublin', 1, 2, 5), ('EU_Ireland_Dublin', 1, 2, 5)), 8: (('Asia_Japan_Tokio', 2, 1, 5), ('Asia_Japan_Tokio', 2, 1, 5)), 9: (('EU_Ireland_Dublin', 2, 2, 3), ('EU_Ireland_Dublin', 2, 2, 3)), 10: (('Asia_Japan_Tokio', 1, 1, 5), ('Asia_Japan_Tokio', 1, 1, 5)), 11: (('Asia_Singapore_Singapore', 2, 2, 4), ('Asia_Singapore_Singapore', 2, 2, 4)), 12: (('NorthAmerica_USWest_Oregon', 2, 1, 1), ('NorthAmerica_USWest_Oregon', 2, 1, 1)), 13: (('NorthAmerica_USWest_Oregon', 2, 2, 3), ('NorthAmerica_USWest_Oregon', 2, 2, 3)), 14: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 2), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 2)), 15: (('NorthAmerica_USWest_California', 1, 2, 3), ('NorthAmerica_USWest_California', 1, 2, 3))}
    #mapreduce {1: (('NorthAmerica_USWest_Oregon', 1, 1, 4), ('NorthAmerica_USWest_Oregon', 1, 1, 4)), 2: (('Asia_Singapore_Singapore', 2, 1, 5), ('Asia_Singapore_Singapore', 2, 1, 5)), 3: (('Asia_Japan_Tokio', 2, 2, 5), ('Asia_Japan_Tokio', 2, 2, 5)), 4: (('SouthAmerica_Brazil_SaoPaolo', 2, 1, 5), ('SouthAmerica_Brazil_SaoPaolo', 2, 1, 5)), 5: (('Asia_Singapore_Singapore', 2, 1, 3), ('Asia_Singapore_Singapore', 2, 1, 3)), 6: (('EU_Ireland_Dublin', 2, 1, 4), ('EU_Ireland_Dublin', 2, 1, 4)), 7: (('NorthAmerica_USWest_California', 1, 2, 3), ('NorthAmerica_USWest_California', 1, 2, 3)), 8: (('NorthAmerica_USWest_Oregon', 1, 1, 5), ('NorthAmerica_USWest_Oregon', 1, 1, 5)), 9: (('NorthAmerica_USWest_California', 2, 1, 3), ('NorthAmerica_USWest_California', 2, 1, 3)), 10: (('NorthAmerica_USWest_California', 1, 2, 4), ('NorthAmerica_USWest_California', 1, 2, 4)), 11: (('NorthAmerica_USWest_Oregon', 1, 2, 4), ('NorthAmerica_USWest_Oregon', 1, 2, 4)), 12: (('EU_Ireland_Dublin', 2, 1, 2), ('EU_Ireland_Dublin', 2, 1, 2)), 13: (('SouthAmerica_Brazil_SaoPaolo', 1, 1, 2), ('SouthAmerica_Brazil_SaoPaolo', 1, 1, 2)), 14: (('EU_Ireland_Dublin', 1, 2, 3), ('EU_Ireland_Dublin', 1, 2, 3)), 15: (('EU_Ireland_Dublin', 1, 2, 2), ('EU_Ireland_Dublin', 1, 2, 2)), 16: (('EU_Ireland_Dublin', 2, 1, 1), ('EU_Ireland_Dublin', 2, 1, 1)), 17: (('EU_Ireland_Dublin', 1, 2, 4), ('EU_Ireland_Dublin', 1, 2, 4)), 18: (('NorthAmerica_USWest_Oregon', 1, 2, 5), ('NorthAmerica_USWest_Oregon', 1, 2, 5))}
    #sequential {1: (('NorthAmerica_USWest_California', 2, 2, 1), ('NorthAmerica_USWest_California', 2, 2, 1)), 2: (('NorthAmerica_USWest_Oregon', 2, 1, 2), ('NorthAmerica_USWest_Oregon', 2, 1, 2)), 3: (('Asia_Japan_Tokio', 2, 2, 4), ('Asia_Japan_Tokio', 2, 2, 4)), 4: (('SouthAmerica_Brazil_SaoPaolo', 2, 1, 3), ('SouthAmerica_Brazil_SaoPaolo', 2, 1, 3)), 5: (('Asia_Japan_Tokio', 1, 1, 1), ('Asia_Japan_Tokio', 1, 1, 1)), 6: (('Asia_Japan_Tokio', 1, 1, 3), ('Asia_Japan_Tokio', 1, 1, 3)), 7: (('EU_Ireland_Dublin', 2, 2, 2), ('EU_Ireland_Dublin', 2, 2, 2)), 8: (('EU_Ireland_Dublin', 1, 2, 4), ('EU_Ireland_Dublin', 1, 2, 4)), 9: (('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5), ('SouthAmerica_Brazil_SaoPaolo', 1, 1, 5)), 10: (('NorthAmerica_USWest_Oregon', 2, 2, 4), ('NorthAmerica_USWest_Oregon', 2, 2, 4)),11: (('Asia_Japan_Tokio', 2, 2, 3), ('Asia_Japan_Tokio', 2, 2, 3))} 

    (vmlist, locations)=utilvms.readVMtypes('data/vm-types.txt')

    t=tasks.TasksInfo(('data/et-tasks-cstem.txt','data/size-tasks-cstem.txt'))
    dag=tasks.DAG(t.data,'data/dag-cstem.txt')
    taskdeplist={1: (('EU_Ireland_Dublin', 2, 2, 5), ('EU_Ireland_Dublin', 2, 2, 5)), 2: (('Asia_Japan_Tokio', 2, 2, 5), ('Asia_Japan_Tokio', 2, 2, 5)), 3: (('EU_Ireland_Dublin', 2, 1, 5), ('EU_Ireland_Dublin', 2, 1, 5)), 4: (('EU_Ireland_Dublin', 1, 2, 5), ('EU_Ireland_Dublin', 1, 2, 5)), 5: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 1), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 1)), 6: (('Asia_Singapore_Singapore', 1, 1, 2), ('Asia_Singapore_Singapore', 1, 1, 2)), 7: (('EU_Ireland_Dublin', 1, 2, 5), ('EU_Ireland_Dublin', 1, 2, 5)), 8: (('Asia_Japan_Tokio', 2, 1, 5), ('Asia_Japan_Tokio', 2, 1, 5)), 9: (('EU_Ireland_Dublin', 2, 2, 3), ('EU_Ireland_Dublin', 2, 2, 3)), 10: (('Asia_Japan_Tokio', 1, 1, 5), ('Asia_Japan_Tokio', 1, 1, 5)), 11: (('Asia_Singapore_Singapore', 2, 2, 4), ('Asia_Singapore_Singapore', 2, 2, 4)), 12: (('NorthAmerica_USWest_Oregon', 2, 1, 1), ('NorthAmerica_USWest_Oregon', 2, 1, 1)), 13: (('NorthAmerica_USWest_Oregon', 2, 2, 3), ('NorthAmerica_USWest_Oregon', 2, 2, 3)), 14: (('SouthAmerica_Brazil_SaoPaolo', 1, 2, 2), ('SouthAmerica_Brazil_SaoPaolo', 1, 2, 2)), 15: (('NorthAmerica_USWest_California', 1, 2, 3), ('NorthAmerica_USWest_California', 1, 2, 3))}
    
  
    #vml=acpu.onevmforall(dag, t.data,vmlist,taskdeplist)    
    #vml=acpu.startparnotexceed(dag, t.data,vmlist,taskdeplist)
    #vml=acpu.startparexceed(dag, t.data,vmlist,taskdeplist)
    vml=acpu.allparexceed(dag, t.data,vmlist,taskdeplist)
    #vml=acpu.allparnotexceed(dag, t.data,vmlist,taskdeplist)
    #vml=acpu.cpaeager(dag,t.data,vmlist,taskdeplist)
    #vml=acpu.gain(dag,t.data,vmlist,taskdeplist)
    #vml=acpu.allpar1lns(dag,t.data,vmlist,taskdeplist)
    #vml=acpu.allpar1lnsdyn(dag,t.data,vmlist,taskdeplist)

    # !!! set second argument to try if you want to export it in a JSON format similar to Schlouder's.
    #you can then use the json-to-fig script to generate a nice gant diagram. 
    acpu.printvmlist(vml,False,'HEFT','AllParExceed')

    print 'Rent Cost',acpu.computerentcosts(vml)
    print 'Transfer Cost',acpu.computetransfercosts(vml, t.data, taskdeplist, vmlist)
    print 'Total Cost', acpu.computerentcosts(vml)+acpu.computetransfercosts(vml, t.data, taskdeplist, vmlist)
    print 'Idle time',acpu.computeidletime(vml)
    
# !!! Uncommment one of the following lines this depending on ant you want to test
#experimentUtility()
#experimentComplex()
experimentSingleAlgorithm()

