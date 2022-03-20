'''
Created on Jan 17, 2013

@author: mfrincu
'''

import generation

"""
Computes the probability that the numbers in a given list are smaller than a reference value
params: the list of numbers, the reference value
return: the probability that the values in the list are smaller than the reference 
"""
def probCdf(t, x):    
    count = 0.0
    for value in t:
        if value <= x:
            count += 1.0
    
    prob = count / len(t)
    return prob

"""
Computes the CDF of a list of values and outputs the result in a file.
params: the list of values, the filepath and name
"""
def outCdf(l, location):
    minet=min(l)
    maxet=max(l)

    p=[]
    v=[]
    for j in range(int(minet),int(maxet),(maxet-minet)/30):
        p.append(probCdf(l,j))
        v.append(j)
    
    with open(location,'w') as f:
        for j in range(0,len(p)):
            f.write(str(v[j])+'\t'+str(p[j])+'\n')

#t=tasks.TasksInfo(('../data/et-tasks-mapreduce.txt','../data/size-tasks-mapreduce.txt'))

#print t.data[0]

#minet=min(t.data[0])
#maxet=max(t.data[0])

#for i in range(500,2100,100):
#    l=[]
#    l.append(generation.generateParetoList(100,2,i))
#    outCdf(l,'../results/cdf/cdf-'+i+'.dat')
    