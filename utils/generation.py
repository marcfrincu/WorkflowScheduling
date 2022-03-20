'''
Created on Oct 24, 2012

@author: mfrincu
'''

import random
 
"""
Randomly assigns locations to task input/output data.
This is not used by CPU intensive tasks
"""
def generatedatadependecylocations(notasks,locations,norooms,noracks,noservers):
    datalocations={}
    assert notasks >1 and norooms>1 and noracks > 1 and noservers > 1
    for i in range(1,notasks+1):
        inputdata=(locations[random.randint(1,len(locations)-1)],
                   random.randint(1,norooms), 
                   random.randint(1,noracks), 
                   random.randint(1,noservers))
        #TODO add a different location for output data
        outputdata=inputdata
        datalocations[i]=(inputdata,outputdata)
    return datalocations
 
"""
Generates a list of Pareto value given a scale and shape parameter
params: the number of numbers to generate, the shape, the scale
return: a list o numbers
"""
def generateParetoList(n,alpha,scale):
    if scale < 0:
        scale=1    
    l=[random.paretovariate(alpha)*scale for i in range(int(n))]

    return l
 
"""
Generates a list of Pareto value given shape parameter and the alpha and beta for the distribution
params: the number of numbers to generate, alpha, beta, the scale
return: a list o numbers
"""
def generateWeibullList(n,alpha,beta,scale):
    if scale < 0:
        scale=1
    l=[random.weibullvariate(alpha,beta)*scale for i in range(int(n))]
    
    return l

"""
Generates a list of execution times that deviate by stdev from the given ones.
This method could be used to generate errors in the user given ETs
params: the list of ETs and the standard deviation
return: a list of two lists containing the given ETs and the generated ones
"""
def generateerrorET(et, stdev):
    l=[abs(i+random.choice([-1,1])*random.randint(0,stdev)) for i in et]
    return [et,l]

#print generateerrorET([10,20,30],15)

#n=15 for CSTEM DAG
#n=24 for MONTAGE DAG
#n=17 for MAPREDUCE
#n=10 for SEQUENTIAL
# arrival rate
#print generateParetoList(n,1.75,1)
# execution time
#print generateParetoList(15,2,500)

#min=[]
#max=[]
#for i in range(0,24):
 #   min.append(1)
#    max.append(3*3600)
    
#print min
#print max

# size 
#print generateParetoList(10,1.3,800)

#(vms, locations)=vms.readVMtypes('data/vm-types.txt')

#print vms

#print generatedatadependecylocations(10, locations, 2, 2, 5)