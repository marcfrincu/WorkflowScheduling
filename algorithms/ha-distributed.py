'''
Created on May 28, 2013

@author: mfrincu
'''
import random

nbcomponenttype=5
maxload=0.9
cload=[0.01,0.02,0.01,0.05,0.03]


class VM:
    def __init__(self,n,vms):
        self.id=1
        self.components={}
        for i in range(0,nbcomponenttype):
            self.components[i+1]=random.randint(0,n)
        
        self.neighbour=[]
        if len(vms)>0:
            self.neighbour=[random.randint(0,len(vms)-1)]   

def main(vms):
    epsilon=0.5
    
    for vm in vms:
        # not HA and almost fully loaded
        # we need to move some load to neighbors
        if not isHA(vm) and maxload-load(vm)<=epsilon:
            c=getcomponent(vm)
            retry=0
            vmnew=None
            if len(vm.neighbour)>0:
                vmnew=vms[vm.neighbour[random.randint(0,len(vm.neighbour)-1)]]
                while load(vmnew)+cload[c]>maxload-epsilon or (retry<10 and ismissingcomponent(vmnew,c)):
                    vmnew=vms[vm.neighbour[random.randint(0,len(vm.neighbour)-1)]]                    
                    c=getcomponent(vm)                    
                if retry<10:
                    addcomponent(vmnew,c)
        # not HA and there is enough space to accommondate components from neighbors
        # get missing components from neighbours
            
def isHA(vm):
    for k in vm.components:
        if vm.components[k]==0:
            return False
    return True
    
def getcomponent(vm):
    c=random.randint(1,len(vm.components))
    print c,vm.components[c]
    while (vm.components[c]<=1):
        c=random.randint(1,len(vm.components))
    vm.components[c]=vm.components[c]-1
    return c

def addcomponent(vm,c):
    vm.components[c]=vm.components[c]+1
    
def ismissingcomponent(vm,c):
    if c in vm.components:
        return False
    return True

def load(vm):
    l=0
    for c in vm.components:
       l=l+vm.components[c]*cload[c-1] 
    return l

#test

vms=[]

vm1=VM(10,vms)

vms=[vm1]

vm2=VM(5,vms)

vms=[vm1,vm2]

print vm1.components
print vm2.components

print isHA(vm1)
print isHA(vm2)

print getcomponent(vm1)
print getcomponent(vm2)

print vm1.components
print vm2.components

addcomponent(vm1,1)
addcomponent(vm2,2)

print vm1.components
print vm2.components

print load(vm1)
print load(vm2)

print ismissingcomponent(vm1,2)
print ismissingcomponent(vm2,10)

main(vms)