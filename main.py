import time
import yaml
import threading
import datetime

ms = '1B'           #Milestone number

file1 = open("Milestone"+ms+".txt","w")

def timeFunction(inputs, name):
    funcInput = inputs["FunctionInput"]
    t = inputs["ExecutionTime"]
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing TimeFunction ("+ funcInput+ ', '+ t+ ")\n")
    time.sleep(int(t))
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def flow(workflow,name,isSequential):
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if isSequential:                            #SEQUENTIAL
        for k in workflow.keys():
            sname = name+'.'+k
            if workflow[k]["Type"] == "Flow":
                if workflow[k]["Execution"] == "Sequential":
                    flow(workflow[k]["Activities"],sname,True)
                else:
                    flow(workflow[k]["Activities"],sname,False)
            elif workflow[k]["Type"] == "Task":
                if workflow[k]["Function"] == "TimeFunction":
                    timeFunction(workflow[k]["Inputs"],sname)
    else:                                       #CONCURRENT
        threadList = []
        for k in workflow.keys():
            sname = name+'.'+k
            if workflow[k]["Type"] == "Flow":
                if workflow[k]["Execution"] == "Sequential":
                    threadList.append(threading.Thread(target=flow,args=(workflow[k]["Activities"],sname,True)))
                else:
                    threadList.append(threading.Thread(target=flow,args=(workflow[k]["Activities"],sname,False)))
            elif workflow[k]["Type"] == "Task":
                if workflow[k]["Function"] == "TimeFunction":
                    threadList.append(threading.Thread(target=timeFunction,args=(workflow[k]["Inputs"],sname)))
        for th in threadList:
            th.start()
        for th in threadList:
            th.join()
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

if __name__ == "__main__":
    yamlFile = open(f"Milestone"+ms[0]+"/Milestone"+ms+".yaml",'r')
    workflow = yaml.safe_load(yamlFile)
    name = "M"+ms+"_Workflow"
    if workflow[name]["Execution"] == "Sequential":
        flow(workflow[name]["Activities"],name,True)
    else:
        flow(workflow[name]["Activities"],name,False)

file1.close()

"""
def flow(workflow,name):
    file1.write(str(datetime.datetime.now())+ '; '+ name+ " Entry\n")
    threadList = []
    for k in workflow.keys():
        sname = name+'.'+k
        if workflow[k]["Type"] == "Flow":
            if workflow[k]["Execution"] == "Sequential":
                threadList.append(threading.Thread(target=sequential,args=(workflow[k]["Activities"],sname))
            elif workflow[k]["Execution"] == "Concurrent":
                concurrent(workflow[k]["Activities"],sname)
        elif workflow[k]["Type"] == "Task":
            if workflow[k]["Function"] == "TimeFunction":
                timeFunction(workflow[k]["Inputs"],sname)
    file1.write(str(datetime.datetime.now())+ '; '+ name+ " Exit\n")
"""