import time
import yaml
import threading
import datetime
import pandas as pd

outputs = {}

ms = '2B'           #Milestone number

file1 = open("Milestone"+ms+".txt","w")

def checkCondition(condition):
    if len(condition):
        s = e = 0
        n = 0
        con = ''
        for i in range(len(condition)):
            if condition[i] == '(':
                s = i
            elif condition[i] == ')':
                e = i
            if e and condition[i]==' ':
                n = i
            if condition[i] == '>' or condition[i] == '<':
                con = condition[i]
        conName = condition[s+1:e-12]
        val = int(condition[n+1:])
        #print(condition,conName,con,val)
        while conName not in outputs.keys():
            continue
        if (con=='>' and outputs[conName][1]>val) or (con=='<' and outputs[conName][1]<val):
            return True
        else:
            return False
    else:
        return True

def binner(rule ,name, dataset, condition):
    dataset = dataset[2:-11]
    dataset = outputs[dataset][0]
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing Binning ()\n")
        
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")
def dataLoad(inputs, name, condition):
    inputFile = inputs["Filename"]
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing DataLoad ("+ inputFile+")\n")    
        df = pd.read_csv("Milestone"+ms[0]+"/"+inputFile)
        outputs[name] = [df,df.shape[0]]
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def timeFunction(inputs, name, condition):
    #print(name, inputs, condition)
    funcInput = inputs["FunctionInput"]
    if funcInput[0] == '$':
        funcInput = funcInput[2:-13]
        while funcInput not in outputs.keys():
            continue
        funcInput = str(outputs[funcInput][1])
    t = inputs["ExecutionTime"]
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing TimeFunction ("+ funcInput+ ', '+ t+ ")\n")
        time.sleep(int(t))
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
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
                condition = ""
                if "Condition" in workflow[k].keys():
                    condition += workflow[k]["Condition"]
                if workflow[k]["Function"] == "TimeFunction":                  
                    timeFunction(workflow[k]["Inputs"],sname, condition)
                elif workflow[k]["Function"] == "DataLoad":
                    dataLoad(workflow[k]["Inputs"],sname, condition)
                elif workflow[k]["Function"] == "Binning":
                    binning()
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
                condition = ""
                if "Condition" in workflow[k].keys():
                    condition += workflow[k]["Condition"]
                if workflow[k]["Function"] == "TimeFunction":
                    threadList.append(threading.Thread(target=timeFunction,args=(workflow[k]["Inputs"],sname,condition)))
                elif workflow[k]["Function"] == "DataLoad":
                    threadList.append(threading.Thread(target=dataLoad,args=(workflow[k]["Inputs"],sname,condition)))
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