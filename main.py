import time
import yaml
import threading
import datetime
import pandas as pd

outputs = {}

ms = '3A'           #Milestone number

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

def export(outputFile, defectTable, name, condition):
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing ExportResults ()\n")
        defectTable = defectTable.split('.')
        dtable = defectTable[0][2:]
        for i in range(1,len(defectTable)-1):
            dtable = dtable+'.'+defectTable[i]
        df = outputs[dtable][0]
        df.to_csv(outputFile)
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")
def merge(inputs, outs, name, condition):
    print(name)
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing MergeResults ()\n")
        file2 = open("Milestone"+ms[0]+"/"+inputs["PrecedenceFile"],"r")
        precedence = file2.read()
        file2.close()
        #print(precedence)
        precedence.split(" >> ")
        bin = [-1 for i in range(outputs[inputs["DataSet1"][2:-21]][1])]
        i = len(precedence)-1
        while i>=0:
            curr = outputs[inputs["DataSet1"][2:-24]+precedence[i]][0]["Bincode"]
            for j in len(curr):
                if curr[j]!=-1:
                    bin[j] = curr[j]
            i-=1
        output = outputs[inputs["DataSet1"][2:-21]]
        output["Bincode"] = bin
        print(name)
        outputs[name] = [output,output.shape[0]]
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def binning(inputs, name, condition):
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing Binning ()\n")
        dataset = inputs["DataSet"]
        dataset = dataset[2:-11]
        dataset = outputs[dataset][0]
        rule = pd.read_csv("Milestone"+ms[0]+"/"+inputs["RuleFilename"])
        binID, rule = rule["BIN_ID"][0], rule["RULE"][0]
        rule = rule.split(' ')
        con = []
        val = []
        if len(rule)==3:
            con.append(rule[1])
            val.append(int(rule[2]))
        else:
            con.append(rule[1])
            val.append(int(rule[2]))
            con.append(rule[5])
            val.append(int(rule[6]))
        bin = []
        for i in dataset["Signal"]:
            check = True
            for j in range(len(con)):
                if not ((con[j]=='>' and i>val[j]) or (con[j]=='<' and i<val[j])):
                    check = False
            if check:
                bin.append(binID)
            else:
                bin.append(-1)
        dataset["Bincode"] = bin
        outputs[name] = [dataset,dataset.shape[0]]
        #print(dataset)

    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def dataLoad(inputs, name, condition):
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        inputFile = inputs["Filename"]
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing DataLoad ("+ inputFile+")\n")    
        df = pd.read_csv("Milestone"+ms[0]+"/"+inputFile)
        outputs[name] = [df,df.shape[0]]
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def timeFunction(inputs, name, condition):
    #print(name, inputs, condition)
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if checkCondition(condition):
        funcInput = inputs["FunctionInput"]
        if funcInput[0] == '$':
            funcInput = funcInput[2:-13]
            while funcInput not in outputs.keys():
                continue
            funcInput = str(outputs[funcInput][1])
        t = inputs["ExecutionTime"]
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Executing TimeFunction ("+ funcInput+ ', '+ t+ ")\n")
        time.sleep(int(t))
    else:
        file1.write(str(datetime.datetime.now())+ ';'+ name+ " Skipped\n")
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Exit\n")

def flow(workflow,name,isSequential):
    file1.write(str(datetime.datetime.now())+ ';'+ name+ " Entry\n")
    if isSequential:                            #SEQUENTIAL
        print("sequential", name)
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
                    binning(workflow[k]["Inputs"] ,sname, condition) 
                elif workflow[k]["Function"] == "MergeResults":
                    merge(workflow[k]["Inputs"] , workflow[k]["Outputs"],sname, condition) 
                elif workflow[k]["Function"] == "ExportResults":
                    export(workflow[k]["FileName"],workflow[k]["DefectTable"],sname,condition)
    else:
        print("concurrent", name)                                       #CONCURRENT
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
                elif workflow[k]["Function"] == "Binning":
                    threadList.append(threading.Thread(target=binning,args=(workflow[k]["Inputs"] ,sname, condition)))
                elif workflow[k]["Function"] == "MergeResults":
                    threadList.append(threading.Thread(target=merge,args=(workflow[k]["Inputs"] ,workflow[k]["Outputs"], sname, condition)))
                elif workflow[k]["Function"] == "ExportResults":
                    threadList.append(threading.Thread(target=export,args=(workflow[k]["FileName"],workflow[k]["DefectTable"],sname,condition)))
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