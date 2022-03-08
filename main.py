import time
import yaml

file1 = open("Milestone1A.txt","w")

currTime = ""

def timeFunction(inputs, name):
    funcInput = inputs["FunctionInput"]
    t = inputs["ExecutionTime"]
    file1.write(currTime+ '; '+ name+ " Entry\n")
    file1.write(currTime+ '; '+ name+ " Executing TimeFunction ("+ funcInput+ ', '+ t+ ")\n")
    time.sleep(int(t))
    file1.write(currTime+ '; '+ name+ " Exit\n")

def concurrent(workflow,name):
    print("concurrent")

def sequential(workflow,name):
    file1.write(currTime+ '; '+ name+ " Entry\n")
    for k in workflow.keys():
        sname = name+'.'+k
        if workflow[k]["Type"] == "Flow":
            if workflow[k]["Execution"] == "Sequential":
                sequential(workflow[k]["Activities"],sname)
            elif workflow[k]["Execution"] == "Concurrent":
                concurrent(workflow[k]["Activities"],sname)
        elif workflow[k]["Type"] == "Task":
            if workflow[k]["Function"] == "TimeFunction":
                timeFunction(workflow[k]["Inputs"],sname)
    file1.write(currTime+ '; '+ name+ " Exit\n")

if __name__ == "__main__":
    yamlFile = open(f"Milestone1/Milestone1A.yaml",'r')
    workflow = yaml.safe_load(yamlFile)
    name = "M1A_Workflow"
    if workflow["M1A_Workflow"]["Execution"] == "Sequential":
        sequential(workflow["M1A_Workflow"]["Activities"],name)
    elif workflow["M1A_Workflow"]["Execution"] == "Concurrent":
        concurrent(workflow["M1A_Workflow"]["Activities"],name)

file1.close()