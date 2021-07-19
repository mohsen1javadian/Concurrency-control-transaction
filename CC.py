from os import waitpid
import networkx as nx
from networkx.algorithms.cycles import simple_cycles

# global variables /////////////////////////////////////////

# list of database records
Records = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# task array and latest operation index of each task
TaskArray = []
ExeIndex = [0]

# locks table
Locks = []
for i in range(10):Locks.append({})

# [{1:s,2:s)},{2:x}]

# pointer to latest log id for each task
LogIdList = [-1]  # *len(TaskArray)

# wait graph for check dead lock
WaitGraph = nx.DiGraph()
FixDeadLockWay = 0

# create log file and log index ,timestamp
LogFile = open('log.txt', 'w+')
LogIndex = 0


#//////////////////////////////////////////////////////////

# Functions //////////////////////////////////////////////


# return True if success False if not
def GetSharedLock(RecordId, TaskId):
    global Locks

    if 'x' not in Locks[RecordId].values():
        Locks[RecordId][TaskId] = 's'
        return True
    elif TaskId in Locks[RecordId]:
        return True
    else: return False
# end func


# return True if success False if not
def GetExclusiveLock(RecordId, TaskId):
    global Locks

    if len(Locks[RecordId]) == 0:
        Locks[RecordId][TaskId] = 'x'
        return True
    elif len(Locks[RecordId]) == 1 and TaskId in Locks[RecordId]:
        Locks[RecordId][TaskId] = 'x'
        return True
    else: return False
# end func


def ReleaseLocks(TaskId):
    global Locks
    global WaitGraph

    for lock in Locks:
        if TaskId in lock:
            del lock[TaskId]
    if WaitGraph.has_node(TaskId):
        WaitGraph.remove_node(TaskId)
# end func


def SaveWriteLog(TaskId, RecordId, OldValue, NewValue):
    global LogFile
    global LogIdList
    global LogIndex
    
    LogFile.write("W,{},{},{},{},{},{}\n".format(LogIndex, TaskId, RecordId,OldValue, NewValue,LogIdList[TaskId]))
    LogFile.flush()
    LogIdList[TaskId] = LogIndex
    LogIndex+=1

# end func


def SaveReadLog(TaskId, RecordId, ReadValue):
    global LogFile
    global LogIdList
    global LogIndex
    
    LogFile.write("R,{},{},{},{},{}\n".format(LogIndex, TaskId, RecordId,ReadValue, LogIdList[TaskId]))
    LogFile.flush()
    LogIdList[TaskId] = LogIndex
    LogIndex+=1
# end func


def SaveCommitLog(TaskId):
    global LogFile
    global LogIdList
    global LogIndex

    LogFile.write("C,{},{},{}\n".format(LogIndex, TaskId, LogIdList[TaskId - 1]))
    LogFile.flush()
    LogIdList[TaskId] = LogIndex
    LogIndex+=1
# end func


def SaveAbortLog(TaskId):
    global LogFile
    global LogIdList
    global LogIndex

    LogFile.write("A,{},{},{}\n".format(LogIndex, TaskId, LogIdList[TaskId - 1]))
    LogFile.flush()
    LogIdList[TaskId] = LogIndex
    LogIndex+=1
# end func


def AbortTask(TaskId):
    global LogFile
    global LogIdList
    global Records
    global WaitGraph
    global LogIndex

    LogFile.seek(0)
    logLines = LogFile.readlines()
    logid = LogIdList[TaskId]
    while logid != -1:
        splitLine = logLines[logid].split(',')
        if splitLine[0] == 'W':
            Records[int(splitLine[3])] = int(splitLine[4])
        logid = int(splitLine[-1])
    #remove wating edge
    if WaitGraph.has_node(TaskId):
        WaitGraph.remove_node(TaskId)

    SaveAbortLog(TaskId)
    ReleaseLocks(TaskId)
#end func


def FixDeadLock():
    global WaitGraph

    for cycle in nx.simple_cycles(WaitGraph):
        if FixDeadLockWay == 0:
            AbortTask(max(cycle))
        elif FixDeadLockWay == 1:
            degree = WaitGraph.degree
            tid = max(cycle,key=degree.get)
            AbortTask(tid)
    if len(list(nx.simple_cycles(WaitGraph))) != 0:
        FixDeadLock()
#end func


def AddWaitEdge(TaskId, RecordId):
    global Locks
    global WaitGraph
    for tid in Locks[RecordId].keys():
        if tid!=TaskId:
            WaitGraph.add_edge(TaskId, tid)
    if len(list(nx.simple_cycles(WaitGraph))) != 0:
        FixDeadLock()
# end func


# main program ////////////////////////////////////
def main():
    global Records
    global TaskArray
    global ExeIndex
    global LogIdList
    global LogFile
    
    FixDeadLockWay = int(input("Enter way of select task to abort when dead lock apear:"))
    print("Enter each task, line by line.Enter empty line to end input task.")
    while True:
        task = input()
        if len(task) < 1:break
        TaskArray.append(task.split(';'))
    #end while
    
    ExeIndex*=len(TaskArray)
    LogIdList*=len(TaskArray)
    TaskId = 0
    while True:
        uniqueExe = list(set(ExeIndex))
        if len(uniqueExe) == 1 and uniqueExe[0] == -1:break #if all task execute C and set its exeIndex to -1
        
        if ExeIndex[TaskId] == -1: #this task finished
            TaskId = 0 if TaskId + 1 == len(TaskArray) else TaskId + 1
            continue

        operation = TaskArray[TaskId][ExeIndex[TaskId]]
        if operation[0] == 'W':
            recordId = int(operation[2:-1].split(',')[0])
            value = int(operation[2:-1].split(',')[1])
            if GetExclusiveLock(recordId,TaskId):
                SaveWriteLog(TaskId,recordId,Records[recordId],value)
                Records[recordId] = value
                ExeIndex[TaskId]+=1
            else :
                AddWaitEdge(TaskId,recordId)
        elif operation[0] == 'R':
            recordId = int(operation[2:-1])
            if GetSharedLock(recordId,TaskId):
                SaveReadLog(TaskId,recordId,Records[recordId])
                ExeIndex[TaskId]+=1
            else :
                AddWaitEdge(TaskId,recordId)
        elif operation[0] == 'C':
            ReleaseLocks(TaskId)
            SaveCommitLog(TaskId)
            ExeIndex[TaskId] = -1
        
        TaskId = 0 if TaskId + 1 == len(TaskArray) else TaskId + 1
    #end while

    print('records:',Records)

    LogFile.seek(0)
    logs = LogFile.readlines()
    LogFile.close()
    for line in logs: print(line)

# end main ////////////////////////////////////////

# run program
main()