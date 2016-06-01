
import pexpect 
import os
import argparse
import subprocess
import time
import numpy
import pylab
import threading



def checkRunningStatus():
    nodeToolProg = pexpect.spawn("./cassandra/bin/nodetool version")
    out = nodeToolProg.expect(["ReleaseVersion:", "nodetool: Failed to connect"])

    # use the nodetool version command to see if cassandra is running or not
    if out == 1:
        print "Cassandra is not running."
        return False
    elif out == 0:
        print "Cassandra is running."
        return True


def startJMXTerm(writeFile):
    cmd = ["java", "-jar", "jmxterm-1.0-alpha-4-uber.jar", "-l", "127.0.0.1:7199", "-v", "silent", "-n"]
    jmxProc = subprocess.Popen(cmd, stdin = subprocess.PIPE, stdout = writeFile, stderr = writeFile)
    time.sleep(3)
    return jmxProc


def beginJMXRecording(jmxProc, readFile, valuesFile):
    cmdLiveSSTableCount = r'get -s -b org.apache.cassandra.metrics:keyspace=keyspace1,scope=standard1,name=LiveSSTableCount,type=ColumnFamily Value'
    cmdAllMemTablesDataSize = r'get -s -b org.apache.cassandra.metrics:keyspace=keyspace1,scope=standard1,name=AllMemtablesLiveDataSize,type=ColumnFamily Value'
    cmd95thPercentile = r'get -s -b org.apache.cassandra.metrics:name=Latency,scope=Write,type=ClientRequest 95thPercentile'

    time.sleep(1)

    jmxProc.stdin.write("%s\n" % cmdLiveSSTableCount)    
    liveSSTableCount = readFile.readline().strip()

    jmxProc.stdin.write("%s\n" % cmdAllMemTablesDataSize)
    dataSize = readFile.readline().strip()

    jmxProc.stdin.write("%s\n" % cmd95thPercentile)
    percentile = readFile.readline().strip()

    
    if liveSSTableCount == "":
        liveSSTableCount = 0

    if dataSize == "":
        dataSize = 0

    if percentile == "":
        percentile = 0.0


    assert(liveSSTableCount >= 0)
    assert(dataSize >= 0)
    assert(percentile >= 0.0)

    valuesFile.write("%s, %s, %s\n" % (liveSSTableCount, dataSize, percentile))


def stressTest():    
    stressExec1 = ["./cassandra/tools/bin/cassandra-stress", "mixed", "no-warmup", "n=100000", "-rate", "threads=10"]
    
    time.sleep(5)
    try:
        stressProc1 = subprocess.check_output(stressExec1, stderr = subprocess.STDOUT)
       
    except subprocess.CalledProcessError as e:
        print e.output
    time.sleep(5)


def prepareCSVData(csvFile):
    fp = open(csvFile, "r")

    lines = fp.readlines()

    jmxMetricsList = [[] for x in range(3)]
    for line in lines:
        values = line.split(',')
        for i in range(len(values)):
            jmxMetricsList[i].append(float(values[i]))
    
    return jmxMetricsList


def graphCSVData(metriclist):
    dataTypeDict = {0: "LiveSSTablesCount", 1: "AllMemTablesDataSize", 2: "95thPercentile"}
    kwargs = ['r.-', 'g.-', 'b.-']

    for i in range(3):
        currData = metriclist[i]

        timespan = len(metriclist[i])
        endtime = 1 * timespan
        timevals = numpy.linspace(0, endtime, timespan)

        fig = pylab.figure()
        fig.suptitle(dataTypeDict[i])
        pylab.grid(True)
        pylab.plot(timevals, currData, kwargs[i])
        pylab.xlabel('(seconds)')

        pylab.savefig('%s.png' % (dataTypeDict[i]))


def main():
    writeFile = open("output", "wb")
    readFile = open("output", "r")
    valuesFile = open("vals.csv", "w")
    

    if checkRunningStatus() == True:
        jmx = startJMXTerm(writeFile)


        stressBackgroundProcess = threading.Thread(target = stressTest)
        stressBackgroundProcess.start()
        print "about to start while loop"
        while stressBackgroundProcess.isAlive():
            print "in while"
            beginJMXRecording(jmx, readFile, valuesFile)  

        writeFile.close()
        readFile.close()
        valuesFile.close()
        
        metricLists = prepareCSVData("vals.csv")
        graphCSVData(metricLists)
                    

    else:
        print "noooooo"


main()