import matplotlib.pyplot as plt
import numpy as np
import pylab


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
        timevals = np.linspace(0, endtime, timespan)

        fig = pylab.figure()
        fig.suptitle(dataTypeDict[i])
        pylab.grid(True)
        pylab.plot(timevals, currData, kwargs[i])
        pylab.xlabel('(seconds)')

        pylab.savefig('%s.png' % (dataTypeDict[i]))

vals = prepareCSVData("vals.csv")

graphCSVData(vals)
