import random
import datetime
import plotly.plotly as ply
import plotly.graph_objs as go
import numpy as np
import argparse

LOG_FILENAME = 'Insert.log'

parser = argparse.ArgumentParser()
parser.add_argument('-mi', type=int, dest='minInterval', default=1)
parser.add_argument('-si', type=int, dest='secInterval', default=0)
parser.add_argument('-lf', type=str, dest='logfilename', default="Total.log")

def main():
    parsing = parser.parse_args()
    LOG_FILENAME = parsing.logfilename
    minInterval = parsing.minInterval
    secInterval = parsing.secInterval
    CreateGraph(LOG_FILENAME, minInterval,secInterval)

def CreateGraph(LOG_FILENAME, minInterval,secInterval):
    data = ParseValue(LOG_FILENAME,minInterval,secInterval)
    ply.plot(data,filename="TotalQueries"+str(random.random()),auto_open=False)

def ParseValue(fileName,minInterval,secInterval):
    delta = datetime.timedelta(minutes=minInterval,seconds= secInterval)
    with open(fileName, 'r') as f:
        Xset = []
        Yset = []
        width= []
        count=0
        startTime= None
        for line in f:
            timestr, query = line.split('|')
            timestamp = parseTime(timestr)
            if startTime==None:
                count=1
                startTime= timestamp
            elif check(timestamp, startTime, delta):
                Yset.append(count)
                Xset.append(startTime)
                width.append(1)
                count= parseQuery(query)
                startTime = timestamp
            else:
                count += parseQuery(query)
    Yset.append(count)
    Xset.append(startTime)
    Y = np.array(Yset)
    data = []
    trace = go.Scatter(x=Xset, y=Y)
    data.append(trace)
    return data

def parseQuery(Query):
    return 1 if Query.strip()[0]=="S" else 0

def parseTime(time):
    time = time.strip()
    return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

def check(current,old,delta):
    return current >= old+ delta

if __name__ == "__main__":
    main()
