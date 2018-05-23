import random
import sys
import datetime
import plotly.plotly as ply
import plotly.graph_objs as go
import numpy as np
import argparse
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', type=str, dest='folder', default="n_0")

path= "/Users/ankitdamodarprabhu/couchbaseTests/ns_server/logs/"
FileName= "/couchdb.log"
folder = "n_0"
totalRequest= [0,0]

def main():
    labels= ["I_OK","I_updateAfter","I_false","E_OK","E_updateAfter","E_false"]
    data = CreateData(labels)
    print "Internal: ",totalRequest[0],"External: ",totalRequest[1]
    ply.plot(data,filename="CouchDBQueriesQueries"+str(random.random()),auto_open=False)

def CreateData(labels):
    Xset=[]
    Yset=[]
    count= 0
    oldValue = [0,0,0,0,0,0]
    parsing = parser.parse_args()
    folder = parsing.folder
    with open(path+folder+FileName) as fd:
        for line in fd:
            if checkline(line):
                timeStamp= parseTime(line)
                newTable =parseData(fd)

                if len(Yset) and oldValue==newTable:
                    count+=1
                    if count==7:
                        break
                elif len(Yset):
                    count=0

                table= newTable
                Xset.append(timeStamp)
                oldValue= newTable
                Yset.append(table)
    Y = np.array(Yset)
    data = []
    for i, y in enumerate(Y.T):
        trace = go.Scatter(x=Xset, y=y, name=labels[i])
        data.append(trace)
    return data

def substract(old,new):
    for index in xrange(6):
        new[index]-=old[index]
    return new

def parseTime(line):
    timestamp= line.split(",")[1]
    return datetime.datetime.strptime(timestamp[:19], '%Y-%m-%dT%H:%M:%S')

def parseData(fd):
    global totalRequest
    data= [0,0,0,0,0,0]
    for line in fd:
        if line=="---\n":
            break
        line = line.strip("\n").split("|")
        for i in xrange(2):
            index = line[i+1].find("=")
            trimmed= line[i+1][index+1:].rstrip("} ")
            trimmed= trimmed.lstrip("{")
            array = trimmed.split(",")
            array= Create(array)
            for j in xrange(3):
                data[(4-i)%4+j]+= int(array[j])
                totalRequest[i]+=int(array[j])
    return data

def Create(string):
    array=[0,0,0]
    for i in xrange(3):
        index= string[i].find(":")
        array[i]= string[i][index+1:]
    return array

def checkline(line):
    return line.find("Query-Volume") != -1

if __name__=="__main__":
    main()
