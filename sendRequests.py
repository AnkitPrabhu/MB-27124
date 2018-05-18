import random
import time
import logging
import datetime
import plotly.plotly as ply
import plotly.graph_objs as go
import numpy as np
import requests
import threading
import argparse

LOG_FILENAME = 'Total.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, format='%(asctime)s | %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument('-nw', type= int, dest='workers', default=1)
parser.add_argument('-rpw', type=int, dest='queries',default=10000000)
parser.add_argument('-b', type=str, dest='bucket',default="beer-sample")
parser.add_argument('-vn', type=str , dest='viewName',default="view1")
parser.add_argument('-ddoc', type=str, dest='DDoc',default="DDoc1")
def main():
    parsing = parser.parse_args()
    workers = parsing.workers
    queries = parsing.queries
    viewName = parsing.viewName
    Ddoc = parsing.DDoc
    bucket = parsing.bucket
    query(workers, queries, viewName, Ddoc, bucket)

def query(workers,queries, viewName, Ddoc, bucket):
    URL= "http://127.0.0.1:9500/"+bucket+'/_design/'+Ddoc+'/_view/'+viewName+'?limit=6'
    supervisor = qSupervisor(URL, queries, workers)
    supervisor.run()

class qSupervisor():
    def __init__(self, URL, queries, workers):
        self.node = URL
        self.workers = workers
        self.queries = queries

    def run(self):
        slaves = []
        for load in xrange(self.workers):
            slave = qWorker(self.node, self.queries)
            slaves.append(slave)
            slave.start()

        for slave in slaves:
            slave.join()

class qWorker(threading.Thread):
    def __init__(self, URL, queries):
        super(qWorker, self).__init__()
        self.URL = URL
        self.auth = ('Administrator', 'asdasd')
        self.queries = queries

    def run(self):
        for i in xrange(self.queries):
            i+=1
            start=time.time()
            r = requests.get(self.URL, auth=self.auth)
            logging.info(time.time()-start)

    def join(self, timeout=None):
        super(qWorker, self).join(timeout)


if __name__ == '__main__':
    main()
