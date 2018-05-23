import random
import time
import logging
import datetime
import numpy as np
import requests
import threading
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-nw', type= int, dest='workers', default=1)
parser.add_argument('-rpw', type=int, dest='queries',default=sys.maxint)
parser.add_argument('-b', type=str, dest='bucket',default="beer-sample")
parser.add_argument('-vn', type=str , dest='viewName',default="view1")
parser.add_argument('-ddoc', type=str, dest='DDoc',default="DDoc1")
parser.add_argument('--logfile', type=str, dest='log_file',default="Total.log")
expected= ["","",""]
stale = ["false","update_after","ok"]

def main():
    parsing = parser.parse_args()
    workers = parsing.workers
    queries = parsing.queries
    viewName = parsing.viewName
    Ddoc = parsing.DDoc
    bucket = parsing.bucket
    LOG_FILENAME = parsing.log_file
    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, format='%(asctime)s | %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    query(workers, queries, viewName, Ddoc, bucket)

def query(workers,queries, viewName, Ddoc, bucket):
    URL= "http://127.0.0.1:9500/"+bucket+'/_design/'+Ddoc+'/_view/'+viewName+'?limit=6&stale='
    for i in xrange(3):
        expected[i] = requests.get(URL+stale[i], auth= ('Administrator', 'asdasd')).json()
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
            start=time.time()
            r = requests.get(self.URL+stale[i%3], auth=self.auth)
            if r.status_code != 200 or not checkEquals(expected[i%3],r.json()):
                logging.error("Error status Code %d ", r.status_code)
                break
            logging.info(time.time()-start)

    def join(self, timeout=None):
        super(qWorker, self).join(timeout)

def checkEquals(actual,expected):
    return actual == expected

if __name__ == '__main__':
    main()
