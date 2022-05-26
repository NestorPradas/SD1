from xmlrpc.server import SimpleXMLRPCServer
import logging
import requests
import simplejson as json
import redis
import string
from multiprocessing import Process
import dask.dataframe as dd
import csv
import pandas as pd

WORKERS = {}
WORKER_ID = 0
JOBID = 0

REDIS_PORT = 6379
REDIS_HOST = '127.0.0.1'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
r.flushdb()


def start_worker(n):

    while True:
        packed = r.blpop('queue:email')
        to_send = json.loads(packed[1])
        operacio = to_send["Operacio"]
        jobID = to_send["JOBID"]
        url = to_send["URL"]
        text=''
        if operacio == "read_csv":
            for i in url:
                df = pd.read_csv(i)
                text += df.to_string()+'\n\n'
        
        if operacio == "apply":
            a=0

        if operacio == "columns":
            for i in url:
                df = pd.read_csv(i)
                te = df.columns.to_list()
                text += ' '.join(te)+'\n'

        if operacio == "groupby":
            for i in url:
                df = pd.read_csv(i)
                df.groupby(axis=1)
                text += df.to_string()+'\n\n'
            
        r.rpush(jobID, json.dumps(text))
    
    return True

class ServerMethods:

    def crear_worker(self):
        global WORKERS
        global WORKER_ID

        proc = Process(target=start_worker, args=(WORKER_ID,))
        proc.start()
        WORKERS[WORKER_ID] = proc

        WORKER_ID += 1
        return True         # Limitació del xmlrpc


    def delete_worker(self, num):
        num = int(num) - 1
        if not WORKERS[num]:
            return -1
        
        WORKERS[num].terminate()
        return True


    def list_worker(self):
        ABCD = ''
        for worker in WORKERS:
            ABCD += str(WORKERS[worker]) + "\n"

        return ABCD


    def jobRun(self, opcio, urls):
        global JOBID

        opcio = opcio[4:]
        urls = urls[1:-1]
        urls = urls.split(",")
        if opcio == "read_csv":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))

        if opcio == "apply":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))
        
        if opcio == "columns":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))

        if opcio == "groupby":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))

        packed = r.blpop(JOBID)
        to_send = json.loads(packed[1])
        JOBID += 1
        return to_send

    

if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    server = SimpleXMLRPCServer(
        ('localhost', 9000),
        logRequests=True,
    )

    server.register_instance(ServerMethods())

    # Start the server
    try:
        print('Use Control-C to exit')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Exiting')