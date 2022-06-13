from asyncio.windows_events import NULL
from pickle import TRUE
from re import L
from sqlite3 import DatabaseError
from xmlrpc.server import SimpleXMLRPCServer
import logging
import simplejson as json
import redis
from multiprocessing import Process
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
        vari = ""
        if (operacio == "groupby") or (operacio == "head") or (operacio == "isin"):
            vari = to_send["Groupby"]
        jobID = to_send["JOBID"]
        url = to_send["URL"]
        text=''
        if operacio == "read_csv":
            for i in url:
                df = pd.read_csv(i)
                text += df.to_string()+'\n\n'
        
        if operacio == "apply":
            def sum(a):
                a["Score"]=a["Score"]+a["Score2"]
                
                return a
             
            ddf = pd.read_csv(url[0],sep=';')

            for i in url[1:]:
                df = pd.read_csv(i, sep=';')
                ddf["Score2"] = df["Score"] 
                ddf = ddf.apply(func=sum, axis=1)
                del ddf["Score2"]
                
            text = ddf.to_string()
                
        if operacio == "columns":
            for i in url:
                df = pd.read_csv(i,sep=';')
                te = df.columns.to_list()
                text += ' '.join(te)+'\n'

        if operacio == "groupby":

            for i in url:
                df = pd.read_csv(i, sep=';')
                df.groupby(by=vari,level=0)
                text += df.to_string()+'\n\n'

        if operacio == "head":

            for i in url:
                df = pd.read_csv(i, sep=';')
                df = df.head(n=int(vari))
                text += df.to_string()+'\n\n'

        if operacio == "isin":

            for i in url:
                df = pd.read_csv(i, sep=';')
                a=[]
                for i in vari.split(','):
                    a.append(int(i))
                v = pd.Series(a)
                
                ddf = df["Score"].isin(v)
                df["Pass"]=ddf
                text += df.to_string()+'\n\n'
        
        if operacio == "items":

            for i in url:
                df = pd.read_csv(i, sep=';')
                for label, content in df.items():  
                    text += (f'label: {label}')+'\n'
                    text += (f'content: {content}')
                
                text +='\n'
        
        if operacio == "max":

            for i in url:
                df = pd.read_csv(i, sep=';')
                d=df["Score"].max()
                text += str(d)+'\n'
        
        if operacio == "min":

            for i in url:
                df = pd.read_csv(i, sep=';')
                d=df["Score"].min()
                text += str(d)+'\n'
            
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

        if opcio == "items":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))

        if opcio == "max":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls
            }
            r.rpush('queue:email', json.dumps(data))
        
        if opcio == "min":
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

    def jobRun2(self, opcio, urls, group):
        global JOBID

        opcio = opcio[4:]
        urls = urls[1:-1]
        urls = urls.split(",")
        groupby = group
        if opcio == "head":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls,
                'Groupby': groupby
            }
            r.rpush('queue:email', json.dumps(data))

        if opcio == "groupby":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls,
                'Groupby': groupby
            }
            r.rpush('queue:email', json.dumps(data))
        
        if opcio == "isin":
            data={
                'JOBID': JOBID,
                'Operacio': opcio,
                'URL': urls,
                'Groupby': groupby
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