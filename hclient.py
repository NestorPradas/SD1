import xmlrpc.client
import sys


proxy = xmlrpc.client.ServerProxy('http://localhost:9000')
if (sys.argv[1] == "worker"):
    if (sys.argv[2] == "create"):
        proxy.crear_worker()
    if (sys.argv[2] == "delete"):
        proxy.delete_worker(sys.argv[3])
    if (sys.argv[2] == "list"):
        print(proxy.list_worker())
if (sys.argv[1] == "job"):
    if (len(sys.argv)==5):
        print(proxy.jobRun2(sys.argv[2], sys.argv[3], sys.argv[4]))
    else:
        print(proxy.jobRun(sys.argv[2], sys.argv[3]))