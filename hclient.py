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
    print(proxy.jobRun(sys.argv[2], sys.argv[3]))
    