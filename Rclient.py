#producer
from cgi import print_arguments
import pika as pk
import json as js
import pandas as pd
import random
import time

#MAX URL Y OP son +1 (contamos el 0 como una url o op)
MAXURL = 1
MAXOP = 8              

connection_parameters = pk.ConnectionParameters('localhost')
connection = pk.BlockingConnection(connection_parameters)
channel = connection.channel()

channel.exchange_declare(
    exchange='order',
    exchange_type='direct'
)



opcions = ["read_csv","apply","columns","groupby",
            "head","isin","items","max","min"]

urls = ["DATA.csv","DATA2.csv"]
for op in opcions:
    text=''
    uurrll=''
    #rand1 = random.randint(0,MAXOP)
    #op = opcions[rand1]
    rand2 = random.randint(0,MAXURL)
    uurrll+=urls[rand2]+' '
    vari = [5,6,7,8,9,10,11,12,13,14,15] #para isin
    print(op)

    #Lee el .csv
    if op == "read_csv":
        df = pd.read_csv(urls[rand2])
        text += df.to_string()+'\n\n'
        
    #Suma columnas de Score de distintos .csv
    if op == "apply": 
        def sum(a):
            a["Score"]=a["Score"]+a["Score2"]
            return a
                
        ddf = pd.read_csv(urls[rand2],sep=';')
        next = ((rand2+1)%MAXURL)
        uurrll+=urls[next]+' '
        df = pd.read_csv(urls[next], sep=';')
        ddf["Score2"] = df["Score"] 
        ddf = ddf.apply(func=sum, axis=1)
        del ddf["Score2"]           
        text = ddf.to_string()

    #Nombra las columnas que tiene el .csv               
    if op == "columns":
        df = pd.read_csv(urls[rand2],sep=';')
        te = df.columns.to_list()
        text += ' '.join(te)+'\n'

    #Agrupa por Score ?¿
    if op == "groupby":
        df = pd.read_csv(urls[rand2], sep=';')
        df.groupby(by='Score',level=0)
        text += df.to_string()+'\n\n'

    #Devuelve los x primeros de la tabla (x=random)
    if op == "head":
        df = pd.read_csv(urls[rand2], sep=';')
        r=random.randint(0,10)
        df = df.head(n=r)
        text += df.to_string()+'\n\n'

    #Comprueva si un numero esta dentro de la tabla vari (num>4)
    if op == "isin":
        df = pd.read_csv(urls[rand2], sep=';')
        a=[]           
        ddf = df["Score"].isin(vari)
        df["Pass"]=ddf
        text += df.to_string()+'\n\n'
            
    #Devuelve .csv separado por Columnas
    if op == "items":
        df = pd.read_csv(urls[rand2], sep=';')
        for label, content in df.items():  
            text += (f'label: {label}')+'\n'
            text += (f'content:\n {content}')
            text +='\n'
            
    #Devuelve el maximo Score
    if op == "max":
        df = pd.read_csv(urls[rand2], sep=';')
        d=df["Score"].max()
        text += str(d)+'\n'
            
    #Devuelve el minimo Score
    if op == "min":
        df = pd.read_csv(urls[rand2], sep=';')
        d=df["Score"].min()
        text += str(d)+'\n'

    order = {
        'opcio':op,
        'URL':uurrll,
        'return':text
    }

    channel.basic_publish(
        exchange='order',
        routing_key='order.notify',
        body=js.dumps(order)
    )
    time.sleep(1)
    

connection.close()


