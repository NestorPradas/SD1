#consumer
import pika as pk
import json

connection_parameters = pk.ConnectionParameters('localhost')
connection = pk.BlockingConnection(connection_parameters)
channel = connection.channel()
queue = channel.queue_declare(queue='order_noti')
queue_name = queue.method.queue

channel.queue_bind(
    exchange='order',
    queue=queue_name,
    routing_key='order.notify' #binding key
)

def callback(ch, method, properties, body):
    payload = json.loads(body)

    op = payload['opcio']
    url = payload['URL']
    txt = payload['return']
    print('Opcio:'+op)
    print('URL:'+url)
    print(txt)

channel.basic_consume(queue=queue_name, auto_ack=True, 
    on_message_callback=callback)

print("Start")

channel.start_consuming()