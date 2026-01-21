import pika
import json
import uuid
from datetime import datetime


def send_order():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    
    channel.queue_declare(queue='orders', durable=True)


    order = {
        "order_id": str(uuid.uuid4()),
        "customer": "Cassandra",
        "amount": 99.99,
        "status": "pending"
    }


    channel.basic_publish(
        exchange='',
        routing_key='orders',
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=2,  
        )
    )
    print(f"[x] Sent order: {order['order_id']} at {datetime.now().isoformat()}")
    connection.close()


if __name__ == "__main__":
    for i in range(5):
        send_order()
