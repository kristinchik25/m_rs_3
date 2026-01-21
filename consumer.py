consumer.py
import pika
import json
import time
from datetime import datetime


def process_order(ch, method, properties, body):
    order = json.loads(body)
    print(f"[✓] Received order: {order['order_id']} at {datetime.now().isoformat()}")
    
    
    time.sleep(2)
    
    print(f"[✓] Processed order: {order['order_id']} at {datetime.now().isoformat()}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    
    channel.queue_declare(queue='orders', durable=True)
    channel.basic_qos(prefetch_count=1)


 
    channel.basic_consume(queue='orders', on_message_callback=process_order)


    print('[*] Waiting for orders. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()
