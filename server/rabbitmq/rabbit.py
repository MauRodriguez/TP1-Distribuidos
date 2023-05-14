import pika
import logging

class Rabbitmq:
    def __init__(self):
        logging.getLogger("pika").propagate = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host = "rabbitmq"))
                
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count = 1)
        self.queues = []
        self.exchanges = []

    def declare_queue(self, name):
        return self.channel.queue_declare(queue=name, durable=True, auto_delete=True)

    def declare_exchange(self, name, type):
        self.channel.exchange_declare(exchange=name, exchange_type=type)
    
    def bind(self, exchange, queue, routing_key):
        if queue not in self.queues:
            self.declare_queue(queue)
        if exchange not in self.exchanges:
            self.declare_exchange(exchange, "direct")

        self.channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

    def publish(self, exchange, routing_key, msg):
        if exchange == "" and routing_key not in self.queues:
            self.declare_queue(routing_key)
        elif exchange not in self.exchanges:
            self.declare_exchange(exchange, "direct")
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg)

    def consume(self, queue, callback):
        if queue not in self.queues:
            self.declare_queue(queue)
        self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        self.start_consuming()
    
    def start_consuming(self):
        self.channel.start_consuming()

    def close(self):        
        if self.check_close(): return
        self.connection.close()
    
    def check_close(self):
        return (self.connection.is_closed or self.channel.is_closed)