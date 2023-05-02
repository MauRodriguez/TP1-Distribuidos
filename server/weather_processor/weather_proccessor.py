import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import logging

class WeatherProccessor:
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        logging.info(body)

    def run(self):
        result = self.rabbit.declare_queue("test_queue")

        self.rabbit.consume("test_queue", self.callback)

        self.rabbit.start_consuming()

        self.rabbit.close()

