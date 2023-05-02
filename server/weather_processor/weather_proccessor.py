import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import logging
import time

class WeatherProccessor:
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        if body.decode('utf-8')[0] == "E":
            logging.info("End of weathers received")
            ch.close()

    def run(self):
        result = self.rabbit.declare_queue("weather")

        self.rabbit.declare_exchange("dispatcher","topic")

        self.rabbit.bind("dispatcher", "weather", "weather")

        self.rabbit.consume("weather", self.callback)

        self.rabbit.start_consuming()

        self.rabbit.close()

