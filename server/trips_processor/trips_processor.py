import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import time

class TripsProcessor :
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        if body.decode('utf-8')[0] == "E":
            logging.info("End of trips received")
            ch.close()

    def run(self):
        result = self.rabbit.declare_queue("trips")

        self.rabbit.declare_exchange("dispatcher","topic")

        self.rabbit.bind("dispatcher", "trips", "trips")

        self.rabbit.consume("trips", self.callback)

        self.rabbit.start_consuming()

        self.rabbit.close()