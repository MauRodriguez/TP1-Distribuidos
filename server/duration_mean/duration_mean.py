import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq

class DurationMean:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.rainy_trips = {}    

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == "E":
            logging.info("End of rainy trips received")
            logging.info(self.rainy_trips)
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.rainy_trips:
                self.rainy_trips[cols[0]] = (cols[1], 1)

            current = self.rainy_trips[cols[0]]
            current = (current[0] + cols[1], current[1] + 1)

    def run(self):
        self.rabbit.consume("rainy_trips", self.callback)
        self.rabbit.close()      