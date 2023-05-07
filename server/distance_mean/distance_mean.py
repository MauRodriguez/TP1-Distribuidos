import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"

class DistanceMean:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.distances = {}    

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of distances received")
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.distances:
                self.distances[cols[0]] = (float(cols[1]), 1)           

            current = self.distances[cols[0]]
            current = (current[0] + float(cols[1]), current[1] + 1)

    def run(self):
        self.rabbit.consume("distances", self.callback)
        self.rabbit.close()      