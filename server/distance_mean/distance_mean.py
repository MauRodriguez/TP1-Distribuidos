import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MAX_MSG_LENGHT = 8192

class DistanceMean:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.distances = {} 

    def send_partial_distance(self):
        response = ""
        
        for city in self.distances:  
            distance = str(self.distances[city][0])
            trips = str(self.distances[city][1])
            new_data = city + "," + distance + "," + trips + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","distance_mean", response)
                response = ""
            response += new_data
                
        self.rabbit.publish("", "distance_mean", END)

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of distances received")
            self.send_partial_distance()
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.distances:
                self.distances[cols[0]] = (float(cols[1]), 1)
                continue           

            self.distances[cols[0]] = (self.distances[cols[0]][0] + float(cols[1]), self.distances[cols[0]][1] + 1)
            

    def run(self):
        self.rabbit.consume("distances", self.callback)
        self.rabbit.close()      