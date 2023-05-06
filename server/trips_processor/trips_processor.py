import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"

class TripsProcessor :
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of trips received")
            self.rabbit.publish("","test2", body) 
            ch.close()
            return
        rows = body.split(';')
        filter_data = ""
        
        for row in rows:
            cols = row.split(',') 
            if len(cols) < 2: continue
            city = cols[0] 
            date = cols[1].split(' ')[0]
            duration = cols[5]
            filter_data += city + "," + date + "," + duration + ";"  

        self.rabbit.publish("","test2",filter_data) 

    def run(self):
        self.rabbit.bind("dispatcher", "trips", "trips")

        self.rabbit.consume("trips", self.callback)

        self.rabbit.close()