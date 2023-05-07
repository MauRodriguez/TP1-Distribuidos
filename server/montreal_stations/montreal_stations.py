import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq

class MontrealStations:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.rainy_trips = {}    

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == "E":
            logging.info("End of station location received")
            logging.info(self.rainy_trips)
            ch.close()
            return
    
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if (cols[0],cols[1]) in self.weathers:
                filter_data += cols[1] + "," + cols[2] + ";"
                
        if filter_data != "":
            self.rabbit.publish("","montreal_stations",filter_data)

    def run(self):
        self.rabbit.consume("station_location", self.callback)
        self.rabbit.close()      