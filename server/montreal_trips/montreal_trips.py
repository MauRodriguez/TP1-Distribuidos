import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MONTREAL = "m"

class MontrealTrips:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.rainy_trips = {}    

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of start-end trip received")
            logging.info(self.rainy_trips)
            ch.close()
            return
    
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 5: continue
            if cols[0] == MONTREAL:
                filter_data += cols[2] + "," + cols[4] + ";"
                
        if filter_data != "":
            self.rabbit.publish("","montreal_trips",filter_data)

    def run(self):
        self.rabbit.consume("start_end_code_trip", self.callback)
        self.rabbit.close()      