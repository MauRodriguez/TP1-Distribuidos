import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"

class StationsProcessor :
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of stations received")
            self.rabbit.publish("","station_location", body)
            ch.close()
            return
        rows = body.split(';')
        filter_data = ""
        
        for row in rows:
            cols = row.split(',') 
            if len(cols) < 5: continue
            city = cols[0]
            code = cols[1] 
            name = cols[2]
            lat = cols[3]
            long = cols[4]
            filter_data += city + "," + code + "," + name + "," + lat + "," + long + ";"  

        self.rabbit.publish("","station_location",filter_data) 

    def run(self):
        self.rabbit.bind("dispatcher", "stations", "stations")

        self.rabbit.consume("stations", self.callback)

        self.rabbit.close()