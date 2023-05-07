import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MONTREAL = "m"

class MontrealStations:
    def __init__(self):
        self.rabbit = Rabbitmq()   

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of station location received")
            ch.close()
            return
    
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 5: continue
            if cols[0] == MONTREAL:
                code = cols[1] 
                name = cols[2]
                lat = cols[3]
                long = cols[4]
                filter_data += code + "," + name + "," + lat + "," + long + ";"
                
        if filter_data != "":
            self.rabbit.publish("","montreal_stations",filter_data)

    def run(self):
        self.rabbit.consume("station_location", self.callback)
        self.rabbit.close()      