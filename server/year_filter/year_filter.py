import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
YEAR1 = "2016"
YEAR2 = "2017"

class YearFilter:
    def __init__(self):
        self.rabbit = Rabbitmq()  

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of 16-17 trips received")
            self.rabbit.publish("","16_17_trips", body)
            ch.close()
            return
    
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] == YEAR1 or cols[0] == YEAR2:
                filter_data += cols[0] + "," + cols[1] + ";"

        if filter_data != "":
            self.rabbit.publish("","16_17_trips",filter_data)

    def run(self):
        self.rabbit.consume("year_and_start_station", self.callback)
        self.rabbit.close()      