import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"

class RainyDays :
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.weathers = {}

    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of trips filtereds received")            
            self.rabbit.publish("","rainy_trips",body)
            ch.close()
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if (cols[0],cols[1]) in self.weathers:
                filter_data += cols[1] + "," + cols[2] + ";"
                
        if filter_data != "":
            self.rabbit.publish("","rainy_trips",filter_data)

    def callback_weather(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of weathers filtereds received")
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if float(cols[2]) > 30:
                self.weathers[(cols[0],cols[1])] = cols[2]

    def run(self):
        self.rabbit.consume("rain_amount", self.callback_weather)
        self.rabbit.close()        
        self.rabbit = Rabbitmq()
        self.rabbit.consume("trip_duration", self.callback_trips)
        self.rabbit.close()        