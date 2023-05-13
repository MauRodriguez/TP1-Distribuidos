import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MAX_MSG_LENGHT = 8192

class TripCount :
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.stations = {}
        self.counts = {}

    def send_partial_count(self):
        response = ""
        
        for count in self.counts:  
            city = count[0]
            year = count[1]
            trips = str(self.counts[count])
            new_data = city + "," + year + "," + trips + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","trip_count", response)
                response = ""
            response += new_data
            
        self.rabbit.publish("","trip_count", response)
        self.rabbit.publish("", "trip_count", END) 

    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of 16 17 trips received")
            logging.info(self.counts)
            self.send_partial_count()
            ch.close()
            
        rows = body.split(';')
        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if (self.stations[cols[1]],cols[0]) not in self.counts:
                self.counts[(self.stations[cols[1]],cols[0])] = 1
                continue
            self.counts[(self.stations[cols[1]],cols[0])] += 1

    def callback_stations(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of stations code name received")
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            self.stations[cols[0]] = cols[1]             

    def run(self):
        self.rabbit.consume("stations_code_name", self.callback_stations)
        self.rabbit.close()        
        self.rabbit = Rabbitmq()
        self.rabbit.consume("16_17_trips", self.callback_trips)
        self.rabbit.close()        