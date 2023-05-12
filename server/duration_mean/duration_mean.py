import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MAX_MSG_LENGHT = 8192

class DurationMean:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.trips_duration = {} 

    def send_partial_duration(self):
        response = ""
        
        for day in self.trips_duration:  
            duration = str(self.trips_duration[day][0])
            trips = str(self.trips_duration[day][1])
            new_data = day + "," + duration + "," + trips + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","duration_mean", response)
                response = ""
            response += new_data
                
        self.rabbit.publish("", "duration_mean", END)   

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of rainy trips received")
            self.send_partial_duration()
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.trips_duration:
                self.trips_duration[cols[0]] = (float(cols[1]), 1)

            current = self.trips_duration[cols[0]]
            self.trips_duration[cols[0]] = (current[0] + float(cols[1]), current[1] + 1)

    def run(self):
        self.rabbit.consume("rainy_trips", self.callback)
        self.rabbit.close()      