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
            self.rabbit.publish("","trip_duration", body) 
            self.rabbit.publish("","start_end_code_trip", body)
            ch.close()
            return
        rows = body.split(';')
        trip_duration_data = ""
        start_end_code_trip_data = ""

        for row in rows:
            cols = row.split(',') 
            if len(cols) < 6: continue
            city = cols[0] 
            date = cols[1].split(' ')[0]
            start_code = cols[2]
            end_code = cols[4]
            duration = cols[5]
            trip_duration_data += city + "," + date + "," + duration + ";" 
            start_end_code_trip_data += start_code + "," + end_code + ";"

        if trip_duration_data != "":
            self.rabbit.publish("","trip_duration",trip_duration_data)
        if start_end_code_trip_data != "":
            self.rabbit.publish("","start_end_code_trip",start_end_code_trip_data)


    def run(self):
        self.rabbit.bind("dispatcher", "trips", "trips")

        self.rabbit.consume("trips", self.callback)

        self.rabbit.close()