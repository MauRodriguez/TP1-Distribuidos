import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
from haversine import haversine
END = "E"

class HaversineDistance :
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.stations = {}


    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of montreal trips received")
            self.rabbit.publish("","distances", body)
            ch.close()
        
        rows = body.split(';')
        distances = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            city_1 = self.stations[cols[0]]
            city_2 = self.stations[cols[1]]

            distances += city_2[0] + "," + str(haversine((city_1[1],city_1[2]),(city_2[1],city_2[2]))) + ";"
       
        if distances != "":
            self.rabbit.publish("","distances", distances)
        

    def callback_stations(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of montreal stations received")
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 4: continue
            self.stations[cols[0]] = (cols[1],float(cols[2]), float(cols[3]))

    def run(self):
        self.rabbit.consume("montreal_stations", self.callback_stations)
        self.rabbit.close()        
        self.rabbit = Rabbitmq()
        self.rabbit.consume("montreal_trips", self.callback_trips)
        self.rabbit.close()        