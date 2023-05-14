import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
from haversine import haversine
END = "E"

class HaversineDistance :
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.stations = {}
        self.distances = {}
        self.keep_running = True

    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of montreal trips received")
            self.rabbit.publish("","distances", body)
            ch.close()
        
        rows = body.split(';')
        result = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.stations or cols[1] not in self.stations: continue
            city_1 = self.stations[cols[0]]
            city_2 = self.stations[cols[1]]

            if (city_1,city_2) not in self.distances:
                self.distances[(city_1,city_2)] = haversine((city_1[1],city_1[2]),(city_2[1],city_2[2]))
            
            result += city_2[0] + "," + str(self.distances[(city_1,city_2)]) + ";"
       
        if result != "":
            self.rabbit.publish("","distances", result)
        

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
    
    def stop(self):
        self.rabbit.close()
        self.keep_running = False
        logging.info(f"Haversine Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("montreal_stations", self.callback_stations)
            self.rabbit.close()        
            if self.keep_running: self.rabbit = Rabbitmq()
            self.rabbit.consume("montreal_trips", self.callback_trips)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()        