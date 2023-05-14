import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MAX_MSG_LENGHT = 8192

class TripCount :
    def __init__(self, station_processor_amount):
        self.rabbit = Rabbitmq()
        self.stations = {}
        self.counts = {}
        self.keep_running = True
        self.station_processor_amount = station_processor_amount

    def send_partial_count(self):
        response = ""
        
        for city in self.counts:  
            trips_16 = str(self.counts[city][0])
            trips_17 = str(self.counts[city][1])
            new_data = city + "," + trips_16 + "," + trips_17 + ";"
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
            self.send_partial_count()
            ch.close()
            
        rows = body.split(';')
        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if self.stations[cols[1]] not in self.counts:
                self.counts[self.stations[cols[1]]] = [0,0]

            trips_16 = self.counts[self.stations[cols[1]]][0]
            trips_17 = self.counts[self.stations[cols[1]]][1]

            if cols[0] == "2016":
                self.counts[self.stations[cols[1]]][0] = trips_16 + 1
            elif cols[0] == "2017":
                self.counts[self.stations[cols[1]]][1] = trips_17 + 1

    def callback_stations(self, ch, method, properties, body):
        body = body.decode('utf-8')        
        if body == END:
            self.station_processor_amount -= 1
            if self.station_processor_amount == 0:
                logging.info("End of stations code name received")
                ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            self.stations[cols[0]] = cols[1]   

    def stop(self):
        self.rabbit.close()
        self.keep_running = False
        logging.info(f"Trip Count Gracefully closing")          

    def run(self):
        try:
            self.rabbit.consume("stations_code_name", self.callback_stations)
            self.rabbit.close()        
            if self.keep_running: self.rabbit = Rabbitmq()
            self.rabbit.consume("16_17_trips", self.callback_trips)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()        