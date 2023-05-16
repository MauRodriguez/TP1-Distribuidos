import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MONTREAL = "m"

class MontrealStations:
    def __init__(self, station_processor_amount, harversine_distance_amount):
        self.rabbit = Rabbitmq()   
        self.station_processor_amount = station_processor_amount
        self.harversine_distance_amount = harversine_distance_amount

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')        
        if body == END:
            self.station_processor_amount -= 1
            logging.info("recibo end en montreal stations")
            if self.station_processor_amount == 0:
                for i in range(0, self.harversine_distance_amount):
                    self.rabbit.publish("","montreal_stations",body)
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
    
    def stop(self):
        self.rabbit.close()
        logging.info(f"Montreal Stations Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("station_location", self.callback)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()      