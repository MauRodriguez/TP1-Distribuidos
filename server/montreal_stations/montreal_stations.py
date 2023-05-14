import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MONTREAL = "m"

class MontrealStations:
    def __init__(self):
        self.rabbit = Rabbitmq()   

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of station location received")
            self.rabbit.publish("","montreal_stations",body)
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