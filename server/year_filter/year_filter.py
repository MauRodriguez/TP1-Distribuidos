import logging
import sys
sys.path.append("..")
import pika
from rabbitmq.rabbit import Rabbitmq
END = "E"
YEAR1 = "2016"
YEAR2 = "2017"

class YearFilter:
    def __init__(self, trip_processor_amount):
        self.rabbit = Rabbitmq() 
        self.trip_processor_amount = trip_processor_amount

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')        
        if body == END:
            self.trip_processor_amount -= 1
            if self.trip_processor_amount == 0:
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
        
    def stop(self):
        self.rabbit.close()
        logging.info(f"Year Filter Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("year_and_start_station", self.callback)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()      