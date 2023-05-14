import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MAX_MSG_LENGHT = 8192
QUERY = "Q2"

class TripCountJoiner:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.count = {} 

    def calculate_count_duplicate(self):
        response = f"{QUERY};"

        for city in self.count:  
            trips_16 = self.count[city][0]
            trips_17 = self.count[city][1]
            if (2 * trips_16) > trips_17: continue
            new_data = city + "," + str(trips_16) + "," + str(trips_17) + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","result", response)
                response = f"{QUERY};"
            response += new_data
        
        self.rabbit.publish("", "result", response)                
        self.rabbit.publish("", "result", QUERY + "," + END + ";")

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of trip partial count received")
            self.calculate_count_duplicate()
            ch.close()
            return
    
        rows = body.split(';')
        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if cols[0] not in self.count:
                self.count[cols[0]] = [0,0]                           
            trips_16 = self.count[cols[0]][0]
            trips_17 = self.count[cols[0]][1]

            self.count[cols[0]] = [trips_16 + int(cols[1]), trips_17 + int(cols[2])]
        
    def stop(self):
        self.rabbit.close()
        logging.info(f"Trip Count Joiner Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("trip_count", self.callback)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()      