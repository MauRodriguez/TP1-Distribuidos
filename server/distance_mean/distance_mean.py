import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MAX_MSG_LENGHT = 8192

class DistanceMean:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.distances = {} 

    def send_partial_distance(self):
        response = ""
        
        for city in self.distances:  
            distance = str(self.distances[city][0])
            trips = str(self.distances[city][1])
            new_data = city + "," + distance + "," + trips + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","distance_mean", response)
                response = ""
            response += new_data
                
        self.rabbit.publish("", "distance_mean", END)

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of distances received")
            self.send_partial_distance()
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 2: continue
            if cols[0] not in self.distances:
                self.distances[cols[0]] = (float(cols[1]), 1)
                continue           

            self.distances[cols[0]] = (self.distances[cols[0]][0] + float(cols[1]), self.distances[cols[0]][1] + 1)
            
    def stop(self):
        self.rabbit.close()
        logging.info(f"Distance Mean Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("distances", self.callback)        
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()      