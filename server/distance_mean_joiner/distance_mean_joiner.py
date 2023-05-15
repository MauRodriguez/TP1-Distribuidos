import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"
MAX_MSG_LENGHT = 8192
QUERY = "Q3"

class DistanceMeanJoiner:
    def __init__(self, distance_mean_amount):
        self.rabbit = Rabbitmq()
        self.distances = {}    
        self.distance_mean_amount = distance_mean_amount

    def calculate_mean_distance(self):
        response = f"{QUERY};"
        
        for city in self.distances:  
            distance = self.distances[city][0]
            trips = self.distances[city][1]
            mean = distance / trips
            if mean < 6: continue
            new_data = city + "," + str(mean) + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","distance_join", response)
                response = f"{QUERY};"
            response += new_data
        
        self.rabbit.publish("", "result", response)                
        self.rabbit.publish("", "result", QUERY + "," + END + ";" )

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            self.distance_mean_amount -= 1
            if self.distance_mean_amount == 0:
                logging.info("End of distance mean received")
                self.calculate_mean_distance()
                ch.close()
            return
    
        rows = body.split(';')
        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if cols[0] not in self.distances:
                self.distances[cols[0]] = (float(cols[1]), int(cols[2]))
                continue           

            self.distances[cols[0]] = (self.distances[cols[0]][0] + float(cols[1]), self.distances[cols[0]][1] + int(cols[2]))

    def stop(self):
        self.rabbit.close()
        logging.info(f"Distance Mean Joiner Gracefully closing")
    
    def run(self):
        try:
            self.rabbit.consume("distance_mean", self.callback)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()      