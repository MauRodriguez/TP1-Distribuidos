import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"

class RainyDays :
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.weathers = {}
        self.keep_running = True

    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of trips filtereds received")            
            self.rabbit.publish("","rainy_trips",body)
            ch.close()
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if (cols[0],cols[1]) in self.weathers:
                filter_data += cols[1] + "," + cols[2] + ";"
                
        if filter_data != "":
            self.rabbit.publish("","rainy_trips",filter_data)

    def callback_weather(self, ch, method, properties, body):
        body = body.decode('utf-8')
        
        if body == END:
            logging.info("End of weathers filtereds received")
            ch.close()
            return
    
        rows = body.split(';')

        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if float(cols[2]) > 30:
                self.weathers[(cols[0],cols[1])] = cols[2]
    
    def stop(self):
        self.rabbit.close()
        self.keep_running = False
        logging.info(f"Rainy Days Gracefully closing")

    def run(self):
        try:
            self.rabbit.consume("rain_amount", self.callback_weather)
            self.rabbit.close()        
            if self.keep_running: self.rabbit = Rabbitmq()
            self.rabbit.consume("trip_duration", self.callback_trips)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()        