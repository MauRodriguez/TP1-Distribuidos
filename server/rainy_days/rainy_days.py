import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import pika
END = "E"

class RainyDays :
    def __init__(self, trip_processor_amount, weather_processor_amount, duration_mean_amount):
        self.rabbit = Rabbitmq()
        self.weathers = {}
        self.keep_running = True
        self.trip_processor_amount = trip_processor_amount
        self.weather_processor_amount = weather_processor_amount
        self.duration_mean_amount = duration_mean_amount

    def callback_trips(self, ch, method, properties, body):
        body = body.decode('utf-8')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if body == END:
            self.trip_processor_amount -= 1
            if self.trip_processor_amount == 0:
                logging.info("End of trips filtereds received")
                for i in range(0, self.duration_mean_amount):            
                    self.rabbit.publish("","rainy_trips",body)
                ch.close()
            return
        
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
        ch.basic_ack(delivery_tag=method.delivery_tag)        
        if body == END:
            self.weather_processor_amount -= 1
            if self.weather_processor_amount == 0:
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
            if self.keep_running: 
                self.rabbit = Rabbitmq()
            self.rabbit.consume("trip_duration", self.callback_trips)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()        