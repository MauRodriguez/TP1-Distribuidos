import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import logging
import pika
END = "E"

class WeatherProccessor:
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if body == END:
            logging.info("End of weathers received")
            self.rabbit.publish("","rain_amount",body)
            ch.close()
            return
        rows = body.split(';')
        filter_data = ""

        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            city = cols[0]
            date = cols[1]  
            prec = cols[2]
            filter_data += city + "," + date + "," + prec + ";"  

        self.rabbit.publish("","rain_amount",filter_data) 

    def stop(self):
        self.rabbit.close()
        logging.info(f"Weather Processor Gracefully closing")  

    def run(self):
        try:
            self.rabbit.bind("dispatcher", "weather", "weather")
            self.rabbit.consume("weather", self.callback)
        except pika.exceptions.ChannelWrongStateError as e:
            logging.info(f"Exiting. Pika Exception: {e}")
        except OSError as e:
            logging.info(f"Exiting. OSError: {e}")
        except AttributeError as e:
            logging.info(f"Exiting. AttributeError: {e}")
        self.rabbit.close()

