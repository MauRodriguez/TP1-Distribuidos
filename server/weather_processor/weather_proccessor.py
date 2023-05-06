import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import logging
END = "E"

class WeatherProccessor:
    def __init__(self):
        self.rabbit = Rabbitmq()

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of weathers received")
            self.rabbit.publish("","test",body)
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

        self.rabbit.publish("","test",filter_data)   

    def run(self):
        self.rabbit.bind("dispatcher", "weather", "weather")
        self.rabbit.consume("weather", self.callback)
        self.rabbit.close()

