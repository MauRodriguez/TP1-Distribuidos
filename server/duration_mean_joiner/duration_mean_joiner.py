import logging
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
END = "E"
MAX_MSG_LENGHT = 8192
QUERY = "Q1"

class DurationMeanJoiner:
    def __init__(self):
        self.rabbit = Rabbitmq()
        self.duration = {}    

    def calculate_mean_duration(self):
        response = f"{QUERY};"

        for day in self.duration:  
            duration = self.duration[day][0]
            trips = self.duration[day][1]
            mean = duration / trips
            new_data = day + "," + str(mean) + ";"
            if len(response + new_data) > MAX_MSG_LENGHT:
                self.rabbit.publish("","result", response)
                response = f"{QUERY};"
            response += new_data
        
        self.rabbit.publish("", "result", response)                
        self.rabbit.publish("", "result", QUERY + "," + END + ";")

    def callback(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body == END:
            logging.info("End of distance mean received")
            self.calculate_mean_duration()
            ch.close()
            return
    
        rows = body.split(';')
        for row in rows:
            cols = row.split(',')
            if len(cols) < 3: continue
            if cols[0] not in self.duration:
                self.duration[cols[0]] = (float(cols[1]), int(cols[2]))
                continue           

            self.duration[cols[0]] = (self.duration[cols[0]][0] + float(cols[1]), self.duration[cols[0]][1] + int(cols[2]))

    def run(self):
        self.rabbit.consume("duration_mean", self.callback)
        self.rabbit.close()      