import logging
from connect_socket import ConnectSocket

import csv
MAX_MSG_LENGHT = 8192
TRIP = "T"
STATION = "S"
WEATHER = "W"
END = "E"

class Client:
    def __init__(self, server_address):
        splited_info = server_address.split(":")
        self.socket = ConnectSocket((splited_info[0],int(splited_info[1])))
    
    def run(self):
        self.socket.connect()
        logging.info(f"action: connect | result: success")

        self.read_and_send("montreal", "weather", WEATHER)
        logging.info(f"action: send_montreal | result: success")
        self.read_and_send("toronto", "weather", WEATHER)
        logging.info(f"action: send_toronto | result: success")
        self.read_and_send("washington", "weather", WEATHER)
        logging.info(f"action: send_washington | result: success")
        self.send_msg(END, WEATHER.encode("utf-8"))
        logging.info(f"action: send_end | result: success")

        # self.read_and_send("montreal", "stations", STATION)
        # self.read_and_send("toronto", "stations", STATION)
        # self.read_and_send("washington", "stations", STATION)
        # self.send_msg(END, STATION.encode("utf-8"))

        # self.read_and_send("montreal", "trips", TRIP)
        # self.read_and_send("toronto", "trips", TRIP)
        # self.read_and_send("washington", "trips", TRIP)  
        # self.send_msg(END, TRIP.encode("utf-8"))      
        
        self.socket.close()
        logging.info(f"action: close | result: success")
    
    def read_and_send(self, city, filename, type):
        #path = os.path.join(os.path.dirname(__file__),f"./data/montreal/weather.csv")
        path = f"data/{city}/{filename}.csv"

        with open(path,"r") as file:
            reader = csv.reader(file, delimiter='\n')
            next(reader, None) #Skip header

            batch = "".encode('utf-8')
            for i, line in enumerate(reader):            
                data = (','.join(line)) + ";"
                data_encoded = data.encode("utf-8")

                if(len(batch) + len(data_encoded)) >= MAX_MSG_LENGHT:
                    self.send_msg(type, batch)
                    batch = "".encode('utf-8')
                batch = batch + data_encoded

            self.send_msg(type, batch)
    
    def send_msg(self, type, msg): 

        self.socket.send(type.encode("utf-8"))
        self.socket.send(len(msg).to_bytes(4, "little", signed=False))
        self.socket.send(msg)
        

        