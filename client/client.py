import logging
import socket
from connect_socket import ConnectSocket

import csv
MAX_MSG_LENGHT = 8192
TRIP = "T"
STATION = "S"
WEATHER = "W"
END = "E"
RESULT = "R"

class Client:
    def __init__(self, server_address):
        splited_info = server_address.split(":")
        self.socket = ConnectSocket((splited_info[0],int(splited_info[1])))
        self.results = 0
    
    def run(self):
        try:        
            self.socket.connect()
            logging.info(f"action: connect | result: success")

            self.read_and_send("montreal", "weather", WEATHER)
            self.read_and_send("toronto", "weather", WEATHER)
            self.read_and_send("washington", "weather", WEATHER)
            self.send_msg(WEATHER, END.encode("utf-8"))
            logging.info(f"action: end of sending weather")

            self.read_and_send("montreal", "stations", STATION)
            self.read_and_send("toronto", "stations", STATION)
            self.read_and_send("washington", "stations", STATION)
            self.send_msg(STATION, END.encode("utf-8"))

            self.read_and_send("montreal", "trips", TRIP)
            #self.read_and_send("toronto", "trips", TRIP)
            #self.read_and_send("washington", "trips", TRIP)  
            self.send_msg(TRIP, END.encode("utf-8"))      
            logging.info(f"action: end of sending trips")

            logging.info(f"action: receiving queries results")    
            self.recv_queries_results()

        except OSError as e:
            pass
        self.socket.close()
        logging.info(f"action: close | result: success")
    
    def read_and_send(self, city, filename, type):
        path = f"data/{city}/{filename}.csv"

        with open(path,"r") as file:
            reader = csv.reader(file, delimiter='\n')
            next(reader, None) #Skip header

            batch = "".encode('utf-8')
            for i, line in enumerate(reader):
                # if i % 100 > 5 and type == TRIP:
                #     continue            
                data = city[0] + "," + (','.join(line)) + ";"
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

    def recv_queries_results(self):
        while self.results < 3:
            type = self.socket.recv(1).decode("utf-8")        
            lenght = self.socket.recv(4)
            msg = self.socket.recv(int.from_bytes(lenght, "little", signed=False)).decode("utf-8")
            if msg[-2] == END:
                self.results += 1
            logging.info(f"RESULT: {msg}")

    def stop(self):
        self.socket.close()
        logging.info(f"Client Gracefully closing client socket")

        