from peer_socket import PeerSocket
from listen_socket import ListenSocket
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq
import logging

TRIP = "T"
STATION = "S"
WEATHER = "W"
END = "E"
RESULT = "R"

class Server:
    def __init__(self, port):
        self.socket_listener = ListenSocket('', port, 1)
        self.socket_listener.bind_and_listen()
        self.keep_receiving = True
        self.client_socket = None
        self.rabbit = Rabbitmq()
        self.queries_finished = 0
    
    def run(self):
        self.client_socket = self.socket_listener.accept()
        
        self.receive()
        self.rabbit.consume("result", self.send_results)
        
        self.rabbit.close()
        self.client_socket.close()
        self.socket_listener.close()

    def send_results(self, ch, method, properties, body):
        body = body.decode('utf-8')
        if body[-2] == END:
            self.queries_finished += 1

            msg = body.encode("utf-8")
            self.client_socket.send(END.encode("utf-8"))
            self.client_socket.send(len(msg).to_bytes(4, "little", signed=False))
            self.client_socket.send(msg)
            
            if self.queries_finished == 3:
                logging.info("End of queries")        
                ch.close()
            return     

        msg = body.encode("utf-8")

        self.client_socket.send(RESULT.encode("utf-8"))
        self.client_socket.send(len(msg).to_bytes(4, "little", signed=False))
        self.client_socket.send(msg)

    def receive(self):
        while self.keep_receiving:
            type = self.client_socket.recv(1).decode("utf-8")        
            lenght = self.client_socket.recv(4)
            msg = self.client_socket.recv(int.from_bytes(lenght, "little", signed=False)).decode("utf-8")
            
            routing_key = ""

            if type == WEATHER:
                routing_key = "weather"
            if type == STATION:
                routing_key = "stations"
            if type == TRIP:
                routing_key = "trips"
                if msg == END:
                    self.keep_receiving = False          
            
            self.rabbit.publish(exchange="dispatcher", routing_key=routing_key, msg=msg)