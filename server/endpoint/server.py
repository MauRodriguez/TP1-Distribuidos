from peer_socket import PeerSocket
from listen_socket import ListenSocket
import sys
sys.path.append("..")
from rabbitmq.rabbit import Rabbitmq

TRIP = "T"
STATION = "S"
WEATHER = "W"
END = "E"

class Server:
    def __init__(self, port):
        self.socket_listener = ListenSocket('', port, 1)
        self.socket_listener.bind_and_listen()
        self.keep_running = True
        self.rabbit = Rabbitmq()
    
    def run(self):
        client_socket = self.socket_listener.accept()

        result = self.rabbit.declare_exchange("dispatcher", "topic")
        
        while self.keep_running:
            type = client_socket.recv(1).decode("utf-8")
        
            lenght = client_socket.recv(4)

            msg = client_socket.recv(int.from_bytes(lenght, "little", signed=False)).decode("utf-8")
            
            routing_key = ""

            if type == WEATHER:
                routing_key = "weather"
            if type == TRIP:
                routing_key = "trips"

            if type == END:

                if msg == WEATHER:
                    routing_key = "weather"
                if msg == TRIP:
                    self.keep_running = False
                    routing_key = "trips"               

                self.rabbit.publish(exchange="dispatcher", routing_key=routing_key, msg=type + '|' + msg)
                
                continue
            
            self.rabbit.publish(exchange="dispatcher", routing_key=routing_key, msg=msg)

        
        self.rabbit.close()
        client_socket.close()
        self.socket_listener.close()