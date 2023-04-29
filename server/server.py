from peer_socket import PeerSocket
from listen_socket import ListenSocket
import logging
TRIP = "T"
STATION = "S"
WEATHER = "W"
END = "E"

class Server:
    def __init__(self, port):
        self.socket_listener = ListenSocket('', port, 1)
        self.socket_listener.bind_and_listen()
        self.keep_running = True
    
    def run(self):
        client_socket = self.socket_listener.accept()

        while self.keep_running:
            type = client_socket.recv(1)
        
            lenght = client_socket.recv(4)

            msg = client_socket.recv(int.from_bytes(lenght, "little", signed=False))
            
            if type.decode("utf-8") == END:
                self.keep_running = False
                continue
            print(msg.decode("utf-8"))
        
        client_socket.close()
        self.socket_listener.close()