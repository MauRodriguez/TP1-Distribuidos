import socket
import logging
from peer_socket import PeerSocket

class ListenSocket:
    def __init__(self, dir, port, listen_backlog):
        self._socket = None
        self._port = port
        self._dir = dir
        self._listen_backlog = listen_backlog
    
    def bind_and_listen(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.bind((self._dir, self._port))
            self._socket.listen(self._listen_backlog)
            logging.debug(f"action: bind_and_listen | result: success")
        except socket.timeout as e:
            logging.error(f"action: bind_and_listen | result: error | error: {repr(e)}")
        except socket.error as e:
            logging.error(f"action: bind_and_listen | result: error | error: {repr(e)}")
    
    def accept(self):
        skt, addr = self._socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')

        return PeerSocket(skt, addr)

    def close(self):
        try:
            self._socket.close()
        except OSError as e:
            logging.error(f"action: close | result: error | error: {repr(e)}")
