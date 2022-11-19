from abc import ABC, abstractmethod
import select
import socket
import logging, coloredlogs

class Server(ABC):
    def __init__(self, ip, port):
        logging.basicConfig(level=logging.DEBUG)
        coloredlogs.install(level='DEBUG')
    
        self.IP = ip
        self.PORT = port
        self.server_socket = None
        self.open_client_sockets = []
        self.messages_to_send = []
    
    def create_server(self):
        # Creating a server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.server_socket.bind((self.IP, self.PORT))
        logging.info(f'server is up at : {self.IP}, {self.PORT}')

        self.server_socket.listen(5)
        

    def send_waiting_messages(self, wlist):
        """A function for sending messages
        Args:
            wlist (list): sockets to write to.
        """
        for message in self.messages_to_send: 
            current_socket, data = message 
            if current_socket in wlist:
                try: 
                    current_socket.send(data)
                    logging.debug(f"sending data: {data} to {current_socket.getpeername()}")
                except:
                    logging.warning(f"Failed to send: {data} to {current_socket}")
            self.messages_to_send.remove(message)
    
    def activate(self):
        """Handling clients
        """
        self.create_server()
        while True:
            rlist, wlist, xlist = select.select( [self.server_socket] + self.open_client_sockets, self.open_client_sockets, [])
            
            for current_socket in rlist:
                if current_socket is self.server_socket:
                    (new_socket, address) = self.server_socket.accept()
                    logging.info(f"new socket connected to server: {new_socket.getpeername()}")
                    self.open_client_sockets.append(new_socket)
                else:
                    try:
                        data = current_socket.recv(1024).decode()
                        logging.debug(f'new data from client {current_socket.getpeername()}: {data}')
                        # informing all client to end connection because the number is already found.
                        if data == 'end':
                            p_id = current_socket.getpeername()
                            self.messages_to_send.append((current_socket, b'end'))
                            self.send_waiting_messages(wlist)
                            self.connection_closed(current_socket)
                            self.open_client_sockets.remove(current_socket)
                            current_socket.close()
                            logging.info(f"Connection with client {p_id} closed.")

                        else:
                            self.handle_client(current_socket, data)
                            data=''
                            
                    except ConnectionResetError: # handling a client randomly closed
                        logging.error("Socket forcibly closed! ConnectionResetError")
                        self.connection_closed(current_socket)
                        self.open_client_sockets.remove(current_socket)
            self.send_waiting_messages(wlist)
    
    @abstractmethod
    def handle_client(self, current_socket:socket.socket, data:str):
        """Handling the client's request.

        Args:
            current_socket (socket.socket): the client's socket
            data (str): data received from client
        """ 
        pass
    
    @abstractmethod      
    def connection_closed(self, current_socket:socket.socket):
        """ removing client from requests.
        """
        pass