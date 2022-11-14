from abc import ABC, abstractmethod
import select
import socket

class Server(ABC):
    def __init__(self, ip, port):
        self.IP = ip
        self.PORT = port

        # Creating a server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.server_socket.bind((self.IP, self.PORT))
        print('server is up at : ',self.IP,self.PORT)

        self.server_socket.listen(5)
        self.open_client_sockets = []
        self.messages_to_send = []


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
                    print("Sending data {", data, "}")
                except:
                    print("Failed to send data {", data, '}')
            self.messages_to_send.remove(message)
    
    def activate(self):
        while True:
            rlist, wlist, xlist = select.select( [self.server_socket] + self.open_client_sockets, self.open_client_sockets, [])
            
            for current_socket in rlist:
                if current_socket is self.server_socket:
                    (new_socket, address) = self.server_socket.accept()
                    print("new socket connected to server: ", new_socket.getpeername())
                    self.open_client_sockets.append(new_socket)
                else:
                    try:
                        data = current_socket.recv(1024).decode()
                        print ('New data from client! {'+ data+ '}')
                        
                        # informing all client to end connection because the number is already found.
                        if data == 'end':
                            p_id = current_socket.getpeername()
                            for send_socket in wlist:
                                self.messages_to_send.append((send_socket, b'end'))
                                self.send_waiting_messages(wlist)
                                self.open_client_sockets.remove(send_socket)
                                send_socket.close()
                            print (f"Connection with client {p_id} closed.")

                        else:
                            self.handle_client(current_socket, data)
                            
                    except ConnectionResetError: # handling a client randomly closed
                        print("Socket forcibly closed! ConnectionResetError")
                        self.connection_error(current_socket)
                        self.open_client_sockets.remove(current_socket)
            self.send_waiting_messages(wlist)
    
    @abstractmethod
    def handle_client(self, current_socket, data):
        pass
    
    @abstractmethod      
    def connection_error(self):
        pass