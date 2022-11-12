from server import Server
import select
import socket

IP='127.0.0.1'
PORT=8989

class dbManagerServer(Server):
    def __init__(self,  ip='127.0.0.1', port=9999):
        Server.__init__(self, ip, port)

        self.queue = [] # dictionary of tuple(socket, request)
        self.current_reading = [] # tuple(socket:key)
        self.current_writing = [False, None] # socket:key
        self.all_locked = [None, False]
        
    def handle_client(self, current_socket:socket.socket, data:str):
        request = data.split(':', 2)
        if len(request) == 2:
            self.queue.append((current_socket, request))
        if len(self.queue) == 1:
            self.manage_queue(current_socket)
    
    def manage_queue(self, current_socket:socket.socket):
        while not len(self.queue) == 0:
            cmd = self.queue[0][1][0]
            if not self.all_locked[1]:
                if self.queue[0][1][0].lower() == 'read':
                    if not cmd == self.current_writing[1][1]:
                        self.current_reading.append((self.queue[0][0], self.queue[0][1][1]))
                        self.messages_to_send.append((self.current_reading[-1][0], 'OK'))
                        del self.queue[0]
                
                elif cmd.lower() == 'update':
                    if self.current_writing == None:
                        key_readers = [k for k in self.current_reading if k[1] == self.queue[0][1][1]]
                        self.current_writing = [False, (self.queue[0][0], self.queue[0][1][1])]
                        del self.queue[0]
                        if len(key_readers) == 0:
                            self.current_writing[0] = True
                            self.messages_to_send.append((self.current_writing[1][0], 'OK'))
                
                elif cmd.lower() == 'lock':
                    self.all_locked[1] = True
                    
            elif cmd.lower() == 'release_r':
                self.release_reader(current_socket)
                        
            elif cmd.lower() == 'release_w':
                self.current_writing = [False, None]
            elif cmd == 'unlock':
                self.all_locked[1] = False
            
            else:
                if len(self.current_reading) == 0 and self.current_writing[1] == None and self.all_locked:
                    self.messages_to_send(self.all_locked[0], 'OK')
        
    def release_reader(self, current_socket):
        for reader in self.current_reading:
            if reader[0] == current_socket:
                if reader[1] == self.current_writing[[1][1]]:
                    self.current_writing = True
                    self.messages_to_send.append((self.current_writing[1][0], 'OK'))
                self.current_reading.remove(reader)
                        
    def connection_error(self, current_socket):
        for request in self.queue:
            if request[0][0] == current_socket:
                self.queue.remove(request)
        self.release_reader(current_socket)
        if self.current_writing[1][0] == current_socket:
            self.current_writing = [False, None]
        if self.all_locked[0] == current_socket:
            self.all_locked = [None, False]