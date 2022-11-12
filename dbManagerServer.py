from server import Server
from db import DataBase
import select
import threading
import socket
import time
import os

IP='127.0.0.1'
PORT=8989

class dbManagerServer(Server):
    def __init__(self,  ip=IP, port=PORT):
        Server.__init__(self, ip, port)
        self.db = DataBase()
        self.queue = [] # dictionary of tuple(socket, request)
        self.current_reading = [] # tuple(socket:key)
        self.current_writing = [False, None] # socket,key,value
        self.all_locked = [None, False]
        
    def handle_client(self, current_socket:socket.socket, data:str):
        request = data.split(':', 2)
        if len(request) == 2:
            request = request[0], request[1].split(',', 2)
            self.queue.append((current_socket, request))
        if len(self.queue) == 1:
            self.manage_queue(current_socket)
    
    def manage_queue(self, current_socket:socket.socket):
        while not len(self.queue) == 0:
            cmd = self.queue[0][1][0]
            sock = self.queue[0][0]
            args = self.queue[0][1][1]
            if not self.all_locked[1]:
                if cmd.lower() == 'read':
                    if self.current_writing[1] == None or not cmd == self.current_writing[1][1][1]:
                        self.current_reading.append((sock, args))
                        thread = threading.Thread(target=self.read, args=(sock, args[0]))
                        thread.start()
                        #self.messages_to_send.append((self.current_reading[-1][0], 'OK'))
                        del self.queue[0]
                
                elif cmd.lower() == 'update':
                    if self.current_writing[1] == None:
                        key_readers = [k for k in self.current_reading if k[1] == args]
                        self.current_writing = [False, (sock, args)]
                        del self.queue[0]
                        if len(key_readers) == 0:
                            self.current_writing[0] = True
                            thread = threading.Thread(target=self.write)
                            thread.start()
                            
                            #self.messages_to_send.append((self.current_writing[1][0], 'OK'))
                
                elif cmd.lower() == 'admin_lock_0000':
                    self.all_locked[1] = True
                    
            elif cmd.lower() == 'release_r':
                self.release_reader(current_socket)
                        
            elif cmd.lower() == 'release_w':
                self.release_writer()
            elif cmd == 'admin_unlock_1111':
                self.all_locked[1] = False
            
            else:
                if len(self.current_reading) == 0 and self.current_writing[1] == None and self.all_locked:
                    self.messages_to_send(self.all_locked[0], 'ok')
                else:
                    time.sleep(0.05)
                    
            if os.path.getsize(self.db.changes_path) > 7000:
                client_thread = threading.Thread(target=self.union_client())
                client_thread.start()
        
    def union_client(self):
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        my_socket.connect((self.IP, self.PORT))
        my_socket.send(b'admin_lock_0000')
        data = my_socket.recv(1024).decode()
        if data == 'ok':
            self.db.union()
        my_socket.send(b'admin_unlock_1111')
        
        
    def release_reader(self, current_socket):
        for reader in self.current_reading:
            if reader[0] == current_socket:
                if self.current_writing[1] != None and reader[1] == self.current_writing[1][1][0]:
                    self.current_writing = True
                    self.messages_to_send.append((self.current_writing[1][0], 'OK'))
                self.current_reading.remove(reader)

    def release_writer(self):
        self.current_writing = [False, None]
                        
    def connection_error(self, current_socket):
        for request in self.queue:
            if request[0][0] == current_socket:
                self.queue.remove(request)
        self.release_reader(current_socket)
        if self.current_writing[1] != None and self.current_writing[1][0] == current_socket:
            self.current_writing = [False, None]
        if self.all_locked[0] == current_socket:
            self.all_locked = [None, False]
            
    def read(self, sock:socket.socket, key:str):
        try:
            value = self.db.read(key)
            self.messages_to_send.append((sock, str(value).encode()))
        finally:
            self.release_reader(sock)
        
    def write(self):
        try:
            socket = self.current_writing[1][0]
            key = self.current_writing[1][1][0]
            value = self.current_writing[1][1][1]
            
            self.db.append(key, value)
            self.messages_to_send.append((socket, b'OK'))
        finally:
            self.release_writer()

server = dbManagerServer()
server.activate()