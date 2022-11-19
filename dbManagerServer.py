from server import *
from db import DataBase
import select
import multiprocessing
import threading
import socket
import time
import os

IP='127.0.0.1'
PORT=8989

class dbManagerServer(Server):
    def __init__(self,  ip=IP, port=PORT):
        Server.__init__(self, ip, port)
        self.db = DataBase() # data base object
        
        # a list contains the tasks for the database
        # self.queue[x] = (socket, (request, (request_args)))
        self.queue = []
        
        # list of active file readers
        # self.current_reading[n] = (socket, (key,))
        self.current_reading = []
        
        # list of size 2:
        # self.current_writing[0]: bool - wether someone reads the same key
        # self.current_writing[1]: tuple - socket, (key, value)
        self.current_writing = [False, None]
        
        # list of size 2:
        # self.all_locked[0]: bool - True if the db props are all locked else False
        # self.all_locked[1]: client's socket
        self.all_locked = [False, None]
        
        # if a merge thread has been opened
        self.merge_started = False
        
        # if no one is working on the db
        self.merge_ok_message = False
        
        # lock for manage queue function
        self.queue_lock = threading.Lock()
        # self.update_lock = threading.Lock()
        
        # merging database
        self.db.merge()
        
    def handle_client(self, current_socket:socket.socket, data:str):
        """All client's requests end up here. 
        The function append the request to the queue for db handling,
        and call the queue manager if it is the first request.
        
        Args:
            current_socket (socket.socket): the client
            data (str): client request
        """
        request = data.split(':', 2)
        if len(request) == 2: # checks for index error
            request = request[0], request[1].split(',', 2)
            self.queue.append((current_socket, request))

        if not self.queue_lock.locked():
            manage_queue_thread = threading.Thread(target=self.manage_queue)
            manage_queue_thread.start()

    def manage_queue(self):
        """This function manages the request's queue.
        """
        self.queue_lock.acquire() # making sure only one thread is using this function
        while not len(self.queue) == 0: # while there are requests in the queue
            
            # getting the data for the first element
            sock = self.queue[0][0]
            cmd = self.queue[0][1][0]
            args = self.queue[0][1][1]

            if not self.all_locked[0]: # if db props aren't locked
                if cmd.lower() == 'read': # read from the db
                    if (self.current_writing[1] == None or not args[0] == self.current_writing[1][1][0]) and len(self.current_reading) < 10:
                        logging.debug(f'current reading len {len(self.current_reading)}, +1')
                        self.current_reading.append((sock, args))
                        thread = threading.Thread(target=self.read, args=(sock, args[0]))
                        thread.start()
                        del self.queue[0]
                
                elif cmd.lower() == 'update': # update the db
                    # self.update_lock.acquire()
                    if self.current_writing[1] == None:
                        self.current_writing = [False, (sock, args)]
                        del self.queue[0]
                        if self.__key_available(key=args[0]):
                            self.current_writing[0] = True
                            thread = threading.Thread(target=self.write)
                            thread.start()
                    # self.update_lock.release()
                
                elif cmd.lower() == 'admin_lock_0000': # locks all the db properties
                    self.all_locked[1] = sock
                    self.all_locked[0] = True
                    self.merge_ok_message = False
                    del self.queue[0]

                    
            elif cmd.lower() == 'release_r': # releasing a reader
                self.__release_reader(sock)
                del self.queue[0]
                        
            elif cmd.lower() == 'release_w': # releasing the writer
                self.__release_writer()
                del self.queue[0]

            elif cmd == 'admin_unlock_1111': # unlock to every db property
                self.all_locked[0] = False
                del self.queue[0]

            else:
                while not self.merge_ok_message : # if the merge still waiting for the writer or readers to finish
                    if len(self.current_reading) == 0 and self.current_writing[1] == None:
                        self.messages_to_send.append((self.all_locked[1], b'ok'))
                        self.merge_ok_message = True
                    else:
                        time.sleep(0.05)
            
            # checks if the changes file larger than 7kb
            if os.path.getsize(self.db.changes_path) > 7000 and not self.merge_started:
                self.merge_started = True
                client_thread = threading.Thread(target=self.merge_client)
                client_thread.start()

        self.queue_lock.release()
        
    def merge_client(self):
        """Merge the two data bases
        """
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        my_socket.connect((self.IP, self.PORT))
        my_socket.send(b'admin_lock_0000:')
        data = my_socket.recv(1024).decode()
        logging.debug("admin_socket.data:", data)
        if data.lower() == 'ok':
            self.db.merge()
        my_socket.send(b'admin_unlock_1111:')
        self.merge_started = False
        
    def __key_available(self, key):
        """Receives a key and returns True if no one reads the key

        Args:
            key (str): key

        Returns:
            bool: True if someone reads key value, False otherwise
        """
        key_readers = []
        for k in self.current_reading:
            logging.critical(f'key: {key}, {k[1][0]}')
            if k[1][0] == key:
                key_readers.append(k)
        logging.critical(key_readers)
        return key_readers == []
        
    def __release_reader(self, current_socket, key = None):
        """Releasing the reading request of the current socket

        Args:
            current_socket (socket.socket): client's socket
        """
        if key == 'j':
            pass
        all_readers = self.current_reading.copy()
        if key == None:
            self.current_reading = [reader for reader in all_readers if reader[0]!=current_socket]
        else:
            self.current_reading = [reader for reader in all_readers if reader[0]!=current_socket and key!=reader[1]]
        
        if self.current_writing[1] != None and self.__key_available(self.current_writing[1][1][0]) and not self.current_writing[0]:
            self.current_writing[0] = True
            #self.messages_to_send.append((self.current_writing[1][0], 'ok'))
            thread = threading.Thread(target=self.write)
            thread.start()

    def __release_writer(self):
        """Releasing the writer
        """
        self.current_writing = [False, None]
                        
    def connection_closed(self, current_socket:socket.socket):
        """Remove the socket's task

        Args:
            current_socket (socket.socket): failing socket
        """
        for request in self.queue:
            if request[0] == current_socket and len(self.queue) > 1:
                self.queue.remove(request)
        self.__release_reader(current_socket)
        # if self.current_writing[1] != None and self.current_writing[1][0] == current_socket:
        #     self.current_writing = [False, None]
        if self.all_locked[1] == current_socket:
            self.all_locked = [False, None]
            self.merge_ok_message = False
            
    def read(self, sock:socket.socket, key:str):
        """Reading a key from the database, then releasing the reader

        Args:
            sock (socket.socket): the client's socket
            key (str): key in the db
        """
        try:
            value = self.db.read(key)
            self.messages_to_send.append((sock, str(value).encode()))
        finally:
            self.__release_reader(sock, key)
        
    def write(self):
        """Writing to the db and then releasing the writing property
        """
        try:
            socket = self.current_writing[1][0]
            key = self.current_writing[1][1][0]
            value = self.current_writing[1][1][1]
            
            self.db.append(key, value)
            self.messages_to_send.append((socket, f'updated {key}, {value}'.encode()))
        finally:
            self.__release_writer()

if __name__=='__main__':
    server = dbManagerServer()
    server.activate()
