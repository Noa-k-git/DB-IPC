import socket
import threading
import time

IP='127.0.0.1'
PORT=8989

def create_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((IP, PORT))
    return sock

def read(key:str):
    sock = create_socket()
    print(time.time())
    sock.send(b'read:'+key.encode())
    data = sock.recv(1024)
    print('read ended time:', time.time())
    print(data.decode())
    sock.send(b'end')

def write(key:str, value:str):
    sock = create_socket()
    sock.send(b'update:'+key.encode()+b','+value.encode())
    print(f'sent: {key}, {value} --- time: {time.time()}')
    sock.recv(1024)
    print(f'updated: {key}, {value} --- time: {time.time()}')
    sock.send(b'end')

# create some dummy data in db
print('updating dummy data...')
some_data = [threading.Thread(target=write, args=('a', 'b')),
threading.Thread(target=write, args=('z', 'x')),
threading.Thread(target=write, args=('y', 'u')),
threading.Thread(target=write, args=('p', 'o')),
threading.Thread(target=write, args=('l', 'q'))]

for i in some_data:
    i.start()
for i in some_data:
    i.join()

exit()

# reading values while writing to them
threading.Thread(target=write, args=('j', 'g')).start()
threading.Thread(target=read, args=('j',)).start()
threading.Thread(target=write, args=('j', 'k')).start()
threading.Thread(target=read, args=('j',)).start()

# reading with more than 10 clients
# expected output 10 prints of the same time and 10 prints of one second later
for i in range(20):
    threading.Thread(target=read, args='j').start()

# #connect to server
# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.connect((IP, PORT))
# # adding dommy data
# for i in range(1, 10001, 2):
#     print(b'update:' + str(i).encode() + b',' + str(i+1).encode())
#     sock.send(b'update:' + str(i).encode() + b',' + str(i+1).encode())
#     time.sleep(0.05)
# sock.send(b'end')

# # trying to write and read the same value while writing
# sock_w = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock_w.connect((IP, PORT))
# sock_r = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock_r.connect((IP, PORT))

# sock_w.send(b'update:a,b')
# sock_r.send(b'read:a')
# data = sock_r.recv(1024).decode()
# print(data)
# sock_w.send(b'update:a,c') # 2 sec
# sock_r.send(b'read:a') # 0 sec
# data = sock_r.recv(1024).decode()
# print(data)
# time.sleep(0.02)
# sock_r.send(b'end')
# sock_w.send(b'end')

# del sock_r, sock_w

# trying to read with 20 threads at the same time
# reading takes 4 sec
# for i in range(20):
#     read_thread = threading.Thread(target=read, args=('a',))