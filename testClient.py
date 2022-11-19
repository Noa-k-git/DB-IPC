import socket
from threading import Thread
import time

IP='127.0.0.1'
PORT=8989

def create_socket():
    """Create socket with the server

    Returns:
        socket.socket: the socket with the server
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((IP, PORT))
    return sock

def read(key:str):
    """send to the server a read request

    Args:
        key (str): the wanted key
    """
    sock = create_socket()
    sock.send(b'read:'+key.encode())
    start = time.time()
    print(f'r\tsend read key {key}')
    data = sock.recv(1024)
    print(f'r\treceived read key {key}\tvalue {data.decode()}\t\tat time: {round(time.time() - start, 2)} s')
    sock.send(b'end')
    sock.recv(100)
    sock.close()

def write(key:str, value:str=None):
    """Sending to the server an update request

    Args:
        key (str): wanted key
        value (str, optional): the new value. Defaults to None.
                            * if None it deletes the key from the db.
    """
    sock = create_socket()
    sock.send(b'update:'+key.encode()+b','+str(value).encode())
    start = time.time()
    print(f'w\tsend: update {key}, {value}')
    sock.recv(1024)
    print(f'w\treceived: update {key}, {value} is complete\tat time: {round(time.time() - start, 2)} s')
    sock.send(b'end')
    sock.recv(100)
    sock.close()

def run_test(test):
    for i in test:
        i.start()
        time.sleep(0.005)
    for i in test:
        i.join()
# create some dummy data in db
print('updating dummy data...')
test = [Thread(target=write, args=('a', 'b')),
Thread(target=write, args=('z', 'x')),
Thread(target=write, args=('y', 'u')),
Thread(target=write, args=('p', 'o')),
Thread(target=write, args=('l', 'q'))]
run_test(test)

# reading values while writing to them
print('\nupdating j to g and then read it, updating j to k and then reading it.')
test = [Thread(target=write, args=('j', 'g')),
Thread(target=read, args=('j',)),
Thread(target=write, args=('j', 'k')),
Thread(target=read, args=('j',))]
run_test(test)
# reading with more than 10 clients
# expected output 10 prints of the same time and 10 prints of one second later
print('\nreading the same value (of j) with 20 clients (limit is 10)')
test = [Thread(target=read, args='j') for _ in range(20)]
run_test(test)

print('\ndelete element from db and reading none existing key')
run_test([Thread(target=write, args=('j',)), 
          Thread(target=read, args=('j',)),
          Thread(target=read, args=('X',))])


print('\nRead and write when its not the same value')
run_test((Thread(target=write, args=('j','l')), 
          Thread(target=read, args=('l',))))


print('\nFinale test: reading from multiple clients, writing from one and trying to read the same value')
test = [Thread(target=read, args=('z',)) for _ in range(8)] + \
    [Thread(target=write, args=('z',)), Thread(target=read, args=('z',))]
run_test(test)