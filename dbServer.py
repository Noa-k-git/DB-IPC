from re import match
import select
import socket

IP='127.0.0.1'
PORT=8989

# Creating a server
server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
server_socket.bind((IP, PORT))
print('server is up at : ',IP,PORT)

server_socket.listen(5)
open_client_sockets = []
messages_to_send = []

reading = {} # socket:key value
writing = {} # socket:key value
appending = {} # socket:key value


def send_waiting_messages(wlist):
    """A function for sending messages
    Args:
        wlist (list): sockets to write to.
    """
    for message in messages_to_send: 
        current_socket, data = message 
        if current_socket in wlist:
            try: 
                current_socket.send(data)
                print("Sending data {", data, "}")
            except:
                print("Failed to send data {", data, '}')
        messages_to_send.remove(message)

def available(line_num, tasks): # starts with 0
    for num in tasks:
        if tasks[num] == line_num:
            return False
    return True

while True:
    rlist, wlist, xlist = select.select( [server_socket] + open_client_sockets, open_client_sockets, [])
      
    for current_socket in rlist:
        if current_socket is server_socket:
            (new_socket, address) = server_socket.accept()
            print("new socket connected to server: ", new_socket.getpeername())
            open_client_sockets.append(new_socket)
        else:
            try:
                data = current_socket.recv(1024).decode()
                print ('New data from client! {', data, '}')
                
                # informing all client to end connection because the number is already found.
                if data == 'end':
                    p_id = current_socket.getpeername()
                    for send_socket in wlist:
                        messages_to_send.append((send_socket, b'end'))
                        send_waiting_messages(wlist)
                        send_socket.close()
                        open_client_sockets.remove(send_socket)
                    print (f"Connection with client {p_id} closed.")
                    print('---'*6 + '\nNumber found:\n' + data[5:] + '\n' + '---'*6)

                else:
                    # sending new tasks for clients
                    if data[:4] == "READ":
                        available(int(data[5:]), writing + appending)
                    elif data[:5] == "REPLACE":
                        if available(int(data[6:]), reading+writing+appending):
                            pass
                    elif data[:6] == "APPEND":
                        new_task = last_line
                        last_line += 1
                    messages_to_send.append((current_socket, "OK:" + str(new_task)))
                    
            except ConnectionResetError: # handling a client randomly closed
                print("Socket forcibly closed! ConnectionResetError")

                del tasks[current_socket]
                open_client_sockets.remove(current_socket)
    send_waiting_messages(wlist)