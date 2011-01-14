# msgsocket.py
#
# A messaging-socket that sends discrete size-prefixed messages

import struct

# Utility function that receives a specified amount of data 
def recv_all(sock,size):
    buf = bytearray()
    while size > 0:
        chunk = sock.recv(size)
        if not chunk:
            raise IOError("Incomplete message")
        buf.extend(chunk)
        size -= len(chunk)
    return buf

class MessageSocket:
    def __init__(self,sock):
        self.sock = sock
    def send(self,msg):
        size = struct.pack("!I", len(msg))
        self.sock.sendall(size)
        self.sock.sendall(msg)
    def recv(self):
        size = recv_all(self.sock,4)
        msglen, = struct.unpack("!I",size)
        return recv_all(self.sock,msglen)
    def close(self):
        self.sock.close()

# Example server
if __name__ == '__main__':
    import socket
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv.bind(("",20000))
    serv.listen(1)
    print("Waiting for connection on port 20000")
    client_sock, addr = serv.accept()
    print("Got connection from", addr)
    client = MessageSocket(client_sock)

    # Echo messages back
    while True:
        msg = client.recv()
        client.send(msg)


            
            
            
            
    

        
