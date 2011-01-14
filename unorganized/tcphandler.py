# tcphandler.py
#
# Event-driven TCP server handler

import ioevent
import socket

class TCPServerHandler(ioevent.IOHandler):
    def __init__(self,address,handler,dispatcher):
        self._dispatcher = dispatcher
        self._handler = handler
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self._sock.bind(address)
        self._sock.listen(5)
        self._sock.setblocking(False)
        self._dispatcher.register(self)

    def fileno(self):
        return self._sock.fileno()

    def readable(self):
        return True

    def handle_read(self):
        client,addr = self._sock.accept()
        print("Got connection",addr)
        client.setblocking(False)
        self._handler(client,addr,self._dispatcher)
