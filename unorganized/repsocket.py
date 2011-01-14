# repsocket.py
#
# A durable reply socket.  

import socket
import msgsocket
import threading
import queue
import msgauth
import pickle

# Internal object used to store reply data
class ReplyData:
    def __init__(self):
        self.msg = None
        self.evt = threading.Event()

class ReplySocket:
    def __init__(self):
        self._messages = queue.Queue()        # Received messages
        self._pending_reply = None            # Reply is pending

    # Bind the socket to a given address and start an acceptor thread
    def bind(self,address,authkey=b"default"):
        thr = threading.Thread(target=self._acceptor_thread,args=(address,authkey))
        thr.daemon = True
        thr.start()

    # Internal thread that accepts client connections and launches handler threads
    def _acceptor_thread(self,address,authkey):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self._sock.bind(address)
        self._sock.listen(5)
        print("Listening on ", address)
        while True:
            client_sock, addr = self._sock.accept()
            thr = threading.Thread(target=self._client_handler_thread,
                                   args=(client_sock,addr,authkey))
            thr.daemon = True
            thr.start()

    # Client handler thread. Receives messages and sends responses
    def _client_handler_thread(self,client_sock,addr,authkey):
        print("Got connection from", addr)
        # Authentication.  If bad, immediately drop the connection and return
        if not msgauth.send_challenge(client_sock,authkey):
            print("Bad authentication")
            client_sock.close()
            return

        msgsock = msgsocket.MessageSocket(client_sock)
        reply = ReplyData()
        while True:
            try:
                # Receive an incoming message and queue it
                msg = msgsock.recv()
                reply.msg = None
                reply.evt.clear()
                self._messages.put((msg,reply))

                # Wait for the reply to be set and send it back
                reply.evt.wait()
                msgsock.send(reply.msg)
            except Exception as e:
                print("Closed connection from %s: %s" % (addr, e))
                break
        client_sock.close()

    # Receive a message from any of the connected clients (via queue)
    def recv_bytes(self):
        # If a reply was already pending, it's an error to call recv() again
        if self._pending_reply:
            raise RuntimeError("Must call send() after recv()")

        # Get the message and set the pending reply value
        msg,self._pending_reply = self._messages.get()
        return msg

    # Send a message back to the connected clients
    def send_bytes(self,msg):
        # If no reply is pending, it's an error to call send()
        if not self._pending_reply:
            raise RuntimeError("Must call recv() first")

        # Set the reply message and signal the handler thread that it's ready
        self._pending_reply.msg = msg
        self._pending_reply.evt.set()
        self._pending_reply = None

    # Pickle support
    def send(self,obj):
        self.send_bytes(pickle.dumps(obj))

    def recv(self):
        return pickle.loads(self.recv_bytes())

# Test code
if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        print("Usage: %s port" % sys.argv[0])
        raise SystemExit(1)

    port = int(sys.argv[1])
    def test_server(port):
        print("Echo server running on port",port)
        s = ReplySocket()
        s.bind(("",port),authkey=b"peekaboo")
        while True:
            msg = s.recv()
            print("Got message: ", msg)
            s.send(('response',msg))

    thr = threading.Thread(target=test_server,args=(port,))
    thr.daemon = True
    thr.start()

    # This is so Ctrl-C works
    import time
    while True:
        time.sleep(1)

        
