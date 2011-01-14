# arepsocket.py
#
# An asynchronous reply socket.  

import socket
import msghandler
import ioevent
import threading
import queue
import tcphandler

class ConnectionHandler(msghandler.MessageHandler):
    def __init__(self,repsock,sock,addr,dispatcher):
        super().__init__(sock,addr,dispatcher)
        self._repsock = repsock
        self._send_lock = threading.Lock()

    # Deposit the received message on the socket message queue
    # Note: this saves the message and a reference to myself
    def handle_recv(self,msg):
        if msg is not None:
            self._repsock._messages.put((msg,self))

    # Due to threading, write/send operations need to be synchronized
    # with a lock.   There are probably cleaner ways to do this, but
    # the code below is a first-attempt
    def handle_write(self):
        with self._send_lock:
            super().handle_write()

    def send(self,msg):
        with self._send_lock:
            super().send(msg)
            # Try an "optimistic send" (it might not work hence the try-except)
            # We do this to try and short-cut the send operation so that it
            # happens immediately--even if the event loop hasn't gotten around to it yet
            try:
                super().handle_write()
            except socket.error:
                pass
    
class ReplySocket:
    def __init__(self):
        self._messages = queue.Queue()        # Received messages
        self._address = None                  # Socket address
        self._pending_reply = None            # Reply is pending

    def bind(self,address):
        self.address = address
        thr = threading.Thread(target=self._dispatcher_thread)
        thr.daemon = True
        thr.start()

    # Internal thread that runs the async-dispatcher for client messages
    def _dispatcher_thread(self):
        dispatcher = ioevent.EventDispatcher()
        tcphandler.TCPServerHandler(self.address,
                                    lambda *args: ConnectionHandler(self,*args),
                                    dispatcher)
        dispatcher.run(timeout=0.01)

    # Receive a message from any of the connected clients (via queue)
    def recv(self):
        # If a reply was already pending, it's an error to call recv() again
        if self._pending_reply:
            raise RuntimeError("Must call send() after recv()")

        # Get the message and set the pending reply value
        msg,self._pending_reply = self._messages.get()
        return msg

    # Send a message back to the connected clients
    def send(self,msg):
        # If no reply is pending, it's an error to call send()
        if not self._pending_reply:
            raise RuntimeError("Must call recv() first")

        # Send the reply message back to the async handler
        self._pending_reply.send(msg)
        self._pending_reply = None

# Test code
if __name__ == '__main__':
    import sys

    def test_server(port):
        print("Echo server running on port",port)
        s = ReplySocket()
        s.bind(("",port))
        while True:
            msg = s.recv()
            print("Got message: ", msg)
            s.send(b"Reply:"+msg)

    if len(sys.argv) != 2:
        print("Usage: %s port" % sys.argv[0])
        raise SystemExit(1)

    port = int(sys.argv[1])
    thr = threading.Thread(target=test_server,args=(port,))
    thr.daemon = True
    thr.start()

    # This is so Ctrl-C works
    import time
    while True:
        time.sleep(1)

        
