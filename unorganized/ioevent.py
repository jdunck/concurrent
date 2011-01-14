# ioevent.py
from select import select

class IOHandler:
    # Method to return a file descriptor 
    def fileno(self):
        pass

    # Reading
    def readable(self):
        return False
    def handle_read(self):
        pass

    # Writing
    def writable(self):
        return False
    def handle_write(self):
        pass

class EventDispatcher:
    def __init__(self):
        self.handlers = set()
    def register(self,handler):
        self.handlers.add(handler)
        print("Registering", handler)
    def unregister(self,handler):
        self.handlers.remove(handler)
        print("Unregistering", handler)
    def run(self,timeout=None):
        while self.handlers:
            readers = [h for h in self.handlers
                         if h.readable()]
            writers = [h for h in self.handlers
                         if h.writable()]
            rset,wset,e = select(readers,writers,[],timeout)
            for r in rset:
                r.handle_read()
            for w in wset:
                w.handle_write()

