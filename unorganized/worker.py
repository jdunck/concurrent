# worker.py

import sys
import tasklib
import threading

class UnavailableError(Exception): pass

class FutureResult(object):
    def __init__(self):
        self._evt = threading.Event()
        self._cancelled = False
        self._callback = None
        self._callback_lock = threading.Lock()

    def cancel(self):
        with self._callback_lock:
            self._cancelled = True
            self._evt.set()
            if self._callback:
                self._callback(UnavailableError("Cancelled"))

    def set_callback(self,cb):
        with self._callback_lock:
            self._callback = cb
            if self._evt.is_set():
                if hasattr(self,"_value"):
                    self._callback(self._value)
                elif hasattr(self,"_exc"):
                    self._callback(self._exc[1])
                elif self._cancelled:
                    self._callback(UnavailableError("Cancelled"))

    def set(self,value):
        with self._callback_lock:
            self._value = value
            self._evt.set()
            if self._callback:
                self._callback(value)

    def set_error(self):
        with self._callback_lock:
            self._exc = sys.exc_info()
            self._evt.set()
            if self._callback:
                self._callback(self._exc[1])

    def get(self):
        self._evt.wait()
        if hasattr(self,"_exc"):
            raise self._exc[1].with_traceback(self._exc[2])
        elif hasattr(self,"_value"):
            return self._value
        elif self._cancelled:
            raise UnavailableError("Cancelled")
        else:
            raise UnavailableError("No result")

class WorkerTask(tasklib.Task):
    def apply(self,func,args=(),kwargs={}):
        fresult = FutureResult()
        self.send((fresult,func,args,kwargs))
        return fresult
    def run(self):
        while True:
            fresult,func,args,kwargs = self.recv()
            if fresult._cancelled:
                continue
            try:
                fresult.set(func(*args,**kwargs))
            except:
                fresult.set_error()

class WorkerPool(tasklib.Task):
    def __init__(self,nworkers=1):
        super(WorkerPool,self).__init__(name="workerpool")
        self.nworkers = nworkers

    def apply(self,func,args=(),kwargs={}):
        fresult = FutureResult()
        self.send((fresult,func,args,kwargs))
        return fresult

    def run(self):
        self._running_workers = self.nworkers
        self._all_done = threading.Event()

        # Launch additional worker threads
        for n in range(1,self.nworkers):
            thr = threading.Thread(target=self.do_work)
            thr.daemon = True
            thr.start()
            
        self.do_work()
        # wait for all workers to terminate
        self._all_done.wait()
        

    # Worker method (runs in multiple threads)
    def do_work(self):
        try:
            while True:
                fresult,func,args,kwargs = self.recv()
                if fresult._cancelled:
                    continue
                try:
                    fresult.set(func(*args,**kwargs))
                except:
                    fresult.set_error()
        except tasklib.TaskExit:
            pass
        finally:
            self._running_workers -= 1
            if self._running_workers:
                # Request the next worker to stop
                self.stop()
            else:
                self._all_done.set()
            self.log.info("Worker thread stopped")

# For testing/debugging            
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    pool = WorkerPool(nworkers=8)
    pool.start()

    # Some sample code to try
    # import geocode
    # results = [pool.apply(geocode.streetname,(41.8007,-87.7297))
    #             for n in range(40)]

    # for r in results:
    #     print(r.get())

    
        
    

    
