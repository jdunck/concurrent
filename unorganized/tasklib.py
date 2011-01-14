# tasklib.py

import threading
import logging
import sys
import queue

tasktable = {}
_tasktable_lock = threading.Lock()

# Maximum pending messages
MAXMESSAGES = 256

class TaskError(Exception): pass
class TaskExit(TaskError): pass
class TaskReceiveError(TaskError): pass
class TaskSendError(TaskError): pass

class Task(object):
    _last_taskid = 0
    _last_taskid_lock = threading.Lock()

    def __init__(self,name="Task"):
        self.name = name
        self.state = "INIT"

    # Task start
    def start(self,wait=True):
        if not hasattr(self,"_messages"):
            self._messages = queue.Queue(MAXMESSAGES)
            self._messages_received = 0
        self._start_evt = threading.Event()
        self._exit_evt = threading.Event()
        # Assign a task id (if not already assigned)
        if not hasattr(self,"taskid"):
            with Task._last_taskid_lock:
                self.taskid = Task._last_taskid + 1
                Task._last_taskid = self.taskid
        self._thr = threading.Thread(target=self.bootstrap)
        self._thr.name = "%s-%s" % (self.name, self.taskid)
        self._thr.daemon = True
        self._thr.start()
        with _tasktable_lock:
            tasktable[self.taskid] = self
        # Wait for the task to launch
        if wait:
            self._start_evt.wait()

    # Task bootstrap
    def bootstrap(self):
        try:
            self.must_stop = False
            self.log = logging.getLogger(self.name)
            self.exc_info = None

            self.state = "RUNNING"
            self.log.info("Task starting")
            self._start_evt.set()
            try:
                self.run()
            except TaskExit:
                pass
            except Exception:
                self.exc_info = sys.exc_info()
                self.log.error("Crashed", exc_info=True)
            self.log.info("Exit")
            self.state = "EXIT"
            self._exit_evt.set()
        # This clause is a safety net that signals the start event
        # no matter what (even if bootstrapping crashes).  This
        # prevents the thread that launched the task from deadlock
        finally:
            self._start_evt.set()

    # Task stop
    def stop(self):
        self.must_stop = True
        self._messages.put(TaskExit)

    # Task join
    def join(self):
        self._exit_evt.wait()

    # Task finalization
    def finalize(self):
        with _tasktable_lock:
            del tasktable[self.taskid]
        del self._thr
        del self._start_evt
        del self._exit_evt
        del self._messages
        del self._messages_received
        del self.log
        del self.exc_info
        del self.must_stop
        del self.taskid
        self.state = "FINAL"

    # Task messaging
    def send(self,msg,block=True):
        if not hasattr(self,"_messages"):
            raise TaskSendError("No message queue")
        try:
            self._messages.put(msg,block)
            return True
        except queue.Full:
            return False

    def recv(self,block=True,timeout=None):
        try:
            msg = self._messages.get(block,timeout)
        except queue.Empty:
            raise TaskReceiveError()
        if msg is TaskExit:
            raise TaskExit()
        self._messages_received += 1
        return msg

    # Debugging support
    def pm(self):
        import pdb
        if self.exc_info:
            pdb.post_mortem(self.exc_info[2])
        else:
            print("No traceback")

# ----------------------------------------------------------------------
# Task monitor and debugger
# ----------------------------------------------------------------------

def top():
    with _tasktable_lock:
        taskids = sorted(tasktable)
        print("%6s %-15s %6s %6s %s" % ("Task","State","Recv","Queue","Instance"))
        print(" ".join("-"*n for n in (6,15,6,6,40)))
        for tid in taskids:
            task = tasktable[tid]
            print("%6d %-15s %6d %6d %s" % 
                  (tid, 
                   (task.state+"(CRASH)") if getattr(task,"exc_info",None) else task.state,
                   getattr(task,"_messages_received",0),
                   task._messages.qsize() if hasattr(task,"_messages") else 0,
                   task))

def stop(tid):
    task = tasktable[tid]
    task.stop()
    return task

def pm(tid):
    tasktable[tid].pm()

    
    
