# pubsub.py

import tasklib

class Gateway(tasklib.Task):
    def __init__(self,name="gateway"):
        super(Gateway,self).__init__(name=name)
        self._channels = {}
    def subscribe(self,task,channel):
        self.log.info("Subscribing %s to %s", task, channel)
        self._channels.setdefault(channel,set()).add(task)
    def unsubscribe(self,task,channel):
        self.log.info("Unsubscribing %s from %s", task, channel)
        self._channels[channel].remove(task)
    def publish(self,msg,channel):
        self.log.debug("Publishing %s on %s", msg, channel)
        if channel in self._channels:
            self.send((channel,msg))
    def run(self):
        while True:
            ch, msg = self.recv()
            subscribers = self._channels.get(ch)
            if subscribers:
                must_unsubscribe = []
                for task in subscribers:
                    try:
                        task.send(msg,block=False)
                    except tasklib.TaskSendError:
                        must_unsubscribe.append(task)
                    except Exception:
                        self.log.error("Crash in subscriber send()", exc_info=True)
                        must_unsubscribe.append(task)

                # Unsubscribe all of the dead subscribers (if any)
                for task in must_unsubscribe:
                    self.unsubscribe(task,ch)

# Get a running gateway task with a given name
_gateways = {}
def get_gateway(name):
    if name not in _gateways:
        gateway = Gateway(name=name)
        gateway.start()
        _gateways[name] = gateway
    return _gateways[name]

# Global function for publishing on a gateway (if it exists)
def publish(msg,channel,gatewayname):
    gateway = _gateways.get(gatewayname)
    if gateway:
        gateway.publish(msg,channel)

# Global function for subscribing to a gateway. Creates it if doesn't exist
def subscribe(task,channel,gatewayname):
    gateway = get_gateway(gatewayname)
    gateway.subscribe(task,channel)

# Global function for unsubscribing to a gateway
def unsubscribe(task,channel,gatewayname):
    gateway = _gateways.get(gatewayname)
    if gateway:
        gateway.unsubscribe(task,channel)




    
