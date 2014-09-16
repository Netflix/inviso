import util
import json
from boto.sqs.message import Message

from aws import SQSBase

log = util.get_logger("inviso.publish")


class EventPublisher(object):
    def __init__(self, **kwargs):
        pass
    
    def publish(self, events):
        raise NotImplementedError("Please Implement this method")


class SQSEventPublisher(EventPublisher, SQSBase):
    def __init__(self, queues=[], **kwargs):
        super(EventPublisher, self).__init__(**kwargs)
        super(SQSBase, self).__init__(**kwargs)
        
        self.queues = {}
        
        for queue in queues:
            self.queues[queue] = self.conn.get_queue(queue)
            self.queues[queue].set_message_class(Message)
        
    def publish(self, events):
        for event in events:
            for name, queue in self.queues.iteritems():
                queue.write(Message(body=json.dumps(event)))


class DirectPublisher(EventPublisher):
    def __init__(self, handler, **kwargs):
        super(EventPublisher, self).__init__(**kwargs)

        self.handler = handler

    def publish(self, events):
            self.handler.handle(events)