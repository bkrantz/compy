#!/usr/bin/env python

from uuid import uuid4 as uuid

from compy.actor import Actor
from compy.event import XMLEvent, JSONEvent

__all__ = [
    "FlowController",
    "ToDict",
    "ToXML"
]

class _Flow(Actor):
    def consume(self, event, *args, **kwargs):
        self.send_event(event)

class FlowController(Actor):
    '''
    Simple module that is designed to accept any input and replicate it to any outbox(s)
    For example, if one were to want to abstract multiple potential data flows to a single aggregator (EventMatcher) inbox, the outbox behind
    this FlowController module would serve as a mask so that the data aggregator isn't waiting for data flow channels that will never arrive

    In the future this could be designed to handle multiple functions, such as controlling rate of event flow through it

    '''

    def __init__(self, name, trigger_errors=False, generate_fresh_ids=False, *args, **kwargs):
        self.trigger_errors = trigger_errors
        self.generate_fresh_ids = generate_fresh_ids
        super(FlowController, self).__init__(name, *args, **kwargs)

    def consume(self, event, *args, **kwargs):
        if self.generate_fresh_ids:
            event._event_id = uuid().get_hex()
            event.meta_id = event._event_id
        if event.error and self.trigger_errors:
            self.send_error(event)
        else:
            self.send_event(event)

class ToDict(_Flow):
    output = JSONEvent

class ToXML(_Flow):
    output = XMLEvent