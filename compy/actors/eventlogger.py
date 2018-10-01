#!/usr/bin/env python

import logging

from compy import Actor

class EventLogger(Actor):

    '''**Sends incoming events to logger.**

    Simple module that logs the current event contents
        - level (logging module level)      (Default: logging.INFO) The level that his module will log the incoming event
        - logged_tags ([string])            (Default: ['data']) The event attributes to log. Ignored if log_full_event is True
        - log_full_event (bool)             (Default: True) Whether or not to log the full value of event.to_string() to the logger
        - prefix (str)                      (Default: "") The prefix to prepend to the logged event string

    '''

    def __init__(self, name, level=logging.INFO, logged_tags=['data'], log_full_event=True, prefix="", *args, **kwargs):
        super(EventLogger, self).__init__(name, *args, **kwargs)
        self.level = level
        self.prefix = prefix
        self.logged_tags = logged_tags
        self.log_full_event = log_full_event

    def consume(self, event, *args, **kwargs):
        message = self.prefix + ""
        if self.log_full_event:
            message += str(event)
        else:
            for tag in self.logged_tags:
                if tag == "data":
                    message += event.data_string()
                elif tag == "error":
                    message += event.error_string()
                else:
                    message += str(getattr(event, tag, None))

        self.logger.log(self.level, message, event=event)
        self.send_event(event)