#!/usr/bin/env python

import sys
import datetime

from compy.actor import Actor

class STDOUT(Actor):

    '''**Prints incoming events to STDOUT.**

    Prints incoming events to STDOUT. When <complete> is True,
    the complete event including headers is printed to STDOUT.


    '''

    def __init__(self, name, complete=False, prefix="", timestamp=False, *args, **kwargs):
        super(STDOUT, self).__init__(name, *args, **kwargs)
        self.complete = complete
        self.prefix = prefix
        self.timestamp = timestamp

    def consume(self, event, *args, **kwargs):
        if self.complete:
            data = "{0}{1}".format(self.prefix, event)
        else:
            data = "{0}{1}".format(self.prefix, event.data_string())

        if self.timestamp:
            data = "[{0}] {1}".format(datetime.datetime.now(), data)

        print(data)
        sys.stdout.flush()
        self.send_event(event)
