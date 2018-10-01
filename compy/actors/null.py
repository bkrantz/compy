#!/usr/bin/env python

from compy.actor import Actor

class Null(Actor):

    '''**Purges incoming events.** '''

    def consume(self, event, *args, **kwargs):
        del event
