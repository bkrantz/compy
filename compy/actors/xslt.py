#!/usr/bin/env python

import traceback

from lxml import etree
from lxml.etree import XSLTApplyError #TODO Don't need this and generic etree import

from compy.actor import Actor
from compy.event import XMLEvent, JSONEvent
from compy.errors import MalformedEventData

__all__ = [
    "XSLT",
    "JSONXSLT",
    "XMLXSLT"
]

class _XSLT(Actor):
    '''**A sample module which applies a provided XSLT to an incoming event XML data**

    Parameters:

        name (str):
            | The instance name.
        xslt (str):
            | The xslt to apply to incoming XMLEvent

    Input:
        XMLEvent

    Output:
        XMLEvent

    '''

    input = XMLEvent

    def __init__(self, name, xslt=None, *args, **kwargs):
        super(_XSLT, self).__init__(name, *args, **kwargs)

        if xslt is None and not isinstance(xslt, str):
            raise TypeError("Invalid xslt defined. {_type} is not a valid xslt. Expected 'str'".format(_type=type(xslt)))
        else:
            self.template = etree.XSLT(etree.XML(xslt))

    def consume(self, event, *args, **kwargs):
        try:
            self.logger.debug("In: {data}".format(data=event.data_string().replace('\n', '')), event=event)
            event.data = self.transform(event.data)
            self.logger.debug("Out: {data}".format(data=event.data_string().replace('\n', '')), event=event)
            self.logger.info("Successfully transformed XML", event=event)
            self.send_event(event)
        except XSLTApplyError as err:
            # This is a legacy functionality that was implemented due to the specifics of a single implementation.
            # I'm looking for a way around this, internally
            event.data.append(etree.fromstring("<transform_error>{0}</transform_error>".format(err.message)))
            self._process_error(err.message, event)
        except Exception as err:
            self._process_error(traceback.format_exc(), event)

    def _process_error(self, message, event):
        self.logger.error("Error applying transform. Error was: {0}".format(message), event=event)
        raise MalformedEventData("Malformed Request: Invalid XML")

    def transform(self, etree_element):
        return self.template(etree_element).getroot()

class XSLT(_XSLT):
    output = XMLEvent

class XMLXSLT(XSLT):
    pass

class JSONXSLT(_XSLT):
    output = JSONEvent