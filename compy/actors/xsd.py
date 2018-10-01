#!/usr/bin/env python

from lxml import etree

from compy.actor import Actor
from compy.event import XMLEvent, JSONEvent
from compy.errors import MalformedEventData

__all__ = [
    "XSD",
    "XMLXSD",
    "JSONXSD"
]

class _XSD(Actor):
    '''**A simple actor which applies a provided XSD to an incoming event XML data. If no XSD is defined, it will validate XML format correctness**

    Parameters:

        name (str):
            | The instance name.
        xsd (str):
            | The XSD to validate the schema against

    Input:
        XMLEvent

    Output:
        XMLEvent

    '''

    input = XMLEvent

    def __init__(self, name, xsd=None, *args, **kwargs):
        super(_XSD, self).__init__(name, *args, **kwargs)
        if xsd:
            self.schema = etree.XMLSchema(etree.XML(xsd))
        else:
            self.schema = None

    def consume(self, event, *args, **kwargs):
        try:

            if self.schema:
                self.schema.assertValid(event.data)
            self.logger.info("Incoming XML successfully validated", event=event)
            self.send_event(event)
        except etree.DocumentInvalid as xml_errors:
            messages = [message.message for message in xml_errors.error_log.filter_levels([1, 2])]
            self.process_error(messages, event)
        except Exception as error:
            self.process_error(error, event)

    def process_error(self, message, event):
        self.logger.error("Error validating incoming XML: {0}".format(message), event=event)
        raise MalformedEventData(message)

class XSD(_XSD):
    output = XMLEvent

class XMLXSD(XSD):
    pass

class JSONXSD(_XSD):
    output = JSONEvent