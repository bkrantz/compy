from __future__ import absolute_import

from compy.actor import Actor
from compy.event import XMLEvent, JSONEvent, Event

class DictToXML(Actor):

    input = JSONEvent
    output = XMLEvent

    #TODO: Reimplement escape_xml flag functionality

    """
    **Actor implementation of the xmltodict lib (unparse). Converts an incoming dictionary to XML**

    Parameters:
        name (str)
            | Actor Name

        escape_xml (Optional[bool]) (Default: False)
            | If set to True, a dict key or nested dict key that contains an XML-style string element will be
            | XML escaped. If False, that XML will be retained in the XML element node created from the dictionary key
            | as a literal XML tree

        mode (str<"data"|"properties"> (Default: "data")
            | If set to "data" it will convert the event data from dict to XML.
            | If set to "properties" it will take the internal event dict MINUS data,
            | and override data with the generated XML

    Input:
        JSONEvent

    Output:
        XMLEvent

    Note that the conversion from json to XML is not a 1 to 1 conversion. Several behaviors available in XML are not supported
    in JSON. One in particular is accounted for in the conversion steps. Please see the example.

    Examples:
        Input:
            {"foo": [{"bar": "barvalue1"}, {"bar": "barvalue2"}}]
        Output:
            <jsonified_envelope>
                <foo>
                    <bar>barvalue1</bar>
                </foo>
                <foo>
                    <bar>barvalue2</bar>
                </foo>
            </jsonified_envelope>
        Explanation:
            The output that dicttoxml outputs in the above scenario is an incomplete XML document - namely the two <foo>
            elements. This is obviously not a valid XML scheme, but it DOES accurately reflect the JSON scheme input to it.
            However, in order to process the data as a complete XML doc, we have to add an envelope around it.

    """

    def __init__(self, name, escape_xml=False, key=None, *args, **kwargs):
        super(DictToXML, self).__init__(name, *args, **kwargs)
        self.key = key or name

    def consume(self, event, *args, **kwargs):
        try:
            event = self.convert(event)
            self.logger.info("Successfully converted Dict to XML", event=event)
            self.send_event(event)
        except Exception as err:
            self.logger.error("Unable to convert XML: {0}".format(err), event=event)
            raise

    def convert(self, event):
        if len(event.data) > 1:
            event.data = {self.key: event.data}

        return event.convert(XMLEvent)


class PropertiesToXML(DictToXML):
    """
    **Subclass of DictToXml. Converts other event properties to XML, rather than incoming data**
    """

    input = Event

    def __init__(self, *args, **kwargs):
        super(PropertiesToXML, self).__init__(*args, **kwargs)

    def convert(self, event):
        properties_dict = {self.key: event.get_properties()}
        event.data = properties_dict
        if not isinstance(event, XMLEvent):
            event = event.convert(XMLEvent)


        return event