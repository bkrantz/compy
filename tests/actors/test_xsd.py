import unittest

from compy.actors.xsd import XSD
from compy.event import XMLEvent
from compy.errors import MalformedEventData
from compy.testutils.test_actor import TestActorWrapper

xsd = """
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:element name="foo">
      <xsd:complexType>
        <xsd:sequence>
          <xsd:element name="bar" type="xsd:string"/>
          <xsd:element name="bar2" type="xsd:string"/>
          <xsd:element name="bar3" type="xsd:string"/>
          <xsd:element name="bar4" type="xsd:string"/>
        </xsd:sequence>
      </xsd:complexType>
    </xsd:element>
</xsd:schema>
"""

valid_xml = """
<foo>
    <bar />
    <bar2 />
    <bar3 />
    <bar4 />
</foo>
"""

invalid_xml = """
<foo>
    <bar/>
</foo>
"""

class TestXSD(unittest.TestCase):

    def setUp(self):
        self.actor = TestActorWrapper(XSD("xsd", xsd=xsd))

    def test_valid_xml(self):
        _input = XMLEvent(data=valid_xml)
        self.actor.input = _input
        _output = self.actor.output
        self.assertEqual(_input.data_string(), _output.data_string())

    def test_invalid_xml(self):
        _input = XMLEvent(data=invalid_xml)
        self.actor.input = _input
        _output = self.actor.error
        self.assertTrue(isinstance(_output.error, MalformedEventData))