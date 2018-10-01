import re

from lxml import etree

class XPathLookup(object):
    """
    Wrapper class that auto populates an xpath lookup with the default namespace, if defined by the provided xml.
    This is necessary because lxml does not take default namespaces into account with simple xpath lookups by default
    """

    def __init__(self, xml):
        self.xml = xml
        self.__initialize_namespaces(self.xml)

    def __initialize_namespaces(self, xml):
        self.namespaces = {}

        for key in xml.nsmap:
            if key is None:
                self.namespaces.update({"default": xml.nsmap[key]})
            else:
                self.namespaces.update({key: xml.nsmap[key]})

        for element in xml.iterchildren("*"):
            for key in element.nsmap:
                if key is None:
                    self.namespaces.update({"default": element.nsmap[key]})
                else:
                    self.namespaces.update({key: element.nsmap[key]})

    def lookup(self, xpath):
        """
        Function that auto populates the default namespace of any given xpath lookup
        The default behavior is IF a default namespace is found, for any given elements that do NOT have an explicitly declared
        namespace, insert "default:" in their xpath lookup for use by etree.xpath, and assign the key value 'default' to the default namespace
        of the root xml node

        Otherwise, simply take the explicitely defined namespaces and append them to the namespaces dict so that xpath lookups can use namespace prefixes
        instead of having to explicitely lookup in //{http://some_awful_namespace.com}some/element format.

        Example 1:
            - XML:                          <root xmlns="http://uglynamespaces.com">
                                                <child1>
                                                    <child2/>
                                                </child1>
                                            </root>
            - Input Xpath:                  //root/child1/child2
            - Resulting xml.xpath call:     xml.xpath('//default:root/default:child1/default:child2', namespaces={'default': 'http://uglynamespaces.com'})

        Example 2:
            - XML:                          <root xmlns="http://uglynamespaces.com", xmlns:somens="http://someothernamespace.com">
                                                <child1>
                                                    <somens:child2>
                                                        <child3 xmlns="http://blah.com">
                                                            <child5/>
                                                        </child3>
                                                    </somens:child2>
                                                </child1>
                                            </root>
            - Input Xpath:                  //root/child1/somens:child2
            - Resulting xml.xpath call:     xml.xpath('//default:root/default:child1/somens:child2', namespaces={'default': 'http://uglynamespaces.com'
                                                                                                                    'somens': 'http://someothernamespace.com'})

        LIMITATIONS:
            - Will NOT account for multiple default namespaces. Referencing Example 1 above, if the condition of <child1 xmlns="http://somestupidlocalns.com"> exists, this WILL break.
                It will not, at this time, recursively check for each child nodes default ns and map accordingly
        """

        if self.namespaces.get("default", None):
            xpath = re.sub(r'\/(?!\/|([\w{0, }]\:[\w{0, }]))', r'/default:', xpath)

        return self.xml.xpath(xpath, namespaces=self.namespaces)