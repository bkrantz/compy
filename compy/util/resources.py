import os, re, json

__all__ = [
    "load_resources",
]

__xml_remove_regexes = ['<!DOCTYPE.*?>', '<!--.*?-->']
__extension_to_format = {}

def load_resources(folder_path, file_regex=".*", extension=None):
    extension = extension or folder
    files_dict = {}    
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if re.match(file_regex, file):
                try:
                    name, ext = file.split(os.extsep)
                except ValueError:
                    pass
                else:
                    if ext.lower() == extension:
                        cur_path = os.path.join(root, file)
                        try:
                            cur_obj = __extension_to_format[extension](cur_path)
                        except KeyError: 
                            cur_obj = __read_file(cur_path)
                        files_dict.update({name: cur_obj})
    return files_dict

try:
    from lxml import etree
    import configobj
    from io import StringIO

    def __read_file(file_path):
        content = None
        with open(file_path, 'r') as content_file:
            content = content_file.read()
        return content

    def __read_config(file_path):
        return configobj.ConfigObj(file_path)

    def __read_xml(file_path):
        raw_content = __read_file(file_path=file_path)
        for regex in __xml_remove_regexes:
            raw_content = re.sub(regex, '', raw_content)
        return etree.fromstring(raw_content)

    def __read_html(file_path):
        parser = etree.HTMLParser()
        raw_content = __read_file(file_path=file_path)
        for regex in __xml_remove_regexes:
            raw_content = re.sub(regex, '', raw_content)
        tree = etree.parse(StringIO(unicode(raw_content, "utf-8")), parser)
        return tree.getroot()

    def __read_xslt(file_path):
        xml_content = __read_xml(file_path=file_path)
        return etree.XSLT(xml_content)

    def __read_json(file_path):
        json_content = __read_file(file_path=file_path)
        return json.loads(json_content)

    __extension_to_format = {
        "conf": __read_config,
        "config": __read_config,
        "xml": __read_xml,
        "html": __read_html,
        "xsl": __read_file,
        "json": __read_json,
    }
except:
    pass