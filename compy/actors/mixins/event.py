import xmltodict
from lxml import etree
from decimal import Decimal
import collections
from compy.actors.util.xpath import XPathLookup
from compy.errors import InvalidEventConversion
import json
import re
from xml.parsers import expat

__all__ = [
    "_EventConversionMixin",
    "_XMLEventConversionMixin",
    "_JSONEventConversionMixin",
    "_EventFormatMixin",
    "_XMLEventFormatMixin",
    "_JSONEventFormatMixin",
    "BasicEventModifyMixin",
    "JSONEventModifyMixin",
    "XMLEventModifyMixin",
    "LookupMixin",
    "XPathLookupMixin"
]

class _ConversionMethods:
    _XML_TYPES = [etree._Element, etree._ElementTree, etree._XSLTResultTree]
    _JSON_TYPES = [dict, list, collections.OrderedDict]

    __compy_wrapper_key = "compy_conversion_wrapper"
    __compy_json_type_key = "@compy_json_type"

    def get_conversion_methods(self, conv_type=None):
        if conv_type == "XML":
            conversion_methods = {str: lambda data: etree.fromstring(data)}
            conversion_methods.update(dict.fromkeys(self._XML_TYPES, lambda data: data))
            conversion_methods.update(dict.fromkeys(self._JSON_TYPES, lambda data: etree.fromstring(xmltodict.unparse(self.__internal_xmlify(data)).encode('utf-8'))))
            conversion_methods.update({None.__class__: lambda data: etree.fromstring("<root/>")})
            return conversion_methods
        elif conv_type == "JSON":
            conversion_methods = {str: lambda data: json.loads(data)}
            conversion_methods.update(dict.fromkeys(self._JSON_TYPES, lambda data: json.loads(json.dumps(data, default=self.decimal_default))))
            conversion_methods.update(dict.fromkeys(self._XML_TYPES, lambda data: self.__remove_internal_xmlify(xmltodict.parse(etree.tostring(data), expat=expat))))
            conversion_methods.update({None.__class__: lambda data: {}})
            return conversion_methods
        else:
            return collections.defaultdict(lambda: lambda data: data)

    def __internal_xmlify(self, _json):
        if isinstance(_json, dict) and len(_json) == 0:
            _json = {self.__compy_wrapper_key: {}}
        if isinstance(_json, list) or len(_json) > 1:
            _json = {self.__compy_wrapper_key: _json}
        _, value = next(iter(_json.items()))
        if isinstance(value, list):
            _json = {self.__compy_wrapper_key: _json}
        return _json

    def __remove_internal_xmlify(self, _json):
        if len(_json) == 1 and isinstance(_json, dict):
            key, value = next(iter(_json.items()))
            if key == self.__compy_wrapper_key:
                _json = value
        self.__json_conversion_crawl(_json)
        return _json

    def __json_get_value_of_dict(self, dict_obj):
        text = dict_obj.get("#text", None)
        if text is None or len(dict_obj) > 1:
            return dict_obj
        else:
            return text

    def __json_conversion_crawl_nested(self, _json):
        if isinstance(_json, dict):
            for key, value in _json.items():
                _json[key] = self.__json_conversion_crawl_nested(_json=value)
            json_type = _json.pop(self.__compy_json_type_key, None)
            if json_type == "list":
                new_obj = self.__json_get_value_of_dict(dict_obj=_json)
                if new_obj is None or (isinstance(new_obj, dict) and len(new_obj) == 0):
                    return []
                return [new_obj]
            elif json_type == "string":
                new_value = self.__json_get_value_of_dict(dict_obj=value)
                if isinstance(new_value, (dict, list)):
                    return json.dumps(new_value)
                else:
                    return new_value
            elif json_type == "dict":
                return _json
            elif json_type is not None:
                return None
            return _json
        elif isinstance(_json, list):
            for index, value in enumerate(_json):
                if isinstance(value, dict):
                    for key, sub_value in value.items():
                        value[key] = self.__json_conversion_crawl_nested(_json=sub_value)
                    json_type = value.pop(self.__compy_json_type_key, None)
                    if json_type == "list":
                        _json[index] = self.__json_get_value_of_dict(dict_obj=value)
                    elif json_type == "string":
                        new_value = self.__json_get_value_of_dict(dict_obj=value)
                        if isinstance(new_value, (dict, list)):
                            _json[index] = json.dumps(new_value)
                        else:
                            _json[index] = new_value
                    elif json_type == "dict":
                        _json[index] = None
                    else:
                        _json[index] = value
                else:
                    _json[index] = self.__json_conversion_crawl_nested(_json=value)
        return _json

    def __json_conversion_crawl(self, _json):
        if isinstance(_json, dict):
            for key, value in _json.items():
                _json[key] = self.__json_conversion_crawl_nested(_json=value)
        return _json

    def decimal_default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

__conversion_methods_obj = None

def _getConversionMethods():
    global __conversion_methods_obj
    if __conversion_methods_obj is None:
        __conversion_methods_obj = _ConversionMethods()
    return __conversion_methods_obj

class _EventConversionMixin:

    conversion_methods = _getConversionMethods().get_conversion_methods()

    def isInstance(self, convert_to, current_event=None):
        if current_event is None:
            current_event = self
        return convert_to in current_event.conversion_parents or convert_to == current_event.__class__

    def convert(self, convert_to, current_event=None, force=False, ignore_data=False):
        if current_event is None:
            current_event = self
        try:
            if not force and self.isInstance(convert_to=convert_to, current_event=current_event):
                return current_event
            new_class = convert_to.__new__(convert_to)
            new_class.__dict__.update(current_event.__dict__)
            if not ignore_data:
                new_class.data = current_event.data
        except Exception as err:
            raise InvalidEventConversion("Unable to convert event. <Attempted {old} -> {new}>".format(old=current_event.__class__, new=convert_to))
        return new_class

class _XMLEventConversionMixin(_EventConversionMixin):
    conversion_methods = _getConversionMethods().get_conversion_methods("XML")

class _JSONEventConversionMixin(_EventConversionMixin):
    conversion_methods = _getConversionMethods().get_conversion_methods("JSON")

class _EventFormatMixin(_EventConversionMixin):
    
    def _get_state(self):
        return dict(self.__dict__)

    def format_error(self):
        if self.error:
            messages = self.error.message
            if not isinstance(messages, list):
                messages = [messages]
            errors = map(lambda _error: dict(message=str(getattr(_error, "message", _error)), **self.error.__dict__), messages)
            return errors
        else:
            return None

    def error_string(self):
        if self.error:
            return str(self.format_error())
        else:
            return None

    def data_string(self):
        return str(self.data)

class _XMLEventFormatMixin(_XMLEventConversionMixin, _EventFormatMixin):

    def _get_state(self):
        state = _EventFormatMixin._get_state(self)
        if self.data is not None:
            state['_data'] = etree.tostring(self.data)
        return state

    def data_string(self):
        try:
            return etree.tostring(self.data)
        except TypeError:
            return None

    def format_error(self):
        errors = _EventFormatMixin.format_error(self)
        if errors is not None and len(errors) > 0:
            result = etree.Element("errors")
            for error in errors:
                error_element = etree.Element("error")
                message_element = etree.Element("message")
                error_element.append(message_element)
                message_element.text = error['message']
                result.append(error_element)
        return result

    def error_string(self):
        error = self.format_error()
        if error is not None:
            error = etree.tostring(error, pretty_print=True)
        return error

class _JSONEventFormatMixin(_JSONEventConversionMixin, _EventFormatMixin):

    def _get_state(self):
        state = _EventFormatMixin._get_state(self)
        if self.data is not None:
            state['_data'] = json.dumps(self.data)
        return state

    def data_string(self):
        return json.dumps(self.data, default=_getConversionMethods().decimal_default)

    def error_string(self):
        error = self.format_error()
        if error:
            try:
                error = json.dumps({"errors": error})
            except Exception:
                pass
        return error

class _EventModifyMixin:

    def _process_delete(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        del current_step

    def _process_replace(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        self._
    def _process_event_join(self, event, current_key, join_key, finished_content, content, *args, **kwargs):
        old_content = event.get(current_key, self._get_event_default())
        finished_content = self._join_scope(join=old_content, join_key=join_key)
        return self._join_scope(base=finished_content, join=content, join_key=join_key)

    def _process_event(self, event, current_key, finished_content, content, *args, **kwargs):
        return content

    def _process_event_set(self, event, key, content):
        event.set(key,content)

    def _process_event_delete(self, event, key, content):
        try:
            del event[key]
        except KeyError:
            pass

    def _event_modify(self,
            process_event_func,
            process_event_sd_func,
            process_func, 
            event, 
            content, 
            key_chain, 
            *args, 
            **kwargs):
        full_key_chain = key_chain[:]
        finished_content = None
        current_key = key_chain.pop(0)
        if len(key_chain) == 0:
            finished_content = process_event_func(event=event, current_key=current_key, finished_content=finished_content, content=content, *args, **kwargs)
            process_event_sd_func(event=event, key=current_key, content=finished_content)
        else:
            event_key = current_key
            current_key = self._process_second_key(key_chain=key_chain, current_key=current_key)
            event.set(event_key, event.get(event_key, self._get_event_default(key=current_key)))
            current_steps = [event.get(event_key)]
            #try:
            if True:
                self._perform_root_tests(current_key=current_key, current_steps=current_steps)
                while True:
                    try:
                        current_key = key_chain.pop(0)
                    except IndexError:
                        finished_content, current_steps = self._process_index_error(current_steps=current_steps, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, process_func=process_func, *args, **kwargs)
                        break
                    finished_content, current_steps = self._process_modify(current_steps=current_steps, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, process_func=process_func, *args, **kwargs)
            #except Exception as err:
            #   raise Exception("Unable to follow key chain {key_chain}. Ran into unexpected value".format(key_chain=full_key_chain,))
        return event

    def event_update(self, event, content, key_chain, *args, **kwargs):
        return self._event_modify(process_event_func=self._process_event,
            process_event_sd_func=self._process_event_set,
            process_func=self._process_update,
            event=event,
            content=content,
            key_chain=key_chain,
            *args,
            **kwargs)

    def event_join(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        return self._event_modify(process_event_func=self._process_event_join,
            process_event_sd_func=self._process_event_set,
            process_func=self._process_join,
            event=event,
            content=content,
            key_chain=key_chain,
            join_key=join_key,
            *args,
            **kwargs)

    def event_delete(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        return self._event_modify(process_event_func=self._process_event,
            process_event_sd_func=self._process_event_delete,
            process_func=self._process_delete,
            event=event,
            content=content,
            key_chain=key_chain,
            join_key=join_key,
            *args,
            **kwargs)

    def event_replace(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        return self._event_modify(process_event_func=self._process_event,
            process_event_sd_func=self._process_event_set,
            process_func=self._process_replace,
            event=event,
            content=content,
            key_chain=key_chain,
            join_key=join_key,
            *args,
            **kwargs)

class BasicEventModifyMixin:
    def event_update(self, event, content, key_chain, *args, **kwargs):
        event.set(key_chain.pop(), content)
        return event

    def event_join(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        event.set(key_chain.pop(), content)
        return event

    def event_delete(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        del event[key_chain.pop()]

    def event_replace(self, event, content, key_chain, join_key="aggregated", *args, **kwargs):
        event.set(key_chain.pop(), content)
        return event

class JSONEventModifyMixin(_EventModifyMixin):

    def _get_event_default(self, key=None):
        return {}

    def _process_second_key(self, key_chain, current_key):
        return current_key

    def _perform_root_tests(self, current_key, current_steps):
        pass

    def _process_index_error(self, current_steps, content, current_key, key_chain, finished_content, process_func, *args, **kwargs):
        return finished_content, current_steps

    def _process_modify(self, current_steps, content, current_key, key_chain, finished_content, process_func, *args, **kwargs):
        new_current_steps = []
        for current_step in current_steps:
            try:
                if len(key_chain) == 0:
                    raise KeyError
                next_step = current_step[current_key]
            except KeyError:
                local_steps = []
                try:
                    current_step[current_key]
                    raise Exception
                except TypeError:
                    local_steps.extend(current_step)
                except Exception:
                    local_steps.append(current_step)
                for local_step in local_steps:
                    finished_content, next_step, current_step = process_func(current_step=local_step, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, *args, **kwargs)
            new_current_steps.append(next_step)
        return finished_content, new_current_steps

    def _process_delete(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        if len(key_chain) > 0:
            next_step = {}
            current_step[current_key] = next_step
            return finished_content, next_step, current_step
        try:
            del current_step[current_key]
        except KeyError:
            pass
        return finished_content, None, None

    def _process_replace(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        return self._process_update(current_step=current_step, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, *args, **kwargs)

    def _process_update(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        next_step = {} if len(key_chain) > 0 else content
        current_step[current_key] = next_step
        return finished_content, next_step, current_step

    def _process_join(self, current_step, content, current_key, key_chain, finished_content, join_key, *args, **kwargs):
        data = self.__get_next_scope(current_step=current_step, current_key=current_key)
        if len(key_chain) == 0 and data is not None:
            finished_content = self._join_scope(join=data, join_key=join_key)
        finished_content = self._join_scope(base=finished_content, join=content, join_key=join_key)
        next_step = {} if len(key_chain) > 0 else finished_content
        current_step[current_key] = next_step
        return finished_content, next_step, current_step
    
    def __try_join_scope(self, join_key, base, join):
        try:
            try:
                base[join_key] = base[join_key] + join
            except TypeError:
                base[join_key].append(join)
        except AttributeError:
            base[join_key].update(join)
        return base

    def _join_scope(self, join, join_key, base=None):
        try:
            base[join_key]
        except TypeError:
            return {join_key:join}
        try:
            try:
                self.__try_join_scope(join_key=join_key, join=join, base=base)
            except AttributeError:
                base[join_key] = [base[join_key]]
                base = self.__try_join_scope(join_key=join_key, join=join, base=base)
        except ValueError:
            try:
                base[join_key] = [base[join_key]] + join
            except TypeError:
                base[join_key] = [base[join_key]] + [join]
        return base

    def __get_next_scope(self, current_step, current_key):
        try:
            return current_step[current_key]
        except KeyError:
            return None

class XMLEventModifyMixin(_EventModifyMixin):

    def _get_event_default(self, key=None):
        if key is None:
            return None
        return etree.Element(key)

    def _process_second_key(self, key_chain, current_key):
        return key_chain.pop(0)

    def _perform_root_tests(self, current_key, current_steps):
        for current_step in current_steps:
            if not current_key == current_step.tag:
                raise Exception()

    def _process_index_error(self, current_steps, content, current_key, key_chain, finished_content, process_func, *args, **kwargs):
        new_current_steps = []
        for current_step in current_steps:
            finished_content, _, current_step = process_func(current_step=current_step, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, *args, **kwargs)
            new_current_steps.append(current_step)
        return finished_content, new_current_steps

    def __identify_matches(self, current_step, current_key):
        key_match = re.match("(.+?)(\[([0-9]*)\])?$", current_key)
        local_key, index = key_match.group(1), key_match.group(3)
        index = int(index) if index is not None else index
        matching_subelements = current_step.findall(local_key)
        num_matches = len(matching_subelements)
        matching_elements = []
        if index is None:
            if num_matches == 0:
                next_step = etree.Element(local_key)
                current_step.append(next_step)
                matching_subelements.append(next_step)
            for next_step in matching_subelements:
                matching_elements.append(next_step)
        else:
            for new_index in range(num_matches, index+1):
                next_step = etree.Element(local_key)
                current_step.append(next_step)
            matching_elements.append(current_step.findall(local_key)[index])
        return matching_elements

    def _process_modify(self, current_steps, content, current_key, key_chain, finished_content, process_func, *args, **kwargs):
        new_current_steps = []
        for current_step in current_steps:
            matches = self.__identify_matches(current_step=current_step, current_key=current_key)
            new_current_steps.extend(matches)
        return finished_content, new_current_steps

    def _process_delete(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        try:
            tag = content.tag
        except TypeError:
            children = list(current_step)
        except AttributeError:
            children = current_step.findall(str(content))
        else:
            children = current_step.findall(tag)
        try:
            for child in children:
                current_step.remove(child)
            current_step.text = None
        except TypeError:
            pass
        return finished_content, None, current_step

    def _process_replace(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        try:
            content_tag = content.tag
        except (TypeError, AttributeError):
            current_step.text = str(content)
        else:
            matches = self.__identify_matches(current_step=current_step, current_key=content_tag)
            children = list(current_step)
            for child in children:
                current_step.remove(child)
            for index, child in enumerate(children):
                if child in matches:
                    current_step = self.__append_scope(current_step=current_step, content=content)
                else:
                    current_step = self.__append_scope(current_step=current_step, content=child)
        return finished_content, None, current_step

    def _process_update(self, current_step, content, current_key, key_chain, finished_content, *args, **kwargs):
        current_step = self.__append_scope(current_step=current_step, content=content)
        return finished_content, None, current_step

    def _process_join(self, current_step, content, current_key, key_chain, finished_content, join_key, *args, **kwargs):
        current_step, finished_content = self.__create_transfer_scope(current_step=current_step, join_key=join_key)
        finished_content = self._join_scope(base=finished_content, join=content, join_key=join_key)
        current_step = self.__append_scope(current_step=current_step, content=finished_content)
        return finished_content, None, current_step

    def __append_scope(self, current_step, content):
        try:
            current_step.append(etree.fromstring(etree.tostring(content)))
        except TypeError:
            current_step.text = str(content)
        return current_step

    def _join_scope(self, join, join_key, base=None):
        if base is None:
            base = etree.Element(join_key)
        if join is not None:
            base = self.__append_scope(current_step=base, content=join)
        return base

    def __create_transfer_scope(self, current_step, join_key):
        joined_content = None
        subelements = list(current_step)
        for sub in subelements:
            current_step.remove(sub)
            joined_content = self._join_scope(base=joined_content, join=sub, join_key=join_key)
        if current_step.text is not None:
            joined_content = self._join_scope(base=joined_content, join=current_step.text, join_key=join_key)
            current_step.text = None
        return current_step, joined_content

class LookupMixin(object):
    def _try_list_get(self, obj, key, default):
        try:
            return obj[int(key)]
        except (ValueError, IndexError, TypeError, KeyError, AttributeError) as e:
            return default

    def _try_getattr(self, obj, key, default):
        try:
            return getattr(obj, key, default)
        except TypeError as e:
            return default

    def _try_obj_get(self, obj, key, default):
        try:
            return obj.get(key, default)
        except (TypeError, AttributeError) as e:
            return default

    def _try_ele_find(self, obj, key, default):
        try:
            found = obj.find(key)
            return default if found is None else found
        except (TypeError, AttributeError) as e:
            return default

    def _try_ele_attr(self, obj, key, default):
        try:
            return obj.get(key[1:], default) if key.startswith('@') else default
        except (TypeError, AttributeError) as e:
            return default

    def _single_lookup(self, obj, key, default):
        return self._try_getattr(obj=obj, key=key, 
            default=self._try_ele_find(obj=obj, key=key, 
                default=self._try_obj_get(obj=obj, key=key, 
                    default=self._try_list_get(obj=obj, key=key, 
                        default=self._try_ele_attr(obj=obj, key=key, 
                            default=default)))))

    def lookup(self, obj, key_chain=[]):
        return reduce(lambda obj, key: self._single_lookup(obj, key, None), [obj] + key_chain)

class XPathLookupMixin(LookupMixin):
    def __interpret_key_chain(self, key_chain):
        key_chain = key_chain[:]
        event_key = key_chain.pop(0)
        try:
            key_chain, xpath = key_chain[:-1], key_chain[-1]
        except IndexError:
            key_chain, xpath = [], "/"
        key_chain = [event_key] + key_chain
        return key_chain, xpath

    def lookup(self, obj, key_chain=[]):
        key_chain, xpath = self.__interpret_key_chain(key_chain=key_chain)
        xpath_scope = super(XPathLookupMixin, self).lookup(obj=obj, key_chain=key_chain)
        lookup = XPathLookup(xpath_scope)
        xpath_lookup = lookup.lookup(xpath)
        values = []
        for result in xpath_lookup:
            values.append(self.__parse_result(result=result))
        return values

    def __parse_result(self, result):
        try:
            if len(result.getchildren()) == 0:
                return result.text
        except AttributeError:
            pass
        return result