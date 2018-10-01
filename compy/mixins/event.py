from lxml import etree
from compy.event import JSONEvent, XMLEvent
from compy.actors.util.xpath import XPathLookup
import re

__all__ = [
	"JSONEventModifyMixin",
	"XMLEventModifyMixin",
	"LookupMixin",
	"XPathLookupMixin"
]

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
			#	raise Exception("Unable to follow key chain {key_chain}. Ran into unexpected value".format(key_chain=full_key_chain,))
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

class JSONEventModifyMixin(_EventModifyMixin):
	input = JSONEvent

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
				finished_content, next_step, current_step = process_func(current_step=current_step, content=content, current_key=current_key, key_chain=key_chain, finished_content=finished_content, *args, **kwargs)
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
	input = XMLEvent

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