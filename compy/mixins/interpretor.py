from lxml import etree
import json
import re

class _InterpretorMixin:
	def __try_unformat_xml(self, data, default=None):
		try:
			cur = etree.tostring(data)
			return re.sub('<!--.*?-->', '', cur)
		except:
			if default:
				return default
			return data

	def __try_unformat_json(self, data, default=None):
		try:
			return json.dumps(data)
		except:
			if default:
				return default
			return data

	def unformat(self, data):
		return self.__try_unformat_xml(data=data, default=self.__try_unformat_json(data=data))

	def format(self, data):
		return self.unformat(data=data)

class XMLMixin(_InterpretorMixin):
	def format(self, data):
		return etree.fromstring(self.unformat(data=data))

class JSONMixin(_InterpretorMixin):
	def format(self, data):
		return json.loads(self.unformat(data=data))