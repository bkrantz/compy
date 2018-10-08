import mimeparse
from bottle import *
from collections import defaultdict
import json

from compy.actor import Actor
from compy.errors import InvalidEventConversion, InvalidEventDataModification, MalformedEventData, ResourceNotFound
from compy.event import HttpEvent, JSONHttpEvent, XMLHttpEvent, JSONEvent

__all__ = [
	"HeaderController",
	"CORSController",
	"RequestInterpretor",
	"ResponseInterpretor",
	"RequestRouter"
]

class HeaderController(Actor):
	def __init__(self, name, headers={}, *args, **kwargs):
		super(HeaderController, self).__init__(name=name, *args, **kwargs)
		self.headers = headers

	def consume(self, event, *args, **kwargs):
		current_headers = event.get('headers', {})
		current_headers.update(self.headers)
		event.set('headers', current_headers)
		self.send_event(event)

class CORSController(Actor):
	def __init__(self, name, clear_data=False, origins=[], methods=[], headers=[], *args, **kwargs):
		super(CORSController, self).__init__(name=name, *args, **kwargs)
		self.origins = origins
		self.methods = methods
		self.headers = headers
		self.clear_data = clear_data

	def consume(self, event, *args, **kwargs):
		origin = event.environment["request"]["headers"].get("Origin", None)
		method = event.environment["request"]["headers"].get("Access-Control-Request-Method", None)
		headers = event.environment["request"]["headers"].get("Access-Control-Request-Headers", None)
		
		new_headers = {}
		if len(self.origins) == 0 or origin in self.origins:
			new_headers["Access-Control-Allow-Origin"] = origin

		if method is not None:
			if len(self.methods) == 0:
				new_headers["Access-Control-Allow-Methods"] = ",".join(event.environment["accepted_methods"])
			else:
				new_headers["Access-Control-Allow-Methods"] = ",".join(self.methods)

		if headers is not None:
			if len(self.headers) == 0:
				new_headers["Access-Control-Allow-Headers"] = "content-type"
			else:
				new_headers["Access-Control-Allow-Headers"] = ",".join(self.headers)

		current_headers = event.environment["response"].get("headers", {})
		current_headers.update(new_headers)
		event.environment["response"]['headers'] = current_headers
		if self.clear_data:
			event._data = None
		self.send_event(event)

class __HTTPInterpretor(Actor):
	DEFAULT_DATA_SOURCE = "data"
	DEFAULT_EVENT_CLASS = HttpEvent
	DEFAULT_MIME_TYPE = "text/plain"
	DEFAULTING_CONTENT_TYPES = ["", None]
	CONVERTING_ACCEPTS = ["*/*", "", None]
	MIME_TYPES = {
		"application":{
			"xml":{
				"event_class": XMLHttpEvent
			},
			"json":{
				"event_class": JSONHttpEvent
			},
			"xml+schema":{
				"event_class": XMLHttpEvent
			},
			"json+schema":{
				"event_class": JSONHttpEvent
			},
			"x-www-form-urlencoded":{
				"event_class": JSONHttpEvent,
				"data_source": "forms",
				"interpreted_type": "text/plain"
			}
		},
		"text":{
			"xml":{
				"event_class": XMLHttpEvent
			},
			"json":{
				"event_class": JSONHttpEvent
			},
			"plain":{}
		}
	}

	CONTENT_TYPES = ["%s/%s" % (main_type, sub_type) for main_type in MIME_TYPES.iterkeys() for sub_type in MIME_TYPES[main_type].iterkeys() ]

	def _interpret_mime_type(self, raw_mime, default_raw_mime=None):
		try:
			interpreted_mime = mimeparse.best_match(self.CONTENT_TYPES, raw_mime)
		except Exception:
			interpreted_mime = raw_mime
		if default_raw_mime is None:
			if raw_mime in self.DEFAULTING_CONTENT_TYPES:
				interpreted_mime = DEFAULT_MIME_TYPE
		else:
			if raw_mime in self.CONVERTING_ACCEPTS:
				interpreted_mime = default_raw_mime
		main_type, sub_type = interpreted_mime.split("/")

		converted_type = self.MIME_TYPES[main_type][sub_type].get("interpreted_type", interpreted_mime)
		main_type, sub_type = converted_type.split("/")
		event_class = self.MIME_TYPES[main_type][sub_type].get("event_class", self.DEFAULT_EVENT_CLASS)
		data_source = self.MIME_TYPES[main_type][sub_type].get("data_source", self.DEFAULT_DATA_SOURCE)
		return converted_type, event_class, data_source

class RequestInterpretor(__HTTPInterpretor):
	def consume(self, event, *args, **kwargs):
		raw_mime_type = event.environment["request"]["headers"].get("Content-Type", "").split(';')[0]
		interpreted_mime_type, event_class, data_source = self._interpret_mime_type(raw_mime=raw_mime_type)
		event.environment["request"]["interpreted_content_type"] = interpreted_mime_type
		try:
			event.data = event.get(data_source, None)
			if not isinstance(event, event_class):
				event = event.convert(event_class)
		except (InvalidEventConversion, MalformedEventData) as err:
			self.logger.error("Unable to Interpret Request: %s" % err)
			event.error = err
			self.send_error(event)
		else:
			self.logger.info("Interpreted Request's Content-Type '%s' AS '%s': Using event class '%s'" % (raw_mime_type, interpreted_mime_type, event_class.__name__))
			self.send_event(event)

class ResponseInterpretor(__HTTPInterpretor):
	def format_response_data(self, event):
		if event.error:
			if isinstance(event.data, (list, dict, str)):
				response_data = json.dumps({"errors": event.format_error()})
			else:
				response_data = event.error_string()
		else:
			response_data = event.data_string()
			if not isinstance(event.data, (list, dict, str)) or \
					(isinstance(event.data, dict) and \
						len(event.data) == 1 and \
						event.data.get("data", None)):
				response_data = event.data_string()
			else:
				response_dict = event.data
				response_data = json.dumps(response_dict)
		return response_data

	def consume(self, event, *args, **kwargs):
		accept = event.environment["request"]["headers"].get("Accept", "").split(";")[0]
		content_type = event.environment["request"].get("interpreted_content_type", None)
		interpreted_mime_type, event_class, _ = self._interpret_mime_type(raw_mime=accept, default_raw_mime=content_type)
		event.environment["response"]["headers"]["Content-Type"] = interpreted_mime_type
		try:
			if not event.isInstance(event_class):
				event = event.convert(event_class)
		except (InvalidEventConversion, MalformedEventData) as err:
			event.error = err
			#self.send_error(event)
		data = self.format_response_data(event)
		event.data = data
		event = event.convert(convert_to=HttpEvent, force=True)
		self.send_event(event)



class ServiceRouter(Actor):
	def __is_cors(self, event):
		origin = event.environment["request"]["headers"].get("Origin", None)
		cur_method = event.environment["request"]["method"]
		req_method = event.environment["request"]["headers"].get("Access-Control-Request-Method", None)
		if origin is not None and cur_method == "OPTIONS" and req_method is not None:
			return True
		return False

	def consume(self, event, *args, **kwargs):
		queue_name = event.service or self.name
		queue = self.pool.outbound.get(queue_name, None)
		if self.__is_cors(event=event):
			queue = self.pool.outbound.get("cors", queue)
		if queue is None:
			self.logger.error("Queue name '{queue_name}' was not found".format(queue_name=queue_name))
			self.send_error(event)
		else:
			self.send_event(event, queues=[queue])
