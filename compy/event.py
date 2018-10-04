#!/usr/bin/env python


import traceback
import collections
from uuid import uuid4 as uuid
from copy import deepcopy
from datetime import datetime

from .errors import (ResourceNotModified, MalformedEventData, InvalidEventDataModification, InvalidEventModification,
    ForbiddenEvent, ResourceNotFound, EventCommandNotAllowed, ActorTimeout, ResourceConflict, ResourceGone,
    UnprocessableEventData, EventRateExceeded, CompysitionException, ServiceUnavailable, UnauthorizedEvent)

from compy.mixins.event import (_EventConversionMixin, _XMLEventConversionMixin, _JSONEventConversionMixin, 
    _EventFormatMixin, _XMLEventFormatMixin, _JSONEventFormatMixin)

DEFAULT_SERVICE = "default"
DEFAULT_STATUS_CODE = 200

__all__ = [
    "Event",
    "LogEvent",
    "HttpEvent",
    "XMLEvent",
    "XMLHttpEvent",
    "JSONEvent",
    "JSONHttpEvent"
]

HTTPStatusMap = collections.defaultdict(lambda: {"status": 500},
    {
        ResourceNotModified:            {"status": 304},
        MalformedEventData:             {"status": 400},
        InvalidEventDataModification:   {"status": 400},
        InvalidEventModification:       {"status": 400},
        UnauthorizedEvent:              {"status": 401,
            "headers": {'WWW-Authenticate': 'Basic realm="Compysition Server"'}},
        ForbiddenEvent:                 {"status": 403},
        ResourceNotFound:               {"status": 404},
        EventCommandNotAllowed:         {"status": 405},
        ActorTimeout:                   {"status": 408},
        ResourceConflict:               {"status": 409},
        ResourceGone:                   {"status": 410},
        UnprocessableEventData:         {"status": 422},
        EventRateExceeded:              {"status": 429},
        CompysitionException:           {"status": 500},
        ServiceUnavailable:             {"status": 503},
        type(None):                     {"status": 200}
    })

HTTPStatuses = {
    200: "OK",
    201: "Created",
    202: "Accepted",
    204: "No Content",
    205: "Reset Content",
    206: "Partial Content",
    304: "Not Modified",
    400: "Bad Request",
    401: "Unauthorized",
    402: "Payment Required",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    406: "Not Acceptable",
    408: "Request Timeout",
    409: "Conflict",
    410: "Gone",
    411: "Length Required",
    415: "Unsupported Media Type",
    422: "Unprocessable Entity",
    423: "Locked",
    429: "Too Many Requests",
    500: "Internal Server Error",
    501: "Not Implemented",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
    508: "Loop Detected",
}

class _BaseEvent(object):

    def __init__(self, meta_id=None, data=None, service=None, *args, **kwargs):
        self.service = service
        self.event_id = uuid().get_hex()
        self.meta_id = meta_id if meta_id else self.event_id
        self._data = None
        self.data = data
        self.error = None
        self.created = datetime.now()
        self.__dict__.update(kwargs)

    def set(self, key, value):
        try:
            setattr(self, key, value)
            return True
        except Exception:
            return False

    def get(self, key, default=None):
        return getattr(self, key, default)

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, service):
        self._set_service(service)

    def _set_service(self, service):
        self._service = service
        if self._service == None:
            self._service = DEFAULT_SERVICE

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        try:
            self._data = self.conversion_methods[data.__class__](data)
        except KeyError:
            raise InvalidEventDataModification("Data of type '{_type}' was not valid for event type {cls}: {err}".format(_type=type(data), cls=self.__class__, err=traceback.format_exc()))
        except ValueError as err:
            raise InvalidEventDataModification("Malformed data: {err}".format(err=err))
        except Exception as err:
            raise InvalidEventDataModification("Unknown error occurred on modification: {err}".format(err=err))

    @property
    def event_id(self):
        return self._event_id

    @event_id.setter
    def event_id(self, event_id):
        if self.get("_event_id", None) is not None:
            raise InvalidEventDataModification("Cannot alter event_id once it has been set. A new event must be created")
        else:
            self._event_id = event_id

    def get_properties(self):
        return {k: v for k, v in self.__dict__.iteritems() if k != "data" and k != "_data"}

    def __getstate__(self):
        return self._get_state()

    def __setstate__(self, state):
        self.__dict__ = state
        self.data = state.get('_data', None)
        self.error = state.get('_error', None)

    def __str__(self):
        return str(self.__getstate__())

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, exception):
        self._set_error(exception)

    def _set_error(self, exception):
        self._error = exception

    def clone(self):
        return deepcopy(self)

class _BaseLogEvent(_BaseEvent):
    def __init__(self, level, origin_actor, message, id=None, *args, **kwargs):
        super(_BaseLogEvent, self).__init__(*args, **kwargs)
        self.id = id
        self.level = level
        self.time = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        self.origin_actor = origin_actor
        self.message = message
        self.data = {
            "id":              self.id,
            "level":            self.level,
            "time":             self.time,
            "origin_actor":     self.origin_actor,
            "message":          self.message
        }

class _BaseHttpEvent(_BaseEvent):
    def __init__(self, environment={}, *args, **kwargs):
        self._ensure_environment(environment)
        super(_BaseHttpEvent, self).__init__(*args, **kwargs)

    def __recursive_update(self, d, u):
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                d[k] = self.__recursive_update(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    def _set_service(self, service):
        if service is None:
            service = self.environment["request"]["url"]["path_args"].get("queue", None)
        super(_BaseHttpEvent, self)._set_service(service=service)

    def _ensure_environment(self, environment):
        if self.get("environment", None) is None:
            self.environment = {
                "request": {
                    "headers": {},
                    "method": None,
                    "url":{
                        "scheme": None,
                        "domain": None,
                        "query": None,
                        "path": None,
                        "path_args": {},
                        "query_args": {}
                    }
                },
                "response": {
                    "headers": {},
                    "status": DEFAULT_STATUS_CODE
                },
                "remote": {
                    "address": None,
                    "port": None
                },
                "server": {
                    "name": None,
                    "port": None,
                    "protocol": None
                },
                "accepted_methods": []
            }
        self.environment = self.__recursive_update(self.environment, environment)

    @property
    def status(self):
        return self.environment["response"]["status"]

    @status.setter
    def status(self, status):
        if status is None:
            status = DEFAULT_STATUS_CODE
        try:
            HTTPStatuses[status]
        except KeyError, AttributeError:
            raise InvalidEventModification("Unrecognized status code")
        else:
            self.environment["response"]["status"] = status

    def update_headers(self, headers={}, **kwargs):
        self.environment["response"]["headers"].update(headers)
        self.environment["response"]["headers"].update(kwargs)

    def _set_error(self, exception):
        if exception is not None:
            error_state = HTTPStatusMap[exception.__class__]
            self.status = error_state.get("status", None)
            self.update_headers(headers=error_state.get("headers", {}))
        super(_BaseHttpEvent, self)._set_error(exception)

class Event(_EventFormatMixin, _BaseEvent):
    conversion_parents = []

class LogEvent(_EventFormatMixin, _BaseLogEvent):
    conversion_parents = [Event]

class HttpEvent(_EventFormatMixin, _BaseHttpEvent):
    conversion_parents = [Event]

class XMLEvent(_XMLEventFormatMixin, _BaseEvent):
    conversion_parents = [Event]

class XMLHttpEvent(_XMLEventFormatMixin, _BaseHttpEvent):
    conversion_parents = [Event, HttpEvent]

class JSONEvent(_JSONEventFormatMixin, _BaseEvent):
    conversion_parents = [Event]

class JSONHttpEvent(_JSONEventFormatMixin, _BaseHttpEvent):
    conversion_parents = [Event, HttpEvent]
