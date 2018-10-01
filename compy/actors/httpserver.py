#!/usr/bin/env python
#
# -*- coding: utf-8 -*-
#
#  wsgi.py
#
#  Copyright 2014 James Hulett <james.hulett@cuanswers.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.

import json
import mimeparse
import re

from bottle import * #TODO identify exact imports
from collections import defaultdict
from datetime import datetime
from functools import wraps
from gevent import pywsgi
from gevent.queue import Queue

from compy.actor import Actor
from compy.errors import InvalidEventDataModification, MalformedEventData, ResourceNotFound
from compy.event import HttpEvent, JSONHttpEvent, XMLHttpEvent, response_statuses

BaseRequest.MEMFILE_MAX = 1024 * 1024 # (or whatever you want)

class ContentTypePlugin(object):
    """**Bottle plugin that filters basic content types that are processable by Compysition**"""

    DEFAULT_VALID_TYPES = ("text/xml",
                           "application/xml",
                           "text/plain",
                           "text/html",
                           "application/json",
                           "application/x-www-form-urlencoded")

    name = "ctypes"
    api = 2

    def __init__(self, default_types=None):
        self.default_types = default_types or self.DEFAULT_VALID_TYPES

    def apply(self, callback, route):
        ctype = request.content_type.split(';')[0]
        ignore_ctype = route.config.get('ignore_ctype', False) or request.content_length < 1
        if ignore_ctype or ctype in route.config.get('ctypes', self.default_types):
            return callback
        else:
            raise HTTPError(415, "Unsupported Content-Type '{_type}'".format(_type=ctype))


class HTTPServer(Actor, Bottle):
    """**Receive events over HTTP.**

    Actor runs a pywsgi gevent webserver, using an optional routes json file for complex routing using Bottle

    Parameters:
        name (str):
            | The instance name.
        address(Optional[str]):
            | The address to bind to.
            | Default: 0.0.0.0
        port(Optional[int]):
            | The port to bind to.
            | Default: 8080
        keyfile(Optional([str]):
            | In case of SSL the location of the keyfile to use.
            | Default: None
        certfile(Optional[str]):
            | In case of SSL the location of the certfile to use.
            | Default: None
        routes_config(Optional[dict]):
            | This is a JSON object that contains a list of Bottle route config kwargs
            | Default: {"routes": [{"path: "/<queue>", "method": ["POST"]}]}
            | Field values correspond to values used in bottle.Route class
            | Special values:
            |    id(Optional[str]): Used to identify this route in the json object
            |    base_path(Optional[str]): Used to identify a route that this route extends, using the referenced id

    Examples:
        Default:
            http://localhost:8080/foo is mapped to 'foo' queue
            http://localhost:8080/bar is mapped to 'bar' queue
        routes_config:
            routes_config {"routes": [{"path: "/my/url/<queue>", "method": ["POST"]}]}
                http://localhost:8080/my/url/goodtimes is mapped to 'goodtimes' queue


    """

    DEFAULT_ROUTE = {
        "routes":
            [
                {
                    "id": "base",
                    "path": "/<queue>",
                    "method": [
                        "POST"
                    ]
                }
            ]
    }

    input = HttpEvent
    output = HttpEvent

    QUEUE_REGEX = re.compile("<queue:re:[a-zA-Z_0-9]+?>")

    # Order matters, as this is used to resolve the returned content type preserved in the accept header, in order of increasing preference.

    def combine_base_paths(self, route, named_routes):
        base_path_id = route.get('base_path', None)
        if base_path_id:
            base_path = named_routes.get(base_path_id, None)
            if base_path:
                return HTTPServer._normalize_queue_definition(self.combine_base_paths(base_path, named_routes) + route['path'])
            else:
                raise KeyError("Base path '{base_path}' doesn't reference a defined path ID".format(base_path=base_path_id))
        else:
            return route.get('path')


    @staticmethod
    def _parse_queue_variables(path):
        return HTTPServer.QUEUE_REGEX.findall(path)

    @staticmethod
    def _parse_queue_names(path):
        path_variables = HTTPServer._parse_queue_variables(path)
        path_names = [s.replace("<queue:re:", '')[:-1] for s in path_variables]

        return path_names

    @staticmethod
    def _normalize_queue_definition(path):
        """
        This method is used to filter the queue variable in a path, to support the idea of base paths with multiple queue
        definitions. In effect, the <queue> variable in a path is provided at the HIGHEST level of definition. AKA: A higher
        level route containing a <queue:re:foo> will override the definition of <queue:re:bar> in a base_path.

        e.g. /<queue:re:foo>/<queue:re:bar> -> /foo/<queue:re:bar>

        This ONLY works for SIMPLE regex cases, which should be the case anyway for the queue name.
        """

        path_variables = HTTPServer._parse_queue_variables(path)
        path_names = HTTPServer._parse_queue_names(path)

        for path_variable in path_variables[:-1]:
            path = path.replace(path_variable, path_names.pop(0))

        return path

    def __init__(self, name, address="0.0.0.0", port=8080, keyfile=None, certfile=None, routes_config=None, send_errors=False, use_response_wrapper=True, *args, **kwargs):
        Actor.__init__(self, name, *args, **kwargs)
        Bottle.__init__(self)
        self.blockdiag_config["shape"] = "cloud"
        self.address = address
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.responders = {}
        self.send_errors = send_errors
        self.use_response_wrapper = use_response_wrapper
        self.accepted_methods = []
        routes_config = routes_config or self.DEFAULT_ROUTE

        if isinstance(routes_config, str):
            routes_config = json.loads(routes_config)

        if isinstance(routes_config, dict):
            named_routes = {route['id']:{'path': route['path'], 'base_path': route.get('base_path', None)} for route in routes_config.get('routes') if route.get('id', None)}
            for route in routes_config.get('routes'):
                callback = getattr(self, route.get('callback', 'callback'))
                if route.get('base_path', None):
                    route['path'] = self.combine_base_paths(route, named_routes)

                if route.get('method', None) is None:
                    route['method'] = []
                else:
                    for method in route['method']:
                        if method not in self.accepted_methods:
                            self.accepted_methods.append(method)
                if len(route['method']) > 0 and "OPTIONS" not in route['method']:
                    route['method'].append("OPTIONS")
                
                self.logger.debug("Configured route '{path}' with methods '{methods}'".format(path=route['path'], methods=route['method']))
                self.route(callback=callback, **route)

        self.wsgi_app = self
        self.wsgi_app.install(self.log_to_logger)
        self.wsgi_app.install(ContentTypePlugin())

    def log_to_logger(self, fn):
        '''
        Wrap a Bottle request so that a log line is emitted after it's handled.
        '''
        @wraps(fn)
        def _log_to_logger(*args, **kwargs):
            self.logger.info('[{address}] {method} {url}'.format(address=request.remote_addr,
                                            method=request.method,
                                            url=request.url))
            actual_response = fn(*args, **kwargs)
            return actual_response
        return _log_to_logger

    def __call__(self, e, h):
        """**Override Bottle.__call__ to strip trailing slash from incoming requests**"""

        e['PATH_INFO'] = e['PATH_INFO'].rstrip('/')
        return Bottle.__call__(self, e, h)

    def consume(self, event, *args, **kwargs):
        response_queue = self.responders.pop(event.event_id, None)

        if response_queue:
            local_response = HTTPResponse()
            status, status_message = event.status, response_statuses.get(event.status, "")
            local_response.status = "{code} {message}".format(code=status, message=status_message)

            for header, value in event.environment["response"]["headers"].iteritems():
                local_response.set_header(header, value)

            local_response.body = event.data

            response_queue.put(local_response)
            response_queue.put(StopIteration)
            self.logger.info("[{status}] Returned in {time:0.0f} ms".format(status=local_response.status, time=(datetime.now()-event.created).total_seconds() * 1000), event=event)
        else:
            self.logger.warning("Received event response for an unknown event ID. The request might have already received a response", event=event)

    def __format_env(self, environ):
        return {
            "request": {
                "headers": {key: value for key, value in request.headers.iteritems()},
                "method": environ["REQUEST_METHOD"],
                "url":{
                    "scheme": environ["bottle.request.urlparts"].scheme,
                    "domain": environ["bottle.request.urlparts"].netloc,
                    "query": environ["bottle.request.urlparts"].query,
                    "path": environ["PATH_INFO"],
                    "path_args": {key: value for key, value in environ["route.url_args"].iteritems()},
                    "query_args": {key: value for key, value in environ["bottle.request"].query.iteritems()}
                }
            },
            "response": {
                "headers": {},
                "status": 200
            },
            "remote": {
                "address": environ["REMOTE_ADDR"],
                "port": environ["REMOTE_PORT"]
            },
            "server": {
                "name": environ["SERVER_NAME"],
                "port": environ["SERVER_PORT"],
                "protocol": environ["SERVER_PROTOCOL"]
            },
            "accepted_methods": self.accepted_methods
       }


    def callback(self, queue=None, *args, **kwargs):
        try:
            data = request.body.read()
            data = data if len(data) > 0 else None
        except Exception:
            data = None
        event = HttpEvent(environment=self.__format_env(request.environ), forms=dict(request.forms), data=data)

        response_queue = Queue()
        self.responders.update({event.event_id: response_queue})
        self.logger.info("Received {0} request for service {1}".format(request.method, queue or self.name), event=event)

        self.send_event(event)

        return response_queue

    def post_hook(self):
        self.__server.stop()
        self.logger.info("Stopped serving")

    def __serve(self):
        if self.keyfile is not None and self.certfile is not None:
            self.__server = pywsgi.WSGIServer((self.address, self.port), self, keyfile=self.keyfile, certfile=self.certfile)
        else:
            self.__server = pywsgi.WSGIServer((self.address, self.port), self, log=None)
        self.logger.info("Serving on {address}:{port}".format(address=self.address, port=self.port))
        self.__server.start()

    def pre_hook(self):
        self.__serve()