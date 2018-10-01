#!/usr/bin/env python

import Cookie
import datetime

from uuid import uuid4 as uuid

from compy.actor import Actor

class SetCookie(Actor):

    def __init__(self, name, cookie_key="session", cookie_value="session_value", value_mode="static", expiry=6000, path="/", *args, **kwargs):
        super(SetCookie, self).__init__(name, *args, **kwargs)
        self.cookie_key = cookie_key
        self.expiry = expiry
        self.path = path
        self.value_mode = value_mode
        self.cookie_value = cookie_value

    @property
    def cookie_value(self):
        if self.value_mode != "static":
            return uuid().get_hex()
        else:
            return self._cookie_value

    @cookie_value.setter
    def cookie_value(self, cookie_value):
        self._cookie_value = cookie_value

    def consume(self, event, *args, **kwargs):
        try:
            session_cookie = Cookie.SimpleCookie()
            session_cookie[self.cookie_key] = self.cookie_value
            session_cookie[self.cookie_key]["Path"] = self.path
            session_cookie[self.cookie_key]['expires'] = str(datetime.datetime() + datetime.timedelta(0, self.expiry))
            event.headers.update({"Set-Cookie": next(session_cookie.itervalues()).OutputString()})

            self.logger.debug("Assigned incoming HttpEvent cookie '{key}' value of '{value}'".format(key=self.cookie_key,
                                                                                                     value=session_cookie['session']), event=event)

            self.send_event(event)
        except Exception as err:
            self.logger.error("Unable to assign cookie: {err}".format(err=err), event=event)
            self.send_error(event)

# TODO: Implement static routed "CheckCookie" that routes based on cookie presence and value
# TODO: Implement expiry in value reading