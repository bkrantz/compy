#!/usr/bin/env python

from compy.actor import Actor
from compy.event import HttpEvent

class RESTTranslator(Actor):

    input = HttpEvent
    output = HttpEvent

    """
    The purpose of this actor is to translate a returning event into a proper RESTful
    response.

    e.g. method = POST and status = "200 OK" "method = 201 Created"
    e.g. method = PATCH and status = "200 OK" but no return data exists -> "204 No Content"
    """

    def __init__(self, name, url_post_location=None, *args, **kwargs):
        super(RESTTranslator, self).__init__(name, *args, **kwargs)
        self.url_post_location = url_post_location

    def consume(self, event, *args, **kwargs):

        method = event.environment.get("REQUEST_METHOD", None)
        self.logger.info("Translating REST for {method}".format(method=method), event=event)

        event = getattr(self, "translate_{method}".format(method=method.lower()))(event)
        self.send_event(event)

    def translate_post(self, event):
        status_code = event.status[0]

        if status_code == 200 or status_code == 201:
            event.status = (201, "Created")
            local_url = self.url_post_location or event.environment.get("PATH_INFO", None)
            entity_id = event.get("entity_id", event.meta_id)

            if local_url is not None:
                if "{entity_id}" in local_url:
                    location = local_url.format(entity_id=entity_id)
                else:
                    location = local_url + "/" + entity_id

                event.headers.update({'Location': location})
        else:
            pass

        return event

    def translate_patch(self, event):
        status_code = event.status[0]
        if status_code == 200:
            if event.data is None or event.data == "" or len(event.data) == 0:
                event.status = (204, "No Content")
        else:
            pass

        return event

    def translate_get(self, event):
        return self.translate_patch(event)

    def translate_put(self, event):
        return self.translate_patch(event)

    def translate_delete(self, event):
        return self.translate_patch(event)
