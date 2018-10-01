#!/usr/bin/env python

import re

from .filelogger import FileLogger

class RedactedFileLogger(FileLogger):

    """
    **Redacts sensitive information from file logger based on provided rules**

    Any regular expression groups identified from the provided patterns that are present in the event data, will be replaced
    with 'REDACTED'. This is indended to ensure file based logging does not contain sensitive information (if patterns are configured properly)

    As an example, sending ['<first_name>(.*?)</first_name>'] as the patterns parameter, will replace whatever the group
    (.*?) finds, with REDACTED. The result would be <first_name>REDACTED</first_name> in the file log.

    Parameters:

    - name (str):                       The instance name
    - patterns (list):            List of patterns to replace. It will replace regex groups within the pattern with 'REDACTED'
    - log file config :                   Kwargs from the app logfile configuration.

    """

    #
    def __init__(self, name, patterns, *args, **kwargs):
        super(RedactedFileLogger, self).__init__(name, *args, **kwargs)
        self.patterns = patterns

    def _process_redaction(self, event):
        for pattern in self.patterns:
            event.message = re.sub(pattern, self._redact_message, event.message)
        return event

    @staticmethod
    def _redact_message(match_object):
        return match_object.group(0).replace(match_object.group(1), 'REDACTED')

    def consume(self, event, *args, **kwargs):
        event = self._process_redaction(event)
        self._process_log_entry(event)