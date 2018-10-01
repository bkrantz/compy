#!/usr/bin/env python

import jsonschema
import json

from jsonschema import FormatChecker, ValidationError, SchemaError

from compy.actor import Actor
from compy.event import JSONEvent
from compy.errors import MalformedEventData

def _required(validator, required, instance, schema):
    """Validate 'required' properties.

        This validator adds the index of the required field to the schema_path, which allows us to
        look at error.schema and determine which required field is missing

        If we had a good way to get which required fields failed validation, this wouldn't be necessary.
        See: https://github.com/Julian/jsonschema/issues/119
    """
    if not validator.is_type(instance, 'object'):
        return

    for index, requirement in enumerate(required):
        if requirement not in instance:
            error = ValidationError(
                '{0!r} is a required property'.format(requirement)
            )
            error.schema_path.append(index)
            yield error


class JSONValidator(Actor):
    input = JSONEvent
    output = JSONEvent

    '''**A simple module which applies a provided jsonschema to an incoming event JSON data**

    Parameters:

        name (str):
            | The instance name.
        schema (str):
            | The schema (jsonschema) to validate the incoming json against

    '''

    def __init__(self, name, schema=None, *args, **kwargs):
        super(JSONValidator, self).__init__(name, *args, **kwargs)
        self.schema = schema
        if self.schema:
            formatter = self._build_formatter()
            Validator = jsonschema.validators.extend(
                validator=jsonschema.Draft4Validator,
                validators={
                    'required': _required
                }
            )

            try:
                if isinstance(self.schema, str):
                    self.schema = json.loads(self.schema)

                if isinstance(self.schema, dict):
                    self.schema = Validator(self.schema, format_checker=formatter)
                else:
                    raise ValueError("Schema must be of type str or dict. Instead received type '{type}'".format(type=type(self.schema)))
            except Exception as err:
                self.logger.error("Invalid schema: {err}".format(err=err))
                self.schema = None

    def consume(self, event, *args, **kwargs):
        try:
            if self.schema:
                self.schema.validate(event.data)

            self.logger.info("Incoming JSON successfully validated", event=event)
            self.send_event(event)

        except (SchemaError, ValidationError):
            message = self.format_error_response(self.schema.iter_errors(event.data))
            self.process_error(message, event)


    def format_error_response(self, errors):
        error_reasons = []
        for error in errors:
            err_message = ""
            path = map(str, list(error.path))
            if len(path) > 0:
                err_message = ": ".join(path)

            err_message += " " + error.message
            error_reasons.append(err_message)
        return error_reasons

    @staticmethod
    def _build_formatter():
        """Create a formatter to be passed to the validator. This method can be subclassed to add custom formatters ie:

        formatter = FormatChecker()
        @formatter.checks('my-custom-format', raises=ValidationError)
         def my_custom_format(instance):
            if matches_my_custom_format(instance):
                return True
            else:
                return False

        more info: https://python-jsonschema.readthedocs.io/en/latest/validate/#validating-formats
         """
        return FormatChecker()

    def process_error(self, message, event):
        self.logger.error("Error validating incoming JSON: {0}".format(message), event=event)
        raise MalformedEventData(message)
