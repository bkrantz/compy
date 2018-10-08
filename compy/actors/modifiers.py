from compy.actor import Actor
from compy.actors.mixins.event import BasicEventModifyMixin, JSONEventModifyMixin, XMLEventModifyMixin, LookupMixin, XPathLookupMixin
from compy.actors.mixins.modifiers import _ModifyMixin
from compy.errors import MalformedEventData, CompysitionException
from compy.event import JSONEvent, XMLEvent

__all__ = [
    "BasicEventUpdater",
    "XMLEventUpdater",
    "JSONEventUpdater"
]

class _BaseEventModifier(_ModifyMixin, Actor):
    _internal_error_message = "Invalid modification"

    def __init__(self,
            name,
            modify_definitions=[],
            *args,
            **kwargs):
        super(_BaseEventModifier, self).__init__(name, *args, **kwargs)
        self.modify_definitions = modify_definitions

    def consume(self, event, *args, **kwargs):
        for definition in self.modify_definitions:
            source_values = definition.get_source_values(event=event)
            try:
                source_values[definition.max_values]
            except (KeyError, IndexError):
                try:
                    source_values[0]
                except IndexError:
                    self.logger.error("Unable to identify source value at event.{key}".format(key='.'.join(definition.target_scope)), event=event)
                    raise CompysitionException(self._internal_error_message)
            else:
                self.logger.error("Ambiguous source value {value}, unable to make changes to event.{key}".format(key='.'.join(definition.target_scope), value=definition.value), event=event)
                raise CompysitionException(self._internal_error_message)
            try:
                join_key = getattr(definition, "join_key", None)
                for source_value in source_values:
                    event = getattr(self, self._output_funcs[definition.output_mode])(event=event, content=source_value, key_chain=definition.target_scope[:], join_key=join_key)
            except Exception as err:
                raise MalformedEventData(err)
        self.send_event(event)

class XMLEventUpdater(XMLEventModifyMixin, _BaseEventModifier):
    input = XMLEvent

class JSONEventUpdater(JSONEventModifyMixin, _BaseEventModifier):
    input = JSONEvent

class BasicEventUpdater(JSONEventModifyMixin, _BaseEventModifier):
    pass