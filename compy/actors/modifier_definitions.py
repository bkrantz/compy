from compy.actors.mixins.modifier_definitions import _get_lookup_mixin, _get_xpath_lookup_mixin

__all__ = [
    "StaticModifyDefinition",
    "LookupModifyDefinition",
    "XPathLookupModifyDefinition",
    "StaticDeleteDefinition",
    "XPathLookupDeleteDefinition",
    "LookupItemMapDefinition"
]

class _BaseModifyDefinition(object):
    _valid_output_modes = ["join", "update", "replace", "delete"]
    _max_values = {
        "update": 1,
        "replace": 1,
        "delete": 1
    }

    @property
    def target_scopes(self):
        return [self.target_scope]
    
    @property
    def max_values(self):
        return self._max_values[self.output_mode]
    
    def __init__(self,
            output_mode="update",
            target_scope=['_data'],
            *args,
            **kwargs):
        self.output_mode = self.__get_output_mode(output_mode=output_mode)
        self.target_scope = target_scope

    def __get_output_mode(self, output_mode):
        if output_mode not in self._valid_output_modes:
            raise CompysitionException("Output mode '{output}' not supported for definition of type '{type}'".format(output=output_mode, type=self.__class__.__name__))
        return output_mode

class _BaseAddDefinition(_BaseModifyDefinition):
    _valid_output_modes = ["join", "update", "replace"]

    def __init__(self,
            join_key="aggregated",
            *args,
            **kwargs):
        super(_BaseAddDefinition, self).__init__(*args, **kwargs)
        self.join_key = join_key

class _BaseDeleteDefinition(_BaseModifyDefinition):
    _valid_output_modes = ["delete"]

    def __init__(self,
            output_mode=None,
            *args,
            **kwargs):
        super(_BaseDeleteDefinition, self).__init__(output_mode="delete", *args, **kwargs)

class StaticModifyDefinition(_BaseAddDefinition):
    def __init__(self,
            modify_value,
            *args,
            **kwargs):
        super(StaticModifyDefinition, self).__init__(*args, **kwargs)
        self.modify_value = modify_value

    @property
    def value(self):
        return self.modify_value

    def get_source_values(self, event):
        return [self.modify_value]

class LookupModifyDefinition(_BaseAddDefinition):

    def __init__(self,
            source_scope=["_data"],
            *args,
            **kwargs):
        super(LookupModifyDefinition, self).__init__(*args, **kwargs)
        self.source_scope = source_scope

    @property
    def value(self):
        return self.source_scope

    def get_source_values(self, event):
        return [_get_lookup_mixin().lookup(obj=event, key_chain=self.source_scope)]

class LookupItemMapDefinition(_BaseAddDefinition):
    def __init__(self,
            key_name="key",
            value_name="value",
            source_scope=["_data"],
            *args,
            **kwargs):
        super(LookupItemMapDefinition, self).__init__(*args, **kwargs)
        self.source_scope = source_scope
        self.key_name = key_name
        self.value_name = value_name

    @property
    def value(self):
        return self.source_scope

    def get_source_values(self, event):
        root = _get_lookup_mixin().lookup(obj=event, key_chain=self.source_scope)
        return [ [ { self.key_name: key, self.value_name: value } for key, value in root.iteritems() ] ]

class XPathLookupModifyDefinition(LookupModifyDefinition):
    def get_source_values(self, event):
        return _get_xpath_lookup_mixin().lookup(obj=event, key_chain=self.source_scope)

class StaticDeleteDefinition(_BaseDeleteDefinition):

    def __init__(self,
            key=None,
            *args,
            **kwargs):
        super(StaticDeleteDefinition, self).__init__(*args, **kwargs)
        self.key = key
    
    @property
    def value(self):
        return None

    def get_source_values(self, event):
        return [self.key]

class XPathLookupDeleteDefinition(_BaseDeleteDefinition):

    def __init__(self,
            source_scope=["_data"],
            *args,
            **kwargs):
        super(XPathLookupDeleteDefinition, self).__init__(*args, **kwargs)
        self.source_scope = source_scope
    
    @property
    def value(self):
        return self.source_scope

    def get_source_values(self, event):
        return _get_xpath_lookup_mixin().lookup(obj=event, key_chain=self.source_scope)
