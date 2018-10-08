from compy.actors.mixins.event import LookupMixin, XPathLookupMixin

_lookup_mixin = None
_xpath_lookup_mixin = None

def _get_lookup_mixin():
    global _lookup_mixin
    if _lookup_mixin is None:
        _lookup_mixin = LookupMixin()
    return _lookup_mixin

def _get_xpath_lookup_mixin():
    global _xpath_lookup_mixin
    if _xpath_lookup_mixin is None:
        _xpath_lookup_mixin = XPathLookupMixin()
    return _xpath_lookup_mixin