from compy.actors.mysql import _MySQLMixin
from compy.actors.database import _Database, _DatabaseMixin
from compy.mixins.auth import _AuthDatabaseMixin, _BasicAuthDatabaseMixin

__all__ = [
    "MySQLBasicAuth"
]

class _AuthDatabase(_DatabaseMixin, _Database):
    def __init__(self, name, param_scope=None, output_mode="ignore", expected_results=1, *args, **kwargs):
        super(_AuthDatabase, self).__init__(name=name, param_scope=param_scope, *args, **kwargs)
        self.output_mode = "ignore"
        self.expected_results = 1

class MySQLBasicAuth(_BasicAuthDatabaseMixin, _AuthDatabaseMixin, _MySQLMixin, _AuthDatabase):
    pass