from compy.actors.database import _Database, _DatabaseAuto
from compy.actors.mixins.database import _XMLDatabaseMixin, _JSONDatabaseMixin
from compy.actors.mixins.mysql import _MySQLMixin, _MySQLAutoMixin, _MySQLInsertMixin, _MySQLWriteMixin

__all__ = [
	"XMLMySQLActor",
	"XMLMySQLInsertActor",
	"XMLMySQLWriteActor",
	"XMLMySQLSelectActor",
	"JSONMySQLActor",
	"JSONMySQLInsertActor",
	"JSONMySQLWriteActor",
	"JSONMySQLSelectActor"
]

class XMLMySQLActor(_XMLDatabaseMixin, _MySQLMixin, _Database):
	pass
class XMLMySQLInsertActor(_XMLDatabaseMixin, _MySQLMixin, _MySQLInsertMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class XMLMySQLWriteActor(_XMLDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class XMLMySQLWriteActor(_XMLDatabaseMixin, _MySQLMixin, _MySQLSelectMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass

class JSONMySQLActor(_JSONDatabaseMixin, _MySQLMixin, _Database):
	pass
class JSONMySQLInsertActor(_JSONDatabaseMixin, _MySQLMixin, _MySQLInsertMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class JSONMySQLWriteActor(_JSONDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class JSONMySQLSelectActor(_JSONDatabaseMixin, _MySQLMixin, _MySQLSelectMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
