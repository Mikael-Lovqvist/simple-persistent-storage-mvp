from . import storage_types, journal_types

import pickle

class db_pickler(pickle.Pickler):

	def reducer_override(self, item):
		match item:
			case journal_types.base():
				return (db_unpickler.recreate_journal_entry, (item.__class__, item.__getstate__()))
			case storage_types.namespace():
				return (db_unpickler.recreate_namespace, (tuple(item),))
			case _:
				return NotImplemented

class db_unpickler(pickle.Unpickler):
	def recreate_namespace(parameters):
		return storage_types.namespace(parameters)

	def recreate_journal_entry(entry_type, state):
		return entry_type(**state)

