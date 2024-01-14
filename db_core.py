from . import storage_types, journal_types
from .file_utils import stream_copy
from .data_utils import deep_copy, prepare_for_storage
from .pickling import db_pickler, db_unpickler

from efforting.mvp4 import type_system as TS
from efforting.mvp4.type_system.bases import public_base
from efforting.mvp4.symbols.directory import symbol_node

#TODO - We should also consider having an event system so that we can subscribe to changes to certain paths
#		this might be implemented in a way where we supply a "last known value" of some kind so that we can make sure
#		we don't get a race condition.

#TODO - we should consider a locking mechanism which also should be part of the snapshot.
#		maybe locks have keys (which are not a security measure but rather a method to ensure that intentions are followed properly)

import time, threading, os



class database_core(public_base):
	#TODO - mark these as "private" to make intentions clear
	symbol_tree = TS.factory(symbol_node)
	storage_map = TS.factory(dict)

	#Config
	#TODO - maybe rename journal stuff to make it more clear what is recovery journal and what is archive
	journal_auto_flush_timeout = TS.named(10)
	journal_filename = TS.named('db-journal-recovery.pickle')
	journal_archive_filename = TS.named('db-journal-archive.pickle')
	snapshot_filename = TS.named('db-snapshot.pickle')

	create_snapshot = TS.named(False)

	#State
	pending_log = TS.factory(list)
	journal_auto_flush_timer = TS.state(None)
	write_lock = TS.factory(threading.Lock)
	journal_lock = TS.factory(threading.Lock)

	journal_file = TS.state(None)
	journal_pickler = TS.state(None)

	def recreate_journal_entry(self, entry):
		#TBD: that further subclasses from these may be unintentionally covered here. Since match doesn't support type identity, maybe we should do this with if instead?
		match entry:
			case journal_types.update(time=now, path=path, value=value):
				with self.write_lock:
					symbol = self.symbol_tree.create_symbol(path)
					value_to_store = prepare_for_storage(value)
					if existing := self.storage_map.get(symbol):
						#TBD - should we worry about this? Maybe it should be configurable.
						existing.updated, existing.accessed, existing.value = now, now, value_to_store
					else:
						#TBD - should we worry about this? Maybe it should be configurable.
						new = storage_types.entry(path, value, now, now, now)
						self.storage_map[symbol] = new


			case journal_types.create(time=now, path=path, value=value):
				with self.write_lock:
					symbol = self.symbol_tree.create_symbol(path)
					value_to_store = prepare_for_storage(value)
					new = storage_types.entry(path, value, now, now, now)
					if existing := self.storage_map.get(symbol):
						#TBD - should we worry about this? Maybe it should be configurable.
						pass
					else:
						#Create
						self.storage_map[symbol] = new

			case journal_types.access(time=now, path=path):
				symbol = self.symbol_tree.require_symbol(path)
				meta = self.storage_map[symbol]
				meta.accessed = now

			case _:
				raise TypeError(entry)


	def recover_journal(self):
		with self.journal_lock:
			start_pos = self.journal_file.tell()

			if start_pos:
				self.journal_file.seek(0)	#Rewind

				unpickler = db_unpickler(self.journal_file)
				for entry in unpickler.load():
					self.recreate_journal_entry(entry)
					self.pending_log.append(entry)

				assert start_pos == self.journal_file.tell()

			self.journal_file.seek(0)
			self.journal_file.truncate(0)
			self.journal_pickler = db_pickler(self.journal_file)
			#Note: If we don't recreate the db_pickler we end up in a weird state and get corrupted data, not sure why (maybe a position counter is not reset)
			# Doesn't seem like there is a way to reset it: https://docs.python.org/3/library/pickle.html#pickle.Pickler



	def create_journal_stream(self, filename):
		with self.journal_lock:
			self.journal_file = open(filename, 'ba+')
			self.journal_pickler = db_pickler(self.journal_file)

	def journal(self, entry):
		with self.journal_lock:
			self.pending_log.append(entry)

		if self.journal_auto_flush_timer:
			self.journal_auto_flush_timer.cancel()

		if self.journal_auto_flush_timeout:
			self.journal_auto_flush_timer = threading.Timer(self.journal_auto_flush_timeout, self.flush_journal)
			self.journal_auto_flush_timer.start()

	def flush_journal(self):
		with self.journal_lock:
			to_flush = tuple(self.pending_log)
			self.pending_log.clear()

			self.journal_pickler.dump(to_flush)


	def store(self, path, value):
		with self.write_lock:
			now = time.time()
			symbol = self.symbol_tree.create_symbol(path)
			value_to_store = prepare_for_storage(value)
			if existing := self.storage_map.get(symbol):
				#Update
				self.journal(journal_types.update(now, path, deep_copy(value_to_store), deep_copy(existing.value)))
				existing.updated, existing.accessed, existing.value = now, now, value_to_store
			else:
				#Create
				self.journal(journal_types.create(now, path, value))
				new = storage_types.entry(path, value, now, now, now)
				self.storage_map[symbol] = new

	def load(self, path):
		now = time.time()
		symbol = self.symbol_tree.require_symbol(path)
		meta = self.storage_map[symbol]
		self.journal(journal_types.access(now, path))
		meta.accessed = now
		return deep_copy(meta.value)

	def get(self, path, default=None):
		now = time.time()
		if not (symbol := self.symbol_tree.get_symbol(path)):
			return default
		meta = self.storage_map[symbol]
		self.journal(journal_types.access(now, path))
		meta.accessed = now
		return deep_copy(meta.value)

	#NOTE - loading metadata should not add access to the log unless we also request the value

	def load_meta(self, path, load_value=False):
		now = time.time()
		symbol = self.symbol_tree.require_symbol(path)
		#BUG: THe following is not valid because _from_state doesn't deal with read only properly
		#return immutable_entry._from_state(self.storage_map[symbol].__getstate__())

		meta = self.storage_map[symbol]
		if load_value:
			self.journal(journal_types.access(now, path))
			meta.accessed = now
			info = deep_copy(meta.__getstate__())
			return storage_types.immutable_entry(**info)
		else:
			info = deep_copy(meta.__getstate__())
			info['type'] = type(info.pop('value'))
			return storage_types.meta_entry(**info)

	def get_meta(self, path, default=None, load_value=False):
		now = time.time()
		if not (symbol := self.symbol_tree.get_symbol(path)):
			return default

		#BUG: THe following is not valid because _from_state doesn't deal with read only properly
		#return immutable_entry._from_state(self.storage_map[symbol].__getstate__())
		meta = self.storage_map[symbol]
		if load_value:
			self.journal(journal_types.access(now, path))
			meta.accessed = now
			info = deep_copy(meta.__getstate__())
			return storage_types.immutable_entry(**info)
		else:
			info = deep_copy(meta.__getstate__())
			info['type'] = type(info.pop('value'))
			return storage_types.meta_entry(**info)

	def load_snapshot(self):
		if self.create_snapshot:
			with open(self.snapshot_filename , 'wb'):
				pass

		with open(self.snapshot_filename , 'rb') as snapshot_file:
			snapshot_file.seek(0, os.SEEK_END)
			if snapshot_file.tell():
				snapshot_file.seek(0)
				snapshot_unpickler = db_unpickler(snapshot_file)
				for entry in snapshot_unpickler.load():
					self.storage_map[self.symbol_tree.create_symbol(entry.path)] = entry


	def flush_everything(self):
		with self.journal_lock:
			with self.write_lock:
				with open(self.snapshot_filename, 'wb') as snapshot_file:
					snapshot_pickler = db_pickler(snapshot_file)
					snapshot_pickler.dump(tuple(self.storage_map.values()))

				to_flush = tuple(self.pending_log)

				self.journal_file.seek(0)
				self.journal_file.truncate(0)
				self.journal_pickler = db_pickler(self.journal_file)

				self.journal_pickler.dump(to_flush)

				self.pending_log.clear()


				#TODO - not hardcode
				with open(self.journal_archive_filename, 'ab+') as journal_archive:
					self.journal_file.seek(0)
					stream_copy(self.journal_file, journal_archive)

				self.journal_file.seek(0)
				self.journal_file.truncate(0)
				self.journal_pickler = db_pickler(self.journal_file)

				#Note: If we don't recreate the db_pickler we end up in a weird state and get corrupted data, not sure why (maybe a position counter is not reset)
				# Doesn't seem like there is a way to reset it: https://docs.python.org/3/library/pickle.html#pickle.Pickler




	def terminate(self):
		with self.write_lock:
			if self.journal_auto_flush_timer:
				self.journal_auto_flush_timer.cancel()

		self.flush_everything()

	def __enter__(self):
		self.create_journal_stream(self.journal_filename)
		self.load_snapshot()
		self.recover_journal()
		return self

	def __exit__(self, et, ev, tb):
		self.terminate()
