#Experimental service endpoint
#TODO - separate example from classes and interfaces defined here
#		add method of configuration
#		implement and test automatic flushing of journal

from .db_core import database_core
from .pickling import db_pickler, db_unpickler

from efforting.mvp4 import type_system as TS
from efforting.mvp4.type_system.bases import public_base

from collections.abc import Iterator
import socketserver
import time, threading

class session_lock(public_base):
	path = TS.positional()
	session = TS.positional()


current_sessions = set()
lock_by_path = dict()
lock_by_session = dict()
lock_guard = threading.Lock()


#TODO - do not allow non identifier names nor names that begins with underscore to simplify interfaces

class query_handler(socketserver.StreamRequestHandler):

	def handle(self):
		current_sessions.add(self)
		lock_by_session[self] = set()

		unpickler = db_unpickler(self.rfile)
		pickler = db_pickler(self.wfile)

		def read():
			return unpickler.load()

		def write(value):
			pickler.dump(value)

		while True:
			try:
				command, *args = read()
			except EOFError:
				break
			try:
				result = command_set[command](self, *args)
			except Exception as e:
				import io, traceback
				formatted_exception = io.StringIO()
				traceback.print_exception(e, file=formatted_exception)
				write(('failure', command, formatted_exception.getvalue()))
			else:
				if isinstance(result, Iterator):
					start_time = time.monotonic()
					for sub_item in result:
						write(('partial', command, sub_item))
					stop_time = time.monotonic()
					duration = stop_time - start_time
					write(('response', command, duration))
				else:
					write(('response', command, result))

		current_sessions.discard(self)
		with lock_guard:
			for lock in lock_by_session.pop(self, ()):
				lock_by_path.pop(lock.path, None)


class query_server(socketserver.ThreadingTCPServer):
	allow_reuse_address = True


with database_core(journal_auto_flush_timeout=None) as db:


	def cmd_get_db_info(session):
		return dict(
			stored_object_count=len(db.storage_map),
			sessions_count=len(current_sessions),
			lock_count=len(lock_by_path),
		)

	def cmd_iter_all_object_paths(session, chunk_size=None):
		assert chunk_size is None, 'Not supported yet'
		paths = tuple(o.path for o in db.storage_map.values())
		return iter((paths,))

	def cmd_require(session, path):
		return db.load(path)

	def cmd_get(session, path, default=None):
		return db.get(path, default)

	def cmd_set(session, path, value):	#TODO - offer commands that require path to be free
		check_write_lock(session, path)
		db.store(path, value)

	def cmd_get_meta(session, path, default=None, load_value=False):
		return db.get_meta(path, default, load_value)


	def cmd_session_lock(session, path):
		with lock_guard:
			assert path not in lock_by_path
			lock = lock_by_path[path] = session_lock(path, session)
			lock_by_session[session].add(lock)

	def cmd_session_unlock(session, path):
		with lock_guard:
			lock = lock_by_path.pop(path)
			lock_by_session[session].discard(lock)

	def check_write_lock(session, path):
		if lock := lock_by_path.get(path):
			match lock:
				case session_lock():
					assert session is lock.session

	command_set = dict(
		get_db_info = cmd_get_db_info,
		iter_all_object_paths = cmd_iter_all_object_paths,
		require = cmd_require,
		get = cmd_get,
		set = cmd_set,
		session_lock = cmd_session_lock,
		session_unlock = cmd_session_unlock,
		get_meta = cmd_get_meta,
	)

	with query_server(('0.0.0.0', 55201), query_handler) as server:
		server.serve_forever()
