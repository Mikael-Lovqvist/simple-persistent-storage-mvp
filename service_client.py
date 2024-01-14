from .pickling import db_pickler, db_unpickler

from efforting.mvp4.rudimentary_types.data_path import data_path

from socketserver import StreamRequestHandler
import threading, queue
import socket

SENTINEL = object()



class pythonic_path:
	def __init__(self, *path_spec):
		self._path = data_path(*path_spec)

	def __getattr__(self, key):
		return self.__class__(self._path / key)

	def __repr__(self):
		return f'{self.__class__.__name__}({self._path})'

	def __str__(self):
		return str(self._path)


class interface:
	#TODO: Make a lot of stuff "private", like read/write
	def __init__(self, sock):
		self.socket = sock
		self.stream = StreamRequestHandler(sock, None, None)
		self.stream.setup()
		self.to_request_queue = queue.Queue()
		self.outstanding_queries = dict()
		self.all_finished = threading.Event()

		self.unpickler = db_unpickler(self.stream.rfile)
		self.pickler = db_pickler(self.stream.wfile)


	def get(self, path, default=None):
		return self.query('get', str(path), default).require_response()

	def require(self, path):
		return self.query('require', str(path)).require_response()

	def set(self, path, value):
		self.query('set', str(path), value).require_response()

	def get_meta(self, path, default=None, load_value=False):
		return self.query('get_meta', str(path), default, load_value).require_response()


	def query(self, command, *arguments):
		assert command not in self.outstanding_queries
		q = query(command, arguments)
		self.outstanding_queries[command] = q
		self.to_request_queue.put(q)
		return q

	def read(self):
		return self.unpickler.load()

	def write(self, value):
		self.pickler.dump(value)


	def start(self):
		self.query_response_thread = threading.Thread(target=self.handle_query_responses, daemon=True)
		self.query_response_thread.start()

		self.query_request_thread = threading.Thread(target=self.handle_query_requests, daemon=True)
		self.query_request_thread.start()


	def handle_query_responses(self):
		try:
			while True:
				state, command, arguments = self.read()

				if state == 'response':
					self.outstanding_queries.pop(command).emit_response(arguments)
				elif state == 'failure':
					self.outstanding_queries.pop(command).emit_failure(arguments)
				elif state == 'partial':
					self.outstanding_queries[command].emit_partial(arguments)
				else:
					raise Exception((state, command, arguments))

		except:	#Exceptions here will not be caught because it happens within this thread. We should deal with this better.
			self.to_request_queue.put(SENTINEL)
			raise


	def handle_query_requests(self):
		while True:
			if (q := self.to_request_queue.get()) is SENTINEL:
				break

			self.write((q.command, *q.arguments))

	def exhaust(self):
		self.all_finished.wait()

class query:
	def __init__(self, command, arguments):
		self.command = command
		self.arguments = arguments
		self.finished = threading.Event()
		self.partial = queue.Queue()
		self.success = False

	def emit_response(self, response):
		self.response = response
		self.partial.put(SENTINEL)
		self.success = True
		self.finished.set()

	def emit_failure(self, failure):
		self.response = failure
		self.partial.put(SENTINEL)
		self.success = False
		self.finished.set()

	def emit_partial(self, response):
		self.partial.put(response)

	def wait(self):
		self.finished.wait()
		return self.success, self.response

	def require_response(self):
		self.finished.wait()
		if self.success:
			return self.response
		else:
			import textwrap
			TAB = '\t'
			raise Exception(f'Remote error:\n\n{textwrap.indent(self.response, TAB)}')


class remote_db_interface:
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.socket = None

	def __enter__(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM).__enter__()
		self.socket.connect(('localhost', 55201))
		self.remote = interface(self.socket)
		self.remote.start()
		return self.remote

	def __exit__(self, et, ev, tb):
		self.socket.__exit__(et, ev, tb)	#Closes socket which causes self.remote to terminate too


