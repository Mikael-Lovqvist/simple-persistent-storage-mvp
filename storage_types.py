from efforting.mvp4 import type_system as TS
from efforting.mvp4.type_system.bases import public_base


class namespace:
	def __init__(self, *initial_sources, **initial_values):
		data = dict()
		for source in initial_sources:
			if isinstance(source, namespace):
				data.update(tuple(source))
			else:
				data.update(source)

		data.update(initial_values)

		for key, value in data.items():
			assert isinstance(key, str) and key.isidentifier() and not key.startswith('_')

		object.__setattr__(self, '_data', data)

	def __setattr__(self, key, value):
		self._data[key] = value

	def __delattr__(self, key):
		del self._data[key]

	def __getattr__(self, key):
		return self._data[key]

	def __repr__(self):
		inner = ', '.join(f'{k}={v!r}' for k, v in self._data.items())
		return f'{self.__class__.__name__}({inner})'

	def __iter__(self):
		yield from self._data.items()

	def __eq__(self, other):
		return self._data == other._data


class entry(public_base):
	path = TS.positional()
	value = TS.positional()
	created = TS.positional()
	updated = TS.positional()
	accessed = TS.positional()

class immutable_entry(public_base):
	path = TS.positional(read_only=True)
	value = TS.positional(read_only=True)
	created = TS.positional(read_only=True)
	updated = TS.positional(read_only=True)
	accessed = TS.positional(read_only=True)

class meta_entry(public_base):
	path = TS.positional(read_only=True)
	type = TS.positional(read_only=True)
	created = TS.positional(read_only=True)
	updated = TS.positional(read_only=True)
	accessed = TS.positional(read_only=True)
