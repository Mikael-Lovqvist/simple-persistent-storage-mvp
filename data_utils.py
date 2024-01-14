from .storage_types import namespace


def dict_map(function, dct):
	return {function(k): function(v) for k, v in dct.items()}

def dict_items(function, items):
	return {function(k): function(v) for k, v in items}

def prepare_for_storage(item):
	match item:
		case int() | float() | str() | bytes():
			return item

		case tuple() | list() | set() | frozenset():
			return type(item)(map(prepare_for_storage, item))

		case dict():
			return dict_map(prepare_for_storage, item)

		case namespace():
			return namespace(dict_items(prepare_for_storage, item))

		case _ if item is None:
			return item

		case _:
			raise TypeError(item)


def deep_copy(item):
	match item:
		case int() | float() | str() | bytes():
			return item

		case tuple() | list() | set() | frozenset():
			return type(item)(map(deep_copy, item))

		case dict():
			return dict_map(deep_copy, item)

		case namespace():
			return namespace(dict_items(deep_copy, item))

		case _ if item is None:
			return item

		case _:
			raise TypeError(item)


