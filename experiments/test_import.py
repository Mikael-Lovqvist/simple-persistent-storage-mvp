from efforting.persistent_storage2.service_client import remote_db_interface
from efforting.persistent_storage2 import storage_types



def db_print_stat(db, path):
	from datetime import datetime
	print(f'Path: {path}')
	if meta := db.get_meta(path):

		print(meta.__class__)

		if isinstance(meta, storage_types.meta_entry):
			print(f' - Type: {meta.type}')
		print(f' - Created: {datetime.fromtimestamp(meta.created)}')
		print(f' - Modified: {datetime.fromtimestamp(meta.updated)}')
		print(f' - Accessed: {datetime.fromtimestamp(meta.accessed)}')
		if isinstance(meta, storage_types.immutable_entry):
			print(f' - Value: {meta.value!r}')

	else:
		print(f' - NO OBJECT')


with remote_db_interface('localhost', 55201) as db:
	#db.set('path.to.thing', 'Blaaaargh!')
	db_print_stat(db, 'path.to.thing')
