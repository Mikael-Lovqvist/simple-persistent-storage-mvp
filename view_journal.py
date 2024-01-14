from .pickling import db_unpickler

def view_journal(filename):
	with open(filename, 'rb') as infile:
		while True:
			unpickler = db_unpickler(infile)
			try:
				entries = unpickler.load()
			except EOFError:
				break

			print('---= Section =---')
			for entry in entries:
				print('JOURNAL', repr(entry))

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser(description='View journal file')
	parser.add_argument('filename', type=str, help='Filename of the journal')
	args = parser.parse_args()

	view_journal(args.filename)
