def stream_copy(source, dest):
	while True:
		chunk = source.read(64 * 1024)
		if not chunk:
			break
		dest.write(chunk)
