import zstandard
import os
import csv
import json
import sys
from datetime import datetime
import logging.handlers


log = logging.getLogger("util")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	
	sub_name = file_name
	# sub_name = sub_name.replace('_comments','')
	# sub_name = sub_name.replace('_submissions','')
	sub_name = sub_name.replace('.zstnew','')
	sub_name = sub_name.replace('.csv','')

	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")
			current_date = datetime.fromtimestamp(1514764799).date()
			count = 0
			for line in lines[:-1]:
				# 1514764801 - utc for 2018 Jan
				# 1688169599 - utc for 2023 end of Jun
				obj = json.loads(line)
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()
  
