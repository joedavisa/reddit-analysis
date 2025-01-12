import zstandard
import os
import re
import json
import sys
import csv
from datetime import datetime
import logging.handlers


log = logging.getLogger("bot")
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
	sub_name = sub_name.replace('_comments.zstnew','')
	sub_name = sub_name.replace('_submissions.zstnew','')
	with open(file_name, 'rb') as file_handle, open(sub_name + '_submission_ids.csv', 'w', newline='', encoding="utf-8") as csv_submissions:
		buffer = ''
		re_flairs = re.compile('psyonix|bungie$|season|news|official|announcement|resource|patch notes', re.IGNORECASE)
		
		#stardew
		re_title = re.compile('is out|released|patch|update note|release note', re.IGNORECASE)

		re_links_main = re.compile('nomanssky.com|ea.com|bungie.net|bungie.com|pubg.com|stardewvalley.net|rockstargames.com|rocketleague.com|steamcommunity.com/games',re.IGNORECASE)
		re_links_sub = re.compile('',re.IGNORECASE)

		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		writer = csv.writer(csv_submissions, quoting=csv.QUOTE_MINIMAL)
		writer.writerow([sub_name])
		writer.writerow(['id(name)', 'title', 'permalink'])

		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				# 1514764801 - utc for 2018 Jan
				# 1688169599 - utc for 2023 end of Jun
				obj = json.loads(line)

				num_comments = int(obj['num_comments'])
				if num_comments > 20:
					row_written = False
					created = int(obj['created_utc'])
					flair = str(obj['link_flair_text'])
					flair = flair.lower().strip()
					flair_list = flair.split(sep='//')
					if 'name' in obj:
						link_id = str(obj['name'])
					elif 'id' in obj:
						link_id = str(obj['id'])
					else:
						link_id = ''

					
					#score = str(obj['score']) # negatively received posts won't have a high score :(

					#link flair
					if len(flair_list) == 1:
						if re_flairs.match(flair):
							writer.writerow([link_id, str(obj['title']), str(obj['permalink'])])
							row_written = True
					elif re_flairs.match(flair_list[0]):
						writer.writerow([link_id, str(obj['title']), str(obj['permalink'])])
						row_written = True

					#link to dev website
					if row_written == False:
						selftext = str(obj['selftext'])
						url = str(obj['url'])
						if selftext != "" and re_links_main.match(selftext):
							writer.writerow([link_id, str(obj['title']), str(obj['permalink'])])
							row_written = True
						if url != "" and re_links_main.match(url):
							writer.writerow([link_id, str(obj['title']), str(obj['permalink'])])
							row_written = True

					#regex in title
					if row_written == False:
						if re_title.match( str(obj['title'])):
							writer.writerow([link_id, str(obj['title']), str(obj['permalink'])])
							row_written = True

					yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()


if __name__ == "__main__":
	file_path = sys.argv[1]
	file_size = os.stat(file_path).st_size
	file_lines = 0
	file_bytes_processed = 0
	created = None
	field = "subreddit"
	value = "DestinyTheGame"
	bad_lines = 0
	# try:
	for line, file_bytes_processed in read_lines_zst(file_path):
		try:
			obj = json.loads(line)
			created = datetime.fromtimestamp(int(obj['created_utc']))
			temp = obj[field] == value
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1
		file_lines += 1
		if file_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

	# except Exception as err:
	# 	log.info(err)

	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")

