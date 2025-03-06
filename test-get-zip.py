#!/usr/bin/env python3

# https://docs.python.org/3.11/library/index.html

import zipfile
import requests # https://requests.readthedocs.io/en/latest/, pip install requests
from stream_unzip import async_stream_unzip # pip install stream-unzip
import httpx # pip install httpx
import asyncio # pip install asyncio


# https://stream-unzip.docs.trade.gov.uk/async-interface/

url = "https://api.insee.fr/melodi/file/DS_RP_LOGEMENT_PRINC/DS_RP_LOGEMENT_PRINC_CSV_FR"
url = "https://lorem.jim.netnix.in/james/data.zip" # to not bother insee


def v1():
	print("fetching")
	r = requests.get(url, stream = True)
	print("fetched")
	for content in r.iter_content(1024):
		print(len(content))



async def get_zipped_chunks(client):
	# Iterable that yields the bytes of a zip file
	async with client.stream('GET', url) as r:
		async for chunk in r.aiter_bytes(chunk_size=65536):
			yield chunk

async def get_csv():
	async with httpx.AsyncClient() as client:
		async for file_name, file_size, unzipped_chunks in async_stream_unzip(get_zipped_chunks(client)):
			async for chunk in unzipped_chunks:
				if (file_name.decode('utf8') == 'DS_RP_LOGEMENT_PRINC_data.csv'):
					print(chunk)	

def v2():
	asyncio.run(get_csv())


v2()
