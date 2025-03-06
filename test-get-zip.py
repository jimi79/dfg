#!/usr/bin/env python3

# https://docs.python.org/3.11/library/index.html

import zipfile
import requests # https://requests.readthedocs.io/en/latest/, pip install requests
from stream_unzip import async_stream_unzip # pip install stream-unzip
import httpx # pip install httpx
import asyncio # pip install asyncio
import csv
import time
import psycopg2
import dotenv
import os

# https://stream-unzip.docs.trade.gov.uk/async-interface/

url = "https://api.insee.fr/melodi/file/DS_RP_LOGEMENT_PRINC/DS_RP_LOGEMENT_PRINC_CSV_FR"

logementTableName = "bronze.logement"


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
					yield chunk

async def build_sql(datas):
	for item in datas.list:
		print(item)

async def createTable(cur, line):
	cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (logementTableName.split('.')[0], logementTableName.split('.')[1],))
	if (cur.rowcount != 0):
		cur.execute("DROP TABLE %s" % (logementTableName))
	sql = "CREATE TABLE %s" % logementTableName
	sql = sql + "(id SERIAL PRIMARY KEY,"

	fields = next(csv.reader([line], delimiter = ';', quotechar = '"'))
	sql = sql + ", ".join(["%s VARCHAR(100)" % field for field in fields])
	sql = sql + ")"
	cur.execute(sql)
	return fields


async def processData(cur, values, fields):
	sql = "INSERT INTO %s " % logementTableName
	v = next(csv.reader([values], delimiter = ';', quotechar = '"'))
	sql = sql + "(%s)" % ', '.join(fields)
	sql = sql + "VALUES (" + "%s, " * (len(v) - 1) + "%s)"
	cur.execute(sql, list(v))
	#print(values)
	

async def get_datas():
	conn = psycopg2.connect(dbname = os.getenv('PG_DB_NAME'), user = os.getenv('PG_DB_USER'), password = os.getenv('PG_DB_PWD'), host = os.getenv('PG_DB_HOST'), port = os.getenv('PG_DB_PORT'))
	cur = conn.cursor()

	firstLine = True
	idLine = 0
	blob = ""
	async for chunk in get_csv():
		blob = blob + chunk.decode('utf-8')
		if "\n" in blob:
			blob = blob.split('\n')
			for line in blob[:-1]: # I don't process the last line, as I'm not sure it's fully here
				if firstLine:
					fields = await createTable(cur, line)
					firstLine = False
				else:
					if line.strip() != '':
						await processData(cur, line, fields)
				if idLine % 1000 == 0:
					print("%d records added" % idLine)
					conn.commit() # apparently in psql, uncommited datas are stored in memory
				idLine = idLine + 1
			blob = blob[-1]

	print("-----------")

	for line in blob.split('\n'):
		if line.strip() != '':
			await processData(cur, line, fields) # the last lines
	conn.commit()

def v2():
	asyncio.run(get_datas())

dotenv.load_dotenv()
v2()
