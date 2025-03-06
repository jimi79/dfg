#!/usr/bin/env python3

# we need to find a source for the csv, or a subset of it.
# or use json?
# yeah, maybe json, we don't need that much datas

# count of houses for a commune: https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC?OCS=_T&TDW=1&L_STAY=_T&RP_MEASURE=DWELLINGS&UFS=_T&TIME_PERIOD=2021&GEO=2024-COM-33063
# count of appartments: https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC?OCS=_T&TDW=2&L_STAY=_T&RP_MEASURE=DWELLINGS&UFS=_T&TIME_PERIOD=2021&GEO=2024-COM-33063

# we cannot import the whole dataset as json. More than 1000 "pages", more than 500M
# but we can fetch, and also use a cache, based on the date of the last fetched.
# I mean if there is a big update, we remove everything

import psycopg2
import dotenv
import os
import requests
import json

dotenv.load_dotenv()

class Cache:
    def __init__(self, connection):
        self.connection = connection
# we create the table cache if it's not here
        self.createTable()

    def createTable(self):
        con = self.connection
        cur = con.cursor()
        cur.execute("SELECT * FROM information_schema.tables WHERE table_name = %s", ('cache',))
        if (cur.rowcount == 0):
            print("The table cache isn't here.")
            cur.execute("CREATE TABLE cache(url VARCHAR(200), content TEXT);")
            cur.execute("CREATE UNIQUE INDEX idxCache ON cache(url)")
            con.commit()

    def fetch(self, url):
        con = self.connection
        cur = con.cursor()
        cur.execute("SELECT content FROM cache WHERE url = %s", (url, ))
        if (cur.rowcount == 0):
            req = requests.get(url)
            assert req.status_code == 200
            res = req.text
            cur.execute("INSERT INTO cache(url, content) VALUES (%s, %s)", (url, res))
            con.commit()
        else:
            res = cur.fetchall()[0][0]
        return res

class Logement:
    def __init__(self):
        self.db = psycopg2.connect(dbname = os.getenv('DB_NAME'), user = os.getenv('PG_DB_USER'), password = os.getenv('PG_DB_PWD'), host = 'localhost', port = os.getenv('PG_DB_PORT'))
        self.cache = Cache(self.db)
        self.MAISON = '1'
        self.APPARTEMENT = '2'
        self.TOTAL = '_T'
        self.year = 2021 # we should find a way to know the last one maybe
        self.codeCommuneYear = 2024 

    def getCountHousesPerType(self, codeCommune):
        codeCommuneAPI = "%d-COM-%s" % (self.codeCommuneYear, codeCommune)

        url = "https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC?OCS=_T&L_STAY=_T&TIME_PERIOD=%d&GEO=%s" % (self.year, codeCommuneAPI)

        res = self.cache.fetch(url)
        res = json.loads(res)
        data = { item['dimensions']['TDW']: item['measures']['OBS_VALUE_NIVEAU']['value'] for item in res['observations'] }
        res = {"codegeo": codeCommune, "YEAR": self.year, "LOG": data[self.TOTAL], "MAISON": data[self.MAISON], "APPART": data[self.APPARTEMENT]}
        print(res)


# count of appartments: https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC?OCS=_T&TDW=2&L_STAY=_T&RP_MEASURE=DWELLINGS&UFS=_T&TIME_PERIOD=2021&GEO=2024-COM-33063
        

        
Logement().getCountHousesPerType("33063")
