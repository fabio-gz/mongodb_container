from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb://fabio:{password}@localhost/"

client = MongoClient(connection_string)

dbs = client.list_database_names()
prueba_db = client.prueba
collections = prueba_db.list_collection_names()
#print(collections)

