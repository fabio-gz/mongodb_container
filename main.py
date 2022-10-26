from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
user = os.environ.get("MONGODB_USR")

connection_string = f"mongodb://{user}:{password}@localhost/"

client = MongoClient(connection_string)

dbs = client.list_database_names()
prueba_db = client.prueba
collections = prueba_db.list_collection_names()
#print(collections)

def insert_test_docs():
    """insert one document
    """
    collection = prueba_db.prueba
    test_document = {
        "name": "fabio",
        "type": "test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

production = client.production
person_collection = production.person_collection

def create_documents():
    """create multiple documents on production collection
    """
    names = ['fabio', 'robert', 'leia', 'han']
    last_names = ['gomez', 'downie', 'organa', 'solo']
    ages = [30, 50, 20, 22]

    docs = []

    for name, last_name, age in zip(names, last_names, ages):
        doc = {"name": name, "last_name": last_name, "age": age}
        docs.append(doc)

    person_collection.insert_many(docs)
    #db.production.find()

#create_documents()

printer = pprint.PrettyPrinter()

def find_all_people():
    """print all docs
    """
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

#ind_all_people()

def find_fabio():
    fabio = person_collection.find_one({'name' : 'fabio'})
    printer.pprint(fabio)

#find_fabio()

def count_all_people():
    count = person_collection.count_documents(filter={})
    print('number of people', count)

#count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

#get_person_by_id("6358957f7b8362fc06cb61be")

def get_age_range(min_age, max_age):
    query = {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]}
    
    people = person_collection.find(query).sort("age")

    for person in people:
        printer.pprint(person)

#get_age_range(20,35)

def project_columns():
    columns = {"_id": 0, "name": 1, "last_name": 1}
    people = person_collection.find({}, columns)

    for person in people:
        printer.pprint(person)

#project_columns()

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"name": "namee", "last_name": "last"}
    # }
    # person_collection.update_one({"_id": _id}, all_updates)

    #remove one field
    person_collection.update_one({"_id": _id},{"$unset": {"new_field": ""}})

#update_person_by_id("6358957f7b8362fc06cb61bc")

def replace_one(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new name",
        "last_name": "new last",
        "age": 100
    }
    person_collection.replace_one({"_id": _id}, new_doc)

#replace_one("6358957f7b8362fc06cb61bc")

def delete_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})

#delete_by_id("6358957f7b8362fc06cb61bc")


#----RELATIONS-------

address = {
    "_id": "6358957f7b8459fc06cb63bt",
    "street": "noah st",
    "number": 798,
    "city": "London",
    "country": "UK",
    "zip": 5705
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet": {'addresses': address}})

#add_address_embed("6358957f7b8362fc06cb61bc", address)

def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)

#add_address_relationship("6358957f7b8362fc06cb61bf", address)