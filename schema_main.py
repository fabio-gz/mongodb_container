from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from datetime import datetime as dt

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
user = os.environ.get("MONGODB_USR")

connection_string = f"mongodb://{user}:{password}@localhost/"

client = MongoClient(connection_string)
dbs = client.list_database_names()
production = client.production

printer = pprint.PrettyPrinter()

def create_book_collection():
    #schema validation
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required" : ["title", "authors", "publish_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be an string and required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be an objectid and required"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be date, required"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "only one of the options"
                },
                "copies" :{
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "integer greater than 0"
                }
            }
        }
    }

    try:
        production.create_collection("book")
    except Exception as e:
        print(e)

    #collMod command to add options to collections
    production.command("collMod", "book", validator=book_validator)


def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "string and required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "string and required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "date and required"
                }
            }
        }
    }

    try:
        production.create_collection("author")
    except Exception as e:
        print(e)

    production.command("collMod", "author", validator=author_validator)

def create_data():
    authors = [
        {
            "first_name": "Fabio",
            "last_name": "Gomez",
            "date_of_birth": dt(1989,4,6)
        },
        {
            "first_name": "George",
            "last_name": "Lukas",
            "date_of_birth": dt(1953,3,6)
        },
        {
            "first_name": "Luke",
            "last_name": "Strike",
            "date_of_birth": dt(1976,4,4)
        },
        {
            "first_name": "Mon",
            "last_name": "Luf",
            "date_of_birth": dt(1948,9,1)
        },
    ]
    author_collection = production.author
    authors = author_collection.insert_many(authors).inserted_ids

    

    books = [
        {
            "title": "Spark for beginners",
            "authors": [authors[0]],
            "publish_date": dt.today(),
            "type": "Non-Fiction",
            "copies": 1 
        },
        {
            "title": "Python for beginners",
            "authors": [authors[0]],
            "publish_date": dt.today(),
            "type": "Non-Fiction",
            "copies": 1 
        },
        {
            "title": "Clone wars",
            "authors": [authors[1]],
            "publish_date": dt.today(),
            "type": "Fiction",
            "copies": 200
        },
        {
            "title": "New hope",
            "authors": [authors[1], authors[2]],
            "publish_date": dt.today(),
            "type": "Fiction",
            "copies": 900
        }
    ]

    book_collection = production.book
    book_collection.insert_many(books)

#create_author_collection()
#create_book_collection()
#create_data()

# books_containing_a = production.book.find({"title": {"$regex": "a{1}"}})
# printer.pprint (list(books_containing_a))

# authors_and_books = production.author.aggregate([{
#     "$lookup":{
#         "from": "book",
#         "localField": "_id",
#         "foreignField": "authors",
#         "as": "books"
#     }
# }])

# printer.pprint(list(authors_and_books))


# authors_book_count = production.author.aggregate([{
#     "$lookup":{
#         "from": "book",
#         "localField": "_id",
#         "foreignField": "authors",
#         "as": "books"
#     },
# },
# {
#     "$addFields": {
#         "total_books": {"$size": "$books"}
#     }
# },
# {
#     "$project": {"first_name":1, "last_name": 1, "total_books":1, "_id":0}
# }
# ])
# printer.pprint(list(authors_book_count))


books_with_old_authors = production.book.aggregate([
    {
        "$lookup": {
            "from": "author",
            "localField": "authors",
            "foreignField": "_id",
            "as": "authors"
        }
    },
    {
        "$set": {
            "authors": {
                "$map": {
                    "input": "$authors",
                    "in": {
                        "age": {
                            "$dateDiff":{
                                "startDate": "$$this.date_of_birth",
                                "endDate": "$$NOW",
                                "unit": "year"
                            }
                        },
                        "first_name": "$$this.first_name",
                        "last_name": "$$this.last_name"
                    }
                }
            }
        }
    },
    {
        "$match": {
            "$and": [
                {"authors.age": {"$gte": 50}},
                {"authors.age": {"$lte": 150}}
            ]
        }
    },
    {
        "$sort": {
            "age": 1
        }
    }
])
printer.pprint(list(books_with_old_authors))