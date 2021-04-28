from typing import Dict, List

from pymongo import MongoClient
from pymongo.cursor import Cursor


class MongoAPI:
    """
    inserts and reads with a connection to a mongodb database
    """

    def __init__(self, db: str):
        """
        initializes the MongoAPI object
        :param db: the name of the database to connect to
        """
        self.__client: MongoClient = MongoClient()[db]

    def batch_insert(self, collection: str, insert_me: List[Dict]) -> None:
        """
        inserts a list of documents into the specified collection of the desired mongo database.

        :param collection: the collection to insert to .
        :param insert_me: the document to be inserted into the database.
        :return: None
        """
        self.__client[collection].insert_many(insert_me)

    def read_from_db_agg(self, collection: str, query: List[Dict[str, any]]) -> Cursor:
        """
        passes a given aggregation query to the find method of pymongo
        :param collection: the collection to search
        :param query: the query to search the db for
        :return: the results of the query as a pymongo.cursor.Cursor
        """
        return self.__client[collection].aggregate(query)

