from typing import Dict, List, Optional, Union

from pymongo import MongoClient
from pymongo.cursor import Cursor

from equity_db.dispatcher import dispatcher
from equity_db.variables.base_variables import BaseVariables


class MongoAPI:
    """
    inserts and reads with a connection to a mongodb database
    """

    def __init__(self, db: str, collection: Optional[Union[BaseVariables, str]] = None):
        """
        initializes the MongoAPI object
        :param db: the field of the database to connect to
        """
        self.__client: MongoClient = MongoClient()[db]
        self.__db: str = db
        self.__collection: Optional[BaseVariables] = None
        self.__collection = self.get_variables(collection, False)

    @property
    def db(self):
        """
        :return: the database this object is connected too
        """
        return self.__db

    def get_variables(self, collection: Optional[Union[BaseVariables, str]] = None,
                      raise_error: bool = True) -> BaseVariables:
        """
        makes th correct BaseVariables class to be used
        :param collection: the collection we want to evaluate
        :param raise_error: should we raise an error if we cant find a valid collection,
                if invalid string will always raise_error
        :return: BaseVariables if a collection can be identified else will toss error depending on raise_error
        """
        if (collection is None) and (self.__collection is not None):
            return self.__collection
        if isinstance(collection, BaseVariables):
            return collection
        if isinstance(collection, str):
            return dispatcher(collection)
        if raise_error:
            raise ValueError('Must pass a collection!')

    def copy(self):
        """
        :return: a new deep copy of this MongoAPI object
        """
        return MongoAPI(self.__db)

    def batch_insert(self, insert_me: List[Dict], collection: Optional[Union[BaseVariables, str]] = None) -> None:
        """
        inserts a list of documents into the specified collection of the desired mongo database.

        :param collection: the collection to insert to .
        :param insert_me: the document to be inserted into the database.
        :return: None
        """
        collection = self.get_variables(collection, True)
        self.__client[collection.collection_name].insert_many(insert_me)

    def read_from_db_agg(self, query: List[Dict[str, any]],
                         collection: Optional[Union[BaseVariables, str]] = None) -> Cursor:
        """
        passes a given aggregation query to the find method of pymongo
        :param collection: the collection to search
        :param query: the query to search the db for
        :return: the results of the query as a pymongo.cursor.Cursor
        """
        collection = self.get_variables(collection, True)
        return self.__client[collection.collection_name].aggregate(query)
