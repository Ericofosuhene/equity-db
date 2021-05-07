from __future__ import annotations

import gc

import pandas as pd

from typing import List, Optional
from pymongo.cursor import Cursor

from equity_db.variables.base_variables import BaseVariables


class AssetQuery:
    """
    Handles and formats mongo queries for the client.
    Client interacts with this object

    Allows client the option to save the search results
    therefore not having to rerun queries all the time
    This is very helpful when working with jupyter notebooks

    Allows the api to not force the client to read query results into memory right away.

    provides functionality for turning data into data frame. and cache query results
    """

    def __init__(self, aggregation_query_results: Cursor, unique_identifiers: List[str],
                 variables: Optional[BaseVariables] = None, save: bool = False):
        """
        :param aggregation_query_results: the results of a aggregation query on the database
                MUST HAVE A CUSIP and DATE FIELD in the given query. 'cusip', 'date'
        :param unique_identifiers: the primary key/keys for the given aggregation_query_results
        :param variables: the BaseVaraibles class for the query
        :param save: weather or not to save the aggregation_query_results or exaust them when used
            if true the .df will use a copy of aggregation_query_results therefore not exhausting the object
        """
        self.__aggregation_query_results = aggregation_query_results
        self.__unique_identifiers = unique_identifiers
        self.__variables = variables
        self.__save = save
        self.__saved_df = None  # holds a copy of the query data in the dataframe if __save is True

    @property
    def df(self) -> pd.DataFrame:
        """
        Turns the aggregation query into a pandas dataframe
        :return: data frame
                columns are the parameters for the query index is a
                index is date, cusip
        """
        if self.__save:
            if self.__saved_df is None:
                self.__saved_df = self.make_df_helper()
            return self.__saved_df.copy()

        return self.make_df_helper()

    @property
    def aggregation_query_results(self) -> Cursor:
        """
        If save is False then a mutable Cursor will be returned
        if Save is False then the real cursor will be returned
        :return: returns the raw cursor for the query
        """
        return self.__aggregation_query_results

    @property
    def unique_identifiers(self) -> List[str]:
        """
        :return: the identifiers for the query data
        """
        return self.__unique_identifiers

    def set_save(self, save: bool = True) -> AssetQuery:
        """
        provides the functionality of saving the query and never exhausting the original copy
        :param save: bool weather or not to save the query so we dont have to keep re scanning the database
        :return: self, with the save indicator set to true
        """
        if not save:
            del self.__saved_df
            gc.collect()

        self.__save = save
        return self

    def make_df_helper(self):
        """
        helper to make a dataframe from the mongo cursor
        :return: Dataframe of the mongo cursor
        """
        try:
            # catch a KeyError from setting indexes on a empty dataframe
            query_df = pd.DataFrame(self.aggregation_query_results)
            return query_df.astype(self.__variables.make_dtypes_query(query_df.columns))\
                .set_index(self.__unique_identifiers)

        except KeyError as e:
            print(e)
            raise ValueError('The query contents have already been exhausted or the query has returned no results')
