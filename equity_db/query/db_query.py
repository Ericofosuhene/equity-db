import warnings

import pandas as pd

from typing import Dict, List, Set

from equity_db.api.mongo_connection import MongoAPI
from equity_db.dispatcher import dispatcher
from equity_db.query.asset_query import AssetQuery
from equity_db.variables.base_variables import BaseVariables


class ReadDB:
    """
    This class provides functionality to read from the given equity database
    """

    def __init__(self, api: MongoAPI):
        """
        user is specifying the connection to the mongo database
        :param api: the connection to use for inserting data
        """
        self.__api = api

    def get_asset_data(self, collection: str, assets: List[str], id_field: str, fields: List[str],
                       start: pd.Timestamp = None, end: pd.Timestamp = None, date_col: str = 'datadate') -> AssetQuery:
        """
        queries the database according to the inputs provided by the user.
        returns a AssetQuery object which the user can use to derive their preferred form of output\

        :param collection: the collection we are searching
        :param assets: the assets to search the database by
        :param fields: the data fields we want to search for in our query
        :param id_field: the field name of the identifier we passed in "asset"
        :param start: the start time frame for the time series data
        :param end: the end time for the time series data
        :param date_col: the date column to use
        :return: a AssetQuery object holding the query contents
        """
        variables: BaseVariables = dispatcher(collection)
        partitioned_cols: Dict[str, Set[str]] = variables.get_static_timeseries_intersection(fields)

        # if theres time series cols and not start and ed are passed then throw error
        if (bool(start) + bool(end) != 2) and partitioned_cols['timeseries']:
            raise ValueError('Both start and end must be specified if querying time series data')

        # if theres a start or end passed and olny static data in fields then warn the user
        if (bool(start) + bool(end) > 0) and (not partitioned_cols['timeseries']):
            warnings.warn('Date range passed but no timeseries data in the query')

        # this always needs to be made no matter if there is no static wanted
        static_projection = {field: 1 for field in (partitioned_cols['static'] if partitioned_cols['static'] else [])}
        static_projection['_id'] = 0
        static_projection[id_field] = 1

        if partitioned_cols['timeseries']:
            # making the timeseries projection dict
            timeseries_projection = {field: '$timeseries.' + field for field in partitioned_cols['timeseries']}
            timeseries_projection['date'] = f'$timeseries.{date_col}'

            aggregation_query = [
                {'$match': {id_field: {'$in': assets}}},
                {'$unwind': '$timeseries'},
                {'$match': {f'timeseries.{date_col}': {'$gte': start, '$lt': end}}},
                {'$project': {**static_projection, **timeseries_projection}}
            ]
            primary_key = ['date', id_field]

        else:
            aggregation_query = [{'$match': {id_field: {'$in': assets}}},
                                 {'$project': static_projection}]
            primary_key = [id_field]

        query_results = self.__api.read_from_db_agg(collection, aggregation_query)
        return AssetQuery(aggregation_query_results=query_results, unique_identifiers=primary_key, variables=variables)
