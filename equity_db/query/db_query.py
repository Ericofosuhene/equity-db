import pandas as pd
from typing import List, Optional

from equity_db.api import MongoAPI
from equity_db.db_access import AssetQuery


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

    def get_asset_data(self, assets: List[str], ts_fields: Optional[List[str]] = None,
                       static_fields: Optional[List[str]] = None, start: pd.Timestamp = None,
                       end: pd.Timestamp = None, search_by: str = 'cusip') -> AssetQuery:
        """
        queries the database according to the inputs provided by the user.
        returns a AssetQuery object which the user can use to derive their preferred form of output\

        :param assets: the assets to search the database by
        :param ts_fields: the time series data fields we want to search for in our query
        :param static_fields: the static data fields we want to include in out query
        :param start: the start time frame for the time series data
        :param end: the end time for the time series data
        :param search_by: the field name of the identifier we passed in "assets"
        :return: a AssetQuery object holding the query contents
        """

        if (not ts_fields) and (not static_fields):
            raise ValueError('Either ts_fields or static_fields must be passed')

        if (bool(start) + bool(end) != 2) and ts_fields:
            raise ValueError('Both start and end must be specified if querying time series data')

        # this always needs to be made no matter if there is no static wanted
        static_projection = {field: 1 for field in (static_fields if static_fields else [])}
        static_projection['_id'] = 0
        static_projection[search_by] = 1

        if ts_fields:
            # making the timeseries projection dict
            timeseries_projection = {field: '$timeseries.' + field for field in ts_fields}
            timeseries_projection['date'] = '$timeseries.datadate'

            aggregation_query = [
                {'$match': {search_by: {'$in': assets}}},
                {'$unwind': "$timeseries"},
                {'$match': {'timeseries.datadate': {'$gte': start, '$lt': end}}},
                {'$project': {**static_projection, **timeseries_projection}}
            ]
            primary_key = ['date', search_by]

        else:
            aggregation_query = [{'$match': {search_by: {'$in': assets}}},
                                 {'$project': static_projection}]
            primary_key = [search_by]

        query_results = self.__api.read_from_db_agg('compustat', aggregation_query)
        return AssetQuery(aggregation_query_results=query_results, unique_identifiers=primary_key)
