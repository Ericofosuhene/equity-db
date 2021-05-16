import warnings

import pandas as pd

from typing import Dict, List, Optional, Set, Tuple, Union

from ..api.mongo_connection import MongoAPI
from ..query.asset_query import AssetQuery
from ..variables.base_variables import BaseVariables


class ReadDB:
    """
    This class provides functionality to read from the given equity database
    """

    def __init__(self, api: MongoAPI):
        """
        user is specifying the connection to the mongo database
        :param api: the connection to use for reading data
        """
        self.__api = api

    def get_asset_data(self, assets: List[str], search_by: str, fields: List[str],
                       collection: Union[Optional[BaseVariables], str] = None, start: pd.Timestamp = None,
                       end: pd.Timestamp = None, date_col: str = 'datadate') -> AssetQuery:
        """
        queries the database according to the inputs provided by the user.
        returns a AssetQuery object which the user can use to derive their preferred form of output

        :param assets: the assets to search the database by
        :param fields: the data fields we want to search for in our query
        :param search_by: the field field of the identifier we passed in "asset"
        :param collection: the collection we are searching can be a name or BaseVariable,
                        if self.__api has a collection specified then this is not necessary
                        However you can override the collection in self.__api by passing a collection
        :param start: the start time frame for the time series data
        :param end: the end time for the time series data
        :param date_col: the date column to use
        :raise ValueError: if the passed fields are not valid,
                           if start and end arent passed when getting time series data
        :return: a AssetQuery object holding the query contents
        """
        variables: BaseVariables = self.__api.get_variables(collection, True)
        variables.ensures_valid_fields(fields + [search_by])
        partitioned_cols: Dict[str, Set[str]] = variables.get_static_timeseries_intersection(fields)

        # if theres time series cols and not start and ed are passed then throw error
        if (bool(start) + bool(end) != 2) and partitioned_cols['timeseries']:
            raise ValueError('Both start and end must be specified if querying time series data')

        # if theres a start or end passed and olny static data in fields then warn the user
        if (bool(start) + bool(end) > 0) and (not partitioned_cols['timeseries']):
            warnings.warn('Date range passed but no timeseries data in the query')

        aggregation_query, primary_key = _make_aggregation_query(assets, search_by, partitioned_cols, date_col, start,
                                                                 end)

        query_results = self.__api.read_from_db_agg(aggregation_query, variables)
        return AssetQuery(aggregation_query_results=query_results, unique_identifiers=primary_key, variables=variables)


def _make_aggregation_query(assets: List[str], search_by: str, partitioned_cols: Dict[str, Set[str]], date_col: str,
                            start: pd.Timestamp = None, end: pd.Timestamp = None) -> Tuple[
    List[Dict[str, any]], List[str]]:
    """
    constructs a MQL query searching "assets" for "feilds" optionally over time period "start" to "end"
    :param assets: the assets to search the database by
    :param search_by: the field field of the identifier we passed in "asset"
    :param partitioned_cols: the partitioned fields we are searching for
                must be made by BaseVariables.get_static_timeseries_intersection
    :param start: the start time frame for the time series data
    :param end: the end time for the time series data
    :param date_col: the date column to use
    :return: a Mongo Query to search the database
    """

    # this always needs to be made no matter if there is no static wanted
    static_projection = {field: 1 for field in (partitioned_cols['static'] if partitioned_cols['static'] else [])}
    static_projection['_id'] = 0
    static_projection[search_by] = 1

    if partitioned_cols['timeseries']:
        # making the timeseries projection dict
        timeseries_projection = {field: '$timeseries.' + field for field in partitioned_cols['timeseries']}
        timeseries_projection['date'] = f'$timeseries.{date_col}'

        aggregation_query = [
            {'$match': {search_by: {'$in': assets}}},
            {'$unwind': '$timeseries'},
            {'$match': {f'timeseries.{date_col}': {'$gte': start, '$lt': end}}},
            {'$project': {**static_projection, **timeseries_projection}}
        ]
        primary_key = ['date', search_by]

    else:
        aggregation_query = [{'$match': {search_by: {'$in': assets}}},
                             {'$project': static_projection}]
        primary_key = [search_by]

    return aggregation_query, primary_key
