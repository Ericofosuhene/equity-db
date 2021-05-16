from __future__ import annotations

import gc

import pandas as pd
import pandas_market_calendars as mcal

from typing import List, Optional
from pymongo.cursor import Cursor

from ..variables.base_variables import BaseVariables


class AssetQuery:
    """
    Handles and formats mongo queries for the client.

    Allows client the option to save the search results, therefore not having to rerun queries all the time
    Allows for calendar references, user can specify a market calendar and our query will be
    reindexed for that market calendar

    Allows the api to not force the client to read query results into memory right away.
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
        self.__variables: BaseVariables = variables
        self.__save: bool = save
        self.__saved_df: Optional[pd.DataFrame] = None  # holds a copy of the query in the dataframe if __save is True
        self.__calender: Optional[str] = None  # holds the calendar name we wish to filter by, default is no calendar

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
                self.__saved_df = self.__calender_adjustment(self.__make_df_helper())
            return self.__saved_df.copy()

        return self.__calender_adjustment(self.__make_df_helper())

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

    def set_calendar(self, calender: str) -> AssetQuery:
        """
        sets the desired calendar we wish to filter query results by
        :param calender: the name of the calender we wish to filter by
                Can be: "365" -> every day in a year
                        anything in mcal.get_calendar_names()
        :return: self
        """
        valid_cal: List[str] = ['365'] + mcal.get_calendar_names()
        if calender in valid_cal:
            self.__calender = calender
        else:
            raise ValueError(f'Calender name {calender} is not valid! \nValid Calenders are: {valid_cal}')

        return self

    def __make_df_helper(self):
        """
        helper to make a dataframe from the mongo cursor
        :return: Dataframe of the mongo cursor
        """
        try:
            # catch a KeyError from setting indexes on a empty dataframe
            query_df = pd.DataFrame(self.aggregation_query_results)
            return query_df.astype(self.__variables.make_dtypes_query(query_df.columns)) \
                .set_index(self.__unique_identifiers)

        except KeyError:
            raise ValueError('The query contents have already been exhausted or the query has returned no results')

    def __calender_adjustment(self, query_df: pd.DataFrame) -> pd.DataFrame:
        """
        Reindexs the given dataframe to the schedule specified by the user in set_schedule
        if set_schedule was never called then there will be no reindexing done
        :param query_df: the df we are reindexing
        :return:dataframe with the correct dates for the provided schedule
        """
        # no adjustment wanted
        if self.__calender is None:
            return query_df

        # must do some input checks to ensure we have a valid dataframe to join a calender onto
        # ensuring we have a MultiIndex
        if len(self.__unique_identifiers) != 2:
            raise ValueError('Must have a MultiIndex of length two to preform calendar adjustments')
        # ensuring we have a date field in the MultiIndex
        if 'date' not in self.__unique_identifiers:
            raise ValueError('Can not do calender adjustments if there is no level named "date" in the MultiIndex')

        calender: pd.DatetimeIndex = self.__fetch_calender(query_df)

        level_to_unstack = self.__unique_identifiers[1] if self.__unique_identifiers.index('date') == 0 else \
            self.__unique_identifiers[0]
        return query_df.unstack(level_to_unstack).reindex(calender.values).stack(dropna=False)

    def __fetch_calender(self, query_df: pd.Dataframe) -> pd.DatetimeIndex:
        """
        fetches the correct calendar for whats specified in self.__calender
        :param query_df: the df we are getting the calender for
        :return: a pandas DatetimeIndex of the desired calendar
        """
        start_date = query_df.index.get_level_values('date').min()
        end_date = query_df.index.get_level_values('date').max()

        if self.__calender == '365':
            return pd.date_range(start=start_date, end=end_date, freq='D')

        return mcal.get_calendar(self.__calender).valid_days(start_date=start_date, end_date=end_date)
