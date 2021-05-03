import gc
import os

import pandas as pd

from datetime import datetime
from functools import partial

from typing import Iterable, List, Tuple
from multiprocessing import Pool, Manager, managers, cpu_count

from equity_db.api.mongo_connection import MongoAPI
from equity_db.dispatcher import dispatcher
from equity_db.variables.base_variables import BaseVariables
from equity_db.write.insert_utils import (
    change_types_for_import,
    convert_columns_to_lowercase,
    ensure_good_columns
)


class InsertIntoDB:
    """
    a class to insert equity information a mongodb
    """

    def __init__(self, api: MongoAPI):
        """
        user is specifying the connection to the mongo database
        :param api: the connection to use for inserting data
        """
        self.api = api

    @staticmethod
    def prep_data_for_format_and_insert(data: pd.DataFrame, collection: str, date_format: str) -> pd.DataFrame:
        """
        formats the given data to be inserted into the mongo database

        - makes the index the unique identifier column for the collection
        - Compresses the memory usage by declaring categorical types for static data.
        - Converts all types out of numpy
        - Parses dates
        - Ensures all columns in the given data are valid for the given collection
        - Converts columns to lowercase

        :param data: the dataframe to be prepped
        :param collection: the collection the data is meant to be inserted into
        :param date_format: the format of the dates in the passed data
        :raise ValueError: if a column of the data is not in the collection metadata
        :return: dataframe that is ready to be given to format_and_insert
        """
        variable = dispatcher(collection)

        convert_columns_to_lowercase(data)
        ensure_good_columns(data.columns.tolist(), variable)
        change_types_for_import(data, variable, date_format)

        # checking to see if the index needs to be reset or not
        # assuming a index name of None means a range index is being used
        print('Changing index...')
        if not isinstance(data.index, pd.RangeIndex):
            data.reset_index(inplace=True)
        data.set_index(variable.identifier, inplace=True)

        del variable
        return data

    def format_and_insert(self, data: pd.DataFrame, collection: str) -> None:
        """
        Formats and inserts the given tabular data to a mongo db collection.
        Will automatically detect if a column is static or time series.
        The process the threaded to optimize the formatting ad insertion process.
        Data must be in the format of the output given by ______
        See that function for details on the format and style of the data

        Will Print:

        :param data: the data to be entered into the mongo database
            Must contain "lpermno" as an identifier, must alo contain a "date" column
            Must have range index on dataframe
        :param collection: the collection the data is to be inserted to
            Must have the collection name in the CollectionDispatcher class
        :raise ValueError: if the passed data contains columns that are not present in
            the BaseVariables of the collection
        :return: None


        Insert format:

        {ticker   : "AAPL",
         CUSIP    : "12346",
         static2  : "blah blah",
         timeseries: [
            {date: date_object(2010-01-02),
             close: 120},
            {date: date_object(2010-01-03),
            close: 121}
            ]
         },
        """
        print('Starting insert...')
        # getting chunks of the lpermno and corresponding data
        chunks = _chunk_index_dataframe(data, cpu_count())
        ns = self.make_namespace(collection, data.columns)

        del data
        gc.collect()

        start = datetime.now()
        with Pool(cpu_count()) as pool:
            func = partial(_parallel_format_insert, ns)
            pool.starmap(func, chunks)
        took = (datetime.now() - start).total_seconds()

        # for x in chunks:
        #     _parallel_format_insert(ns, *x)

        print('Finished formatting and inserting!')
        print(f'Took {int(took / 60)} minutes, {took % 60} seconds')

    def make_namespace(self, collection: str, columns: Iterable[str]) -> managers.Namespace:
        """
        helper to make the namespace for multiprocessing
        :param collection: the collection to make namespace for
        :param columns: the columns we are making namespace for
        :return: namespace for multiprocessing
        """
        # getting a BaseVariables for the given collection
        variables: BaseVariables = dispatcher(collection)
        cols_partition = variables.get_static_timeseries_intersection(columns)

        # setting up the namespace
        ns = Manager().Namespace()
        ns.static_cols = cols_partition['static']
        ns.timeseries_cols = cols_partition['timeseries']
        ns.unique_identifier = variables.identifier
        ns.collection = collection
        ns.db = self.api.db

        return ns


def _parallel_format_insert(ns: managers.Namespace, ids: Iterable[any], batch_path: str):
    df = pd.read_csv(batch_path, index_col=ns.unique_identifier)

    list_of_docs = []
    for asset_id in ids:
        # formatting the document data for a single asset at a time
        data_tick = df.loc[asset_id]
        print(asset_id)
        wanted = data_tick.iloc[[0]]
        if not isinstance(wanted, pd.DataFrame):
            print(f'{ns.unique_identifier}: {asset_id} BAAAAAAD')
            UserWarning(f'{ns.unique_identifier}: {asset_id} only has a single row of data')
            continue
        static_df = wanted.reindex(ns.static_cols)

        ticker_dict = list(static_df.to_dict().values())[0]
        ticker_dict[ns.unique_identifier] = asset_id
        ticker_dict['timeseries'] = list(data_tick[ns.timeseries_cols].reset_index().to_dict('index').values())
        list_of_docs.append(ticker_dict)

    MongoAPI(ns.db).batch_insert(ns.collection, list_of_docs)

    del list_of_docs, df
    gc.collect()


def _chunk_index_dataframe(data: pd.DataFrame, amount_chunks: int) -> List[Tuple[List[any], str]]:
    """
    helper to chunk a dataframe by its index, and the corresponding dataframe values for said indexes
    The chunked dataframe is written to a hdf5 file in the directory ~equity_db/write/insert_jobs
    The name of the path to df will be batch_x, x is a int

    :param data: the dataframe to chunk
    :param amount_chunks: the amount of chunks we want
    :return: generator of tuples
            [0] = list of indexes for the chunk
            [1] = string of the full path to the pickled dataframe
    """
    id_list = sorted(data.index.unique())
    data.sort_index(inplace=True)
    chunk_len = -int(-len(id_list) / amount_chunks)  # taking the ceiling

    list_of_chunks = []
    for i in range(0, len(id_list), chunk_len):
        chunk = id_list[i:i + chunk_len]
        batch_path = os.path.dirname(os.path.realpath(__file__)) + f'/insert_jobs/batch_{i / chunk_len}.csv'
        data.loc[chunk[0]:chunk[-1]].to_csv(batch_path)

        yield chunk, batch_path

    del data
    gc.collect()

    return list_of_chunks
