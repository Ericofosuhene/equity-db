import gc
import warnings

import pandas as pd

from datetime import datetime
from functools import partial

from typing import Dict, Generator, List, Tuple
from multiprocessing import Pool, Manager, managers, cpu_count

from equity_db.api.mongo_connection import MongoAPI
from equity_db.variables.base_variables import BaseVariables
from equity_db.write.prep_for_insert import prep_data_for_format_and_insert


class InsertIntoDB:
    """
    a class to insert equity information a mongodb
    """

    def __init__(self, api: MongoAPI):
        """
        user is specifying the connection to the mongo database
        :param api: the connection to use for inserting data
                MUST have specified the collection!
        """
        self.api = api

    def format_and_insert(self, data_path: str) -> None:
        """
        Formats and inserts the given tabular data to a mongo db collection.
        Will automatically detect if a column is static or time series.
        The process the threaded to optimize the formatting ad insertion process.
        Data must be in the format of the output given by ______
        See that function for details on the format and style of the data

        :param data_path: the path to the data to be entered into the mongo database
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

        variables = self.api.get_variables(collection=None, raise_error=True)
        # getting chunks of the identifier and corresponding chunk ranges
        chunks = _chunk_index_dataframe(data_path, cpu_count() * 3, variables.identifier)
        ns = _make_namespace(data_path, variables, self.api.db)

        start = datetime.now()
        with Pool(cpu_count()) as pool:
            func = partial(_parallel_format_insert, ns)
            pool.starmap(func, chunks)
        took = (datetime.now() - start).total_seconds()

        print('Finished formatting and inserting!')
        print(f'Took {int(took / 60)} minutes, {round(took % 60)} seconds')


def _parallel_format_insert(ns: managers.Namespace, skip: int, stop: int) -> None:
    """
    formats and inserts data into a mongo database, the format is described in format_and_insert.
    This function is meant to be called in parallel by the multiprocessing module
    reads in the chunked dataframe, then adjusts types/parses dates,
    then formats the document and inserts the document in batches
    :param ns: the Namespace (needs to be made by _make_namespace)
    :param skip: Index to start reading in the chunk
    :param stop: Index to end reading in the chunk
    :return: None
    """
    # read in the dataframe in the given chunk
    chunked_data = pd.read_csv(ns.data_path, skiprows=range(1, skip - 1), nrows=(stop - skip),
                               dtype=ns.variables.make_dtypes(), header=0)
    chunked_data = prep_data_for_format_and_insert(chunked_data, ns.variables.collection_name, date_format='%Y%m%d')

    # setting up local variables
    var = ns.variables
    partitioned_cols = var.get_static_timeseries_intersection(chunked_data.columns)
    ids = chunked_data.index.unique()
    api = MongoAPI(ns.db, var)

    # formatting and inserting the
    list_of_docs = []
    for asset_id in ids:
        # formatting the document data for a single asset at a time
        data_tick = chunked_data.loc[asset_id]

        # if only one row then the data is likely bad
        if isinstance(data_tick, pd.Series):
            warnings.warn(f'Hit bad data entry {asset_id}')
            continue  # skip the rest

        # turning tabular data into a document
        static_df = data_tick.iloc[0].to_frame().reindex(partitioned_cols['static'])
        ticker_dict = list(static_df.to_dict().values())[0]
        ticker_dict[var.identifier] = asset_id
        ticker_dict['timeseries'] = list(
            data_tick[partitioned_cols['timeseries']].reset_index(drop=True).to_dict('index').values())
        list_of_docs.append(ticker_dict)

        # batch inserting documents into the database
        if len(list_of_docs) == 100:
            _insert_helper(list_of_docs, api)

    # doing last check to ensure there is nothing left over in the documents_to_be_inserted
    if list_of_docs:
        _insert_helper(list_of_docs, api)

    del data_tick
    gc.collect()


def _make_namespace(data_path: str, variables: BaseVariables, db_name) -> managers.Namespace:
    """
    helper to make the namespace for multiprocessing
    :param data_path: the path to the data we are looking to read in the parallel function
    :param variables: the BaseVariables for the collection we are inserting
    :param db_name: the field of the database we are inserting into
    :return: namespace for multiprocessing
    """
    # setting up the namespace
    ns = Manager().Namespace()
    ns.data_path = data_path
    ns.variables = variables
    ns.db = db_name

    return ns


def _chunk_index_dataframe(data_path: str, amount_chunks: int, asset_id_col: str) -> Generator[
        Tuple[int, int], None, None]:
    """
    helper to read in a chunked dataframe by its index, and the corresponding dataframe values for said indexes
    the data's primary key must be ordered for this to work, we only read in the primary key for the data

    :param data_path: the path to the dataframe we want to chunk
    :param amount_chunks: the amount of chunks we want
    :param asset_id_col: the asset identification columns for the dataset
    :return: generator of tuples
            [0] = the first row of the chunk
            [1] = the ending row for the chunk
    """

    # reading in the index to make ranges for reading in the dataframe
    asset_index = pd.read_csv(data_path, usecols=[asset_id_col], dtype={asset_id_col: str})
    asset_index['range_index'] = asset_index.index

    # seeing if index is sorted
    if (asset_index[asset_id_col] >= asset_index[asset_id_col].shift(1)).sum() != asset_index.shape[0] - 1:
        raise ValueError('The index column is not sorted. \n Sort the index and rewrite to the file')

    # aggregating the min and max values of the index
    ranges = asset_index.groupby(asset_id_col).range_index.agg([min, max])
    del asset_index

    chunk_len = int(ranges.shape[0] / amount_chunks) + 1
    for i in range(0, ranges.shape[0], chunk_len):
        chunk = ranges.iloc[i:i + chunk_len]
        yield chunk['min'].min(), chunk['max'].max()


def _insert_helper(list_of_docs: List[Dict], api: MongoAPI) -> None:
    """
    batch inserts the given documents unto the given collection using the given MongoAPI
    :param list_of_docs: A list of documents to be inserted, gets cleared once inserted
    :param api: the mongo connection we use to insert
    :return: None
    """
    api.batch_insert(list_of_docs)
    list_of_docs.clear()
