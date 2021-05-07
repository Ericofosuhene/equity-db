import warnings

import pandas as pd
import numpy as np

from typing import List
from tqdm import tqdm

from equity_db.dispatcher import dispatcher
from equity_db.variables.base_variables import BaseVariables


def prep_data_for_format_and_insert(data: pd.DataFrame, collection: str, date_format: str) -> pd.DataFrame:
    """
    formats the given data to be inserted into the mongo database
    Does:
        - makes columns names lowercase
        - ensures columns are valid for the database
        - adjusts types/pares dates for the dataframe
        - makes the index the asset identifier

    :param data: the dataframe to be prepped
    :param collection: the collection the data is meant to be inserted into
    :param date_format: the format of the dates in the passed data
    :raise ValueError: if a column of the data is not in the collection metadata
    :return: dataframe that is ready to be inserted into a mongo database
    """
    variable = dispatcher(collection)

    _convert_columns_to_lowercase(data)
    _ensure_good_columns(data.columns.tolist(), variable)
    _change_types_for_import(data, variable, date_format)

    # checking to see if the index needs to be reset or not
    if not isinstance(data.index, pd.RangeIndex):
        data.reset_index(inplace=True)
    data.set_index(variable.identifier, inplace=True)

    return data


def _convert_columns_to_lowercase(data: pd.DataFrame) -> None:
    """
    Mutates the given DataFrame columns to all be lowecase
    :param data: frame to mutate
    :return: None
    """
    data.columns = [x.lower() for x in data.columns]


def _ensure_good_columns(columns: List[str], variable: BaseVariables) -> None:
    """
    ensures all columns in the given list are in the specified collection
    :param columns: the columns we are checking
    :param variable: the base variables for the corresponding collection
    :raise ValueError: if a bad column is found
    :return: None
    """
    disjoint = set(columns).difference(set(variable.get_names()))
    if disjoint:
        raise ValueError(f'The passed data has columns that are not contained in the collection: {disjoint}')


def _change_types_for_import(data: pd.DataFrame, variable: BaseVariables, date_format: str) -> pd.DataFrame:
    """
    Mutates the dataframe to the correct types for a write to mongo
    timeseries data:
        - get changed to floats, pd.Timestamps, or objects (strings)
    static data:
        - get changed to categorical pd.Timestamps, or objects (strings)
        - all static floats get casted to ints and then casted to string

    :param data: the dataframe we are changing the types of
    :param variable: the collection variable we are prepping for
    :param date_format: format of the date strings
    :return: the mutated dataframe
    """

    def adjustor_helper(adjustment_dict, data_type: str):
        print(f'Adjusting {data_type} data...')

        for col in tqdm(partitioned_cols[data_type]):
            field_type = variable.get_type(col)

            try:
                data[col] = adjustment_dict[field_type](col)
            except KeyError:
                raise ValueError(f'{data_type} data type "{field_type}" is not recognised')
            except ValueError as e:
                warnings.warn(f'ERROR on col {col}\n' + str(e))

    partitioned_cols = variable.get_static_timeseries_intersection(data.columns)

    # below are the conversions for data types
    # theres a bug in pymongo with Nat and pandas so must convert to numpy date type
    static_conversion = {
        'string': lambda x: data[x].astype("category") if data[x].dtype.name != 'category' else data[x],
        'double': lambda x: data[x].astype(str).astype('category') if data[x].dtype.name != 'category' else data[x],
        'date': lambda x: pd.to_datetime(data[x].tolist(), format=date_format).astype(object).to_numpy(
            dtype=np.datetime64)
    }

    timeseries_conversion = {
        'string': lambda x: data[x].astype(str) if data[x].dtype != str else data[x],
        'double': lambda x: data[x].astype(float) if data[x].dtype != float else data[x],
        'date': lambda x: pd.to_datetime(data[x].tolist(), format=date_format).astype(object).to_numpy(
            dtype=np.datetime64)
    }

    # adjusting the data
    adjustor_helper(static_conversion, 'static')
    adjustor_helper(timeseries_conversion, 'timeseries')

    return data
