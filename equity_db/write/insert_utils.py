import pandas as pd

from dask import dataframe as dd
from typing import List

from tqdm import tqdm

from equity_db.variables.base_variables import BaseVariables


def convert_columns_to_lowercase(data: dd) -> None:
    """
    Mutates the given DataFrame columns to all be lowecase
    :param data: frame to mutate
    :return: None
    """
    data.columns = [x.lower() for x in data.columns]


def ensure_good_columns(columns: List[str], variable: BaseVariables) -> None:
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


def change_types_for_import(data: dd, variable: BaseVariables, date_format: str) -> dd:
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
                print(f'ERROR on col {col}\n' + str(e))

    partitioned_cols = variable.get_static_timeseries_intersection(data.columns)

    static_conversion = {
        'string': lambda x: data[x].astype("category") if data[x].dtype.name != 'category' else data[x],
        'double': lambda x: data[x].astype(str).astype('category') if data[x].dtype.name != 'category' else data[x],
        # 'date': lambda x: pd.to_datetime(data[x], format=date_format, errors='ignore'),
        'date': lambda x: data[x].astype("category") if data[x].dtype.name != 'category' else data[x],
    }

    timeseries_conversion = {
        'string': lambda x: data[x].astype(str) if data[x].dtype != str else data[x],
        'double': lambda x: data[x].astype(float) if data[x].dtype != float else data[x],
        # 'date': lambda x: pd.to_datetime(data[x], format=date_format, errors='raise'),
        'date': lambda x: data[x].astype("category") if data[x].dtype.name != 'category' else data[x],
    }

    # adjusting the data
    adjustor_helper(static_conversion, 'static')
    adjustor_helper(timeseries_conversion, 'timeseries')

    return data
