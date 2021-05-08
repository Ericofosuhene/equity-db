import os

import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Set


class BaseVariables(ABC):
    """
    represents the metadata for a database
    The insertion and reading of data fron database depends on having accurate meta data.
    The metadata is stored in a csv in /equity_db/varaibles/metadata/ the file field is the field of the data base


    Meta data csv must gave columns:
        - field: columns name of the data
        - type: type of the data
        - description: what the data represents
        - static: bool, is the data static

    """

    def __init__(self, collection_name: str, identifier: str):
        """
        :param collection_name: the field of the collection the meta data represents
            MUST have a csv with the same field as the collection_name
        :param identifier: the unique asset identifier
        """
        self._collection_name = collection_name
        self._identifier = identifier
        self._variables = None  # will have a dataframe representing the metadata

    @property
    def collection_name(self) -> str:
        """
        :return: the field of the collection
        """
        return self._collection_name

    @property
    def identifier(self) -> str:
        """
        :return: the unique asset identifier for the dataset
        """
        return self._identifier

    @property
    def variables(self) -> pd.DataFrame:
        """
        :return: all variables for the collection
        """
        self.check_variables_defined()
        return self._variables

    @property
    def static(self) -> List[str]:
        """
        :return: the static variables for the dataset
        """

        self.check_variables_defined()
        return self._variables[self._variables['static']].index.tolist()

    @property
    def timeseries(self) -> List[str]:
        """
        :return: time series variables for the dataset
        """
        self.check_variables_defined()
        return self._variables[~self._variables['static']].index.tolist()

    @property
    def fields(self) -> List[str]:
        """
        :return: The names of all fields in a dataset
        """
        self.check_variables_defined()
        return self._variables.index.tolist()

    def make_dtypes(self) -> Dict[str, any]:
        """
        Makes a dictionary of data types for all fields in the dataset
        This is useful when reading in data using pandas, can compress the data to the correct types
        :return: Dict of column field and the corresponding data type
        """
        out = {}
        for col in self.static:
            out[col] = 'category'
            out[col.upper()] = 'category'
        for col in self.timeseries:
            out[col] = float if self.get_type(col) == 'double' else str
        return out

    def make_dtypes_query(self, fields: Iterable[str]) -> Dict[str, any]:
        """
        makes the dtypes for turning a mongo cursor into a dataframe
        the types are different than make_dtypes because of date handling
        :return: dict with key, value: {column -> type}
        """
        partitioned_cols = self.get_static_timeseries_intersection(fields)
        out = {}
        # static cols
        for col in partitioned_cols['static']:
            out[col] = 'category'

        # time series cols
        for col in partitioned_cols['timeseries']:
            col_type = self.get_type(col)
            if col_type == 'string':
                out[col] = 'category'
            elif col_type == 'double':
                out[col] = 'float64'
            elif col_type == 'date':
                out[col] = 'datetime64[ns]'

        return out

    def is_static(self, field: str) -> bool:
        """
        :param field: The data column to check
        :return: if the given field field is static or not
        """
        self.check_variables_defined()
        return self.locate_helper(field, 'static')

    def get_type(self, field: str) -> str:
        """
        :param field: the field to get the type of
        :return: the type for a given field
        """
        self.check_variables_defined()
        return self.locate_helper(field, 'type')

    def locate_helper(self, field: str, col: str) -> any:
        """
        helper to pull data out of the metadata dataframe
        :param field: the row to get data for
        :param col: the column to get data for
        :raise KeyError: if the row/column combination dosen't exist
        :return: the data in the wanted row/column
        """
        try:
            return self._variables.loc[field, col]
        except KeyError:
            raise KeyError(f'There is no field named "{field}" in the "{self._collection_name}" collection')

    def get_static_timeseries_intersection(self, columns: Iterable[str]) -> Dict[str, Set[str]]:
        """
        partitions the passed columns into  static and timeseries sets
        will ignore if columns is not recognised

        :param columns: the columns we are partitioning
        :return: Dict with two two keys "static" and "timeseries" relating to columns of those types
        """

        out = {
            'static': set(self.static).intersection(columns),
            'timeseries': set(self.timeseries).intersection(columns)
        }
        return out

    def ensures_valid_fields(self, fields: Iterable[str]) -> None:
        """
        ensures all passed fields are valid fields for this collection
        :param fields: the fields we are checking
        :raise ValueError: if there is invalid fields
        :return: None
        """
        disjoint = set(fields) - set(self.fields)
        if disjoint:
            raise ValueError(
                f'The passed fields have contain names that are not contained in the collection: {disjoint}'
            )

    @abstractmethod
    def check_variables_defined(self) -> None:
        """
        checks to see if the variables df has been read.
        If it has not then it reads the dataframe into the object
        :return: None
        """
        if self._variables is None:
            path = os.path.dirname(__file__) + f'/metadata/{self._collection_name}.csv'
            self._variables = pd.read_csv(path, index_col='field')
