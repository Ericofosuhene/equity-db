import os

import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Set


class BaseVariables(ABC):
    """
    represents the metadata for a database
    The insertion and reading of data fron database depends on having accurate meta data.
    The metadata is stored in a csv in /equity_db/varaibles/metadata/ the file name is the name of the data base


    Meta data csv must gave columns:
        - name: name of the field
        - type: type of the data
        - description: what the data represents
        - static: bool, is the data static

    """

    def __init__(self, collection_name: str, identifier: str):
        """
        :param collection_name: the name of the collection the meta data represents
            MUST have a csv with the same name as the collection_name
        :param identifier: the unique asset identifier
        """
        self._collection_name = collection_name
        self._identifier = identifier
        self._variables = None  # will have a dataframe representing the metadata

    @property
    def collection_name(self) -> str:
        """
        :return: the name of the collection
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

    def make_dtypes(self) -> Dict[str, any]:
        """
        Makes a dictionary of data types for all fields in the dataset
        This is useful when reading in data using pandas, can compress the data to the correct types
        :return: Dict of column name and the coresponding data tyoe
        """
        out = {}
        for col in self.static:
            out[col] = 'category'
            out[col.upper()] = 'category'
        for col in self.timeseries:
            out[col] = float if self.get_type(col) == 'double' else str
        return out

    def is_static(self, name: str) -> bool:
        """
        :param name: The data column to check
        :return: if the given field name is static or not
        """
        self.check_variables_defined()
        return self.locate_helper(name, 'static')

    def get_type(self, name: str) -> str:
        """
        :param name: the field to get the type of
        :return: the type for a given field
        """
        self.check_variables_defined()
        return self.locate_helper(name, 'type')

    def get_names(self) -> List[str]:
        """
        :return: The names of all fields in a dataset
        """
        self.check_variables_defined()
        return self._variables.index.tolist()

    def locate_helper(self, name: str, col: str) -> any:
        """
        helper to pull data out of the metadata dataframe
        :param name: the row to get data for
        :param col: the column to get data for
        :raise KeyError: if the row/column combination dosen't exist
        :return: the data in the wanted row/column
        """
        try:
            return self._variables.loc[name, col]
        except KeyError:
            raise KeyError(f'There is no variable named "{name}" in the "{self._collection_name}" collection')

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

    @abstractmethod
    def check_variables_defined(self) -> None:
        """
        checks to see if the variables df has been read.
        If it has not then it reads the dataframe into the object
        :return: None
        """
        if self._variables is None:
            path = os.path.dirname(__file__) + f'/metadata/{self._collection_name}.csv'
            self._variables = pd.read_csv(path, index_col='name')
