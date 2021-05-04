import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Set


class BaseVariables(ABC):

    def __init__(self, collection_name, identifier):
        self._collection_name = collection_name
        self._identifier = identifier
        self._variables = None

    @property
    def collection_name(self):
        return self._collection_name

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def variables(self) -> pd.DataFrame:
        """
        :return: all variables for the collection
        """
        self.check_variables_defined()
        return self._variables

    def make_dtypes(self):
        out = {}
        for col in self.static:
            out[col] = 'category'
            out[col.upper()] = 'category'
        for col in self.timeseries:
            out[col] = float if self.get_type(col) == 'double' else str
        return out

    def is_static(self, name: str) -> bool:
        self.check_variables_defined()
        return self.locate_helper(name, 'static')

    def get_type(self, name) -> str:
        self.check_variables_defined()
        return self.locate_helper(name, 'type')

    def get_names(self) -> List:
        self.check_variables_defined()
        return self._variables.index.tolist()

    def locate_helper(self, name, col):
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

    @property
    def static(self) -> List[str]:
        self.check_variables_defined()
        return self._variables[self._variables['static']].index.tolist()

    @property
    def timeseries(self) -> List[str]:
        self.check_variables_defined()
        return self._variables[~self._variables['static']].index.tolist()

    @abstractmethod
    def check_variables_defined(self) -> None:
        if self._variables is None:
            self._variables = pd.read_csv(
                f'/Users/alex/Documents/GitHub/equity-db/equity_db/variables/{self._collection_name}.csv',
                index_col='name')
