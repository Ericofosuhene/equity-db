from __future__ import annotations

import warnings

from asset_query import AssetQuery


class JoinedAssetQuery:
    """
    Class the provides the functionality to join two AssetQuery's
    """

    def __init__(self):
        """
        self.__df is the joined dataframe
        """
        self.__df = None

    @property
    def df(self):
        """
        :raise AttributeError: if there is no saved dataframe
        :return: pd.Dataframe stored in the object
        """
        if self.__df is None:
            raise AttributeError("No dataframe has been set!")

        return self.__df

    def join(self, onto: AssetQuery, other: AssetQuery, save: bool) -> JoinedAssetQuery:
        """
        joins two AssetQuery's onto this one. Will join on the default unique_identifiers. uses pandas join method.

        :param onto: the AssetQuery we are joining other onto
        :param other: the AssetQuery we are joining onto
        :param save: weather or not to call set_save on self and other
        :return: self with the joined data
        """
        if self.__df is not None:
            warnings.warn('Overwriting another joined dataframe!')

        other_df = other.set_save().df if save else other.df
        self_df = onto.set_save().df if save else onto.df
        self.__df = self_df.join(other_df)

        return self
