from abc import ABC

from .base_variables import BaseVariables


class CRSPVariables(BaseVariables, ABC):
    """
    holds all possible variables in the CRSP database
    """

    def __init__(self):
        """
        the field of the database is "crsp" the asset identifier is "lpermno"
        """
        super().__init__('crsp', 'lpermno')

    def check_variables_defined(self) -> None:
        super().check_variables_defined()
