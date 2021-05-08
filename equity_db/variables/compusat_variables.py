from abc import ABC

from equity_db.variables.base_variables import BaseVariables


class CompustatVariables(BaseVariables, ABC):
    """
    holds all possible variables in the compustat database
    """

    def __init__(self):
        """
        the field of the database is "compustat" and the asset identifier is the "lpermno"
        """
        super().__init__('compustat', 'lpermno')

    def check_variables_defined(self) -> None:
        super().check_variables_defined()
