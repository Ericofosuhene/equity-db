from equity_db.variables.base_variables import BaseVariables
from equity_db.variables.compusat_variables import CompustatVariables
from equity_db.variables.crsp_variables import CRSPVariables


def dispatcher(collection: str) -> BaseVariables:
    """
    gets the correct BaseVariables object for a the given collection
    :param collection: the collection to get the variables class for
    :raise ValueError: of the given collection is not defined in the function
    :return: the variables class for the p[passed collection
    """

    # add collection here to be recognised for writing and querying
    collection_varaibles = {
        'crsp': CRSPVariables(),
        'compustat': CompustatVariables(),
    }

    try:
        return collection_varaibles[collection]
    except KeyError:
        raise ValueError(
            f'The collection "{collection}" is not identifiable\n '
            f'Valid collection names are {list(collection_varaibles.keys())}'
        )

