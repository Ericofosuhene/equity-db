import pandas as pd

from equity_db.api import MongoAPI
from equity_db.db_access import InsertIntoDB

data = pd.read_csv('/Users/alex/Downloads/pullout/DS4300_data_2010.csv')

string = ['cusip', 'tic', 'cik', 'conm', 'exchg', 'add1', 'spcindcd', 'weburl']
integer = ["div", "ajexdi", "cshoc", "cshtrd", "eps", "prccd", "prchd", "prcld", "prcod"]

for col in string:
    data[col] = data[col].astype(str)

for col in integer:
    data[col] = data[col].astype(float)

api = MongoAPI('DS4300')

insert_object = InsertIntoDB(api)

# testing the insert into the db
insert_object.format_and_insert(
    data,
    static_cols=['cusip', 'tic', 'cik', 'conm', 'exchg', 'add1', 'spcindcd', 'weburl'],
    timeseries_cols=['div', 'ajexdi', 'cshoc', 'cshtrd', 'eps', 'prccd', 'prchd', 'prcld', 'prcod']
)
