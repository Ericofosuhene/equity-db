import gc

import pandas as pd

from typing import List, Optional
from tqdm import tqdm

from api.mongo_connection import MongoAPI


class InsertIntoDB:
    """
    a class to insert compustat finical information a mongo db
    """

    def __init__(self, api: MongoAPI):
        """
        user is specifying the connection to the mongo database
        :param api: the connection to use for inserting data
        """
        self.api = api

    def format_and_insert(self, data: pd.DataFrame, timeseries_cols: Optional[List[str]] = None,
                          static_cols: Optional[List[str]] = None) -> None:
        """
        Takes compustat 2-d data, formats it into a document structure
        then inserts the document into a mongo database under the collection "compustst"
        MUTATES the given df


        :param data: the data to be entered into the mongo database
            MUST CONTAIN columns: 'datadate', 'cusip'
            Must have range index on dataframe
        :param timeseries_cols: the columns in the passed data that contain timeseries data
            if nothing is passed then the default values will be used
        :param static_cols: the columns in the passed data that contain static data
            if nothing is passed then the default values will be used
        :return: None

        Insert format:

        {ticker   : "AAPL",
         CUSIP    : "12346",
         static2  : "blah blah",
         timeseries: [
            {date: date_object(2010-01-02),
             close: 120},
            {date: date_object(2010-01-03),
            close: 121}
            ]
         },
        """
        # seeing weather or not to use default, this is cleaner then setting default param
        if not static_cols:
            static_cols = ['cusip', 'tic', 'cik', 'conm', 'exchg', 'add1', 'spcindcd', 'weburl']
        if not timeseries_cols:
            timeseries_cols = ["div", "ajexdi", "cshoc", "cshtrd", "eps", "prccd", "prchd", "prcld", "prcod"]

        print('Parsing dates')
        data['datadate'] = pd.to_datetime(data['datadate'], format='%Y%m%d')

        print('adjusting indexes of passed frame')
        cusip_list = data['cusip'].unique()
        data.set_index(['cusip', 'datadate'], inplace=True)

        # giving user some info
        print(f'Inserting {len(data)} rows of data for {len(cusip_list)} unique assets')

        documents_to_be_inserted = []
        for ticker in tqdm(cusip_list):  # will print progress bar
            # formatting the document data for a single asset at a time
            data_tick = data.xs(ticker)
            static_df = data_tick.iloc[0].to_frame().reindex(static_cols)

            ticker_dict = list(static_df.to_dict().values())[0]
            ticker_dict['cusip'] = ticker
            ticker_dict['timeseries'] = list(data_tick[timeseries_cols].reset_index().to_dict('index').values())

            documents_to_be_inserted.append(ticker_dict)

            # batch inserting documents into the data base in batches of 20
            if len(documents_to_be_inserted) == 100:
                # batch inserting to the db
                self.api.batch_insert('compustat', documents_to_be_inserted)
                del documents_to_be_inserted
                documents_to_be_inserted = []
                gc.collect()

        # doing last check to ensure there is nothing left over in the documents_to_be_inserted
        if documents_to_be_inserted:
            # batch inserting to the db
            self.api.batch_insert('compustat', documents_to_be_inserted)
