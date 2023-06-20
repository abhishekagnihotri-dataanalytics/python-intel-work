__author__ = ""
__email__ = ""
__description__ = "Loads Odata to sql and executes a stored procedure"
__schedule__ = "6:30 AM | 11:30 AM | 3:30 PM PST daily"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from time import time
from Helper_Functions import uploadDFtoSQL, queryOdata, executeStoredProcedure
from Logging import log
from Password import accounts

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def determineColumnLength(df:pd.DataFrame):
    # determine max length of characters in column
    for col1 in df.columns:
        temp = df[col1].map(lambda x: len(x) if isinstance(x, str) else len(str(x)) if isinstance(x, int) else None).max()
        print("Column: {0}, dtype: {1}, max length: {2}".format(col1, df[col1].dtype, temp))
        try:
            print(min(df[col1]))
            print(max(df[col1]))
        except TypeError:
            continue

    print(df.columns)


if __name__ == "__main__":
    start_time = time()

    # Initialize variables
    table = 'dbo.CommercialWorkBench'
    project_name = 'Commercial WorkBench'

    # Query OData feed
    odata_url = 'https://cwbapi.app.intel.com/odata/v1/csidemandmetrics'  # '?$top=10'  # include this for testing to limit OData query to only first 10 rows
    query_succeeded, data, error_msg = queryOdata(odata_url, username=r'AMR\sys_tmebiadm', password=accounts['TMEBI Admin'].password)
    if not query_succeeded:
        log(query_succeeded, project_name=project_name, data_area='CSI Demand Metrics', error_msg='Unable to access OData API. Error: {0}'.format(error_msg))
    else:
        df = pd.DataFrame(data['value'])  # convert list inside dictionary to DataFrame

        # Convert datetime columns from string to datetime objects
        date_cols = ['oaStartDate', 'oaEndDate', 'createdAt', 'updatedAt', 'validationDate', 'rfqInitiatedDate',
                     'rfqPendingAcceptanceDate', 'completedDate', 'inactiveDate', 'priority1Date']
        for col in date_cols:
            df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ') if isinstance(x, str) and '.' in x
                                    else datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ') if isinstance(x, str)
                                    else x)

        # determineColumnLength(df)

        insert_succeeded, error_msg = uploadDFtoSQL(table, df, categorical=['mmNumber', 'supplierId'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area='CSI Demand Metrics', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
        else:
            print(error_msg)

        proc_name = '[dbo].[LoadCommercialWorkBenchAgedBucketsOT]'

        execute_succeeded, error_msg = executeStoredProcedure(proc_name)
        if execute_succeeded:
            print('Successfully executed the {} stored procedure'.format(proc_name))
        else:
            print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
