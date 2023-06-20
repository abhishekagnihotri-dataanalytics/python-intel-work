__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Tool list from EMS to GSCDW DB"
__schedule__ = "Once Daily at 4AM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from time import time
from Helper_Functions import uploadDFtoSQL, queryOdata, executeSQL, executeStoredProcedure
from Logging import log
from Password import accounts

from Project_params import params
#import os
#import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path

# remove the current file's parent directory from sys.path
#try:
#    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
#except ValueError:  # Already removed
#    pass



# Initialize variables
table = 'Stage.EMSTools'
project_name = 'FabConsolidation'
data_area = 'EMS Tool Inventory'
LoadBy = 'EMS-Odata'
dest_server = 'sql2943-fm1-in.amr.corp.intel.com,3181'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

# Query OData feed
odata_url = 'https://ems.intel.com/api/v2/Tools'  # '?$top=10'  # include this for testing to limit OData query to only first 10 rows
query_succeeded, data, error_msg = queryOdata(odata_url, username=r'AMR\sys_tmebiadm', password=accounts['TMEBI Admin'].password)
if not query_succeeded:
    log(query_succeeded, project_name=project_name, data_area='EMS Tool List', error_msg='Unable to access OData API. Error: {0}'.format(error_msg))
else:
    df = pd.DataFrame(data['value'])  # convert list inside dictionary to DataFrame

    df = df.drop(['asset','material','site','demo'], axis=1, errors='ignore')

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = LoadBy

    insert_succeeded, error_msg = uploadDFtoSQL(table, df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area='EMS Tool Data', row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
        table_name = '[Base].[EMSTools]'
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Base].[EMSTools]")
            Insert_query = """insert into [GSCDW].[Base].[EMSTools]
            SELECT *  FROM [GSCDW].[Stage].[EMSTools]"""
            insert_succeeded, error_msg = executeSQL(Insert_query)
            if insert_succeeded:
                print("Successfully copied data from staging to base table")
            else:
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)

            table_name = '[Stage].[EMSTools]'
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
            if truncate_succeeded:
                print("Successfully truncated table [Stage].[EMSTools]")
            else:
                log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        print(error_msg)


