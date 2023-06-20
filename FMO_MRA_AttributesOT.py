__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = """This script loads MRA attributes (Sole Supplier, In Selection, New Item, New Supplier, Rest of World Solution) 
                     to capture quarterly trends for the aforementioned and is then rolled up in the Item Info Export page in the FMO MRA Dashboard."""
__schedule__ = "2:23 PM PST on day 25 of March, June, September, December"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import date
import pandas as pd
from Helper_Functions import querySQL, uploadDFtoSQL, executeStoredProcedure
from Project_params import params
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main":
    # initialize variables
    source_server = 'sql2812-fm1-in.amr.corp.intel.com,3181'
    source_db = 'MRA'
    dest_table = 'stage.stg_FMO_MRA_Attributes_OT'
    project_name = 'FMO MRA P1276'
    data_area = 'FMO MRA P1276 OT Attributes'

    # generate query (or enter your own query here)
    query = """SELECT a.[Id]
                    ,[IsSoleSupplier]
                    ,[IsInSelection]
                    ,[IsNewItem]
                    ,[IsNewSupplier]
                    ,[IsROWSolution]
                FROM [MRA].[dbo].[RiskAssessments] a
                LEFT OUTER JOIN [dbo].[RiskAssessmentPlatformTechnologies] b ON a.Id = b.RiskAssessment_Id
                WHERE b.PlatformTechnology_Id = 480"""
    query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        df['Load Date'] = date.today()

        insert_succeeded, error_msg = uploadDFtoSQL(dest_table, df, truncate=True, driver="{SQL Server}")  # upload dataframe to SQL
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], dest_table, source_server, params['GSMDW_SERVER']))
        else:
            print(error_msg)

    execute_succeeded, error_msg = executeStoredProcedure('fmo.MRA_Attributes_OT_Load', 'N')
    log(execute_succeeded, project_name='SQL: fmo.MRA_Attributes_OT_Load', data_area=data_area, row_count=1, error_msg=error_msg)
    if execute_succeeded:
        print('Successfully executed the fmo.MRA_Attributes_OT_Load stored procedure')
    else:
        print(error_msg)
