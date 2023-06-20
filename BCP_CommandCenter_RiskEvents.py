__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script loads the Risk Events Sharepoint list to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "6x Daily at 8:00 AM, 12:00 AM, 4:00 PM, 8:00 PM, 12:00 PM, 4:00 PM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
from datetime import date, datetime
import pandas as pd
from Project_params import params
from Helper_Functions import loadSharePointList, uploadDFtoSQL, executeStoredProcedure
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    params['EMAIL_ERROR_RECEIVER'].extend(['wayne.chen@intel.com'])
    project_name = 'BCP Command Center'
    data_area = 'Command Center Risk Events'
    sp_site = 'https://intel.sharepoint.com/sites/GSCRiskManagement/GSCBusinessContinuity/CommandCenter'
    list_name = 'Risk Events'
    # start_time = time()

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False)
    pd.set_option('display.max_columns', None)

    if len(df.index) == 0:  # DataFrame is empty:
        log(False, project_name=project_name, data_area=data_area, row_count=0, error_msg="File Does Not Exist / Moved")
        exit(1)

    df = df[['Title', 'Problem Statement', 'Risk Type', 'Current Status Summa', 'Inherent Risk', 'Residual Risk',
          'Risk Trend', 'EMWT Lead (If_', 'EMWT GSC Lead/', 'EMWT Other Points', 'Customer Exposure/Im', 'Supplier Exposure/Im',
          'Intel/Other Exposure', 'Engagement/Communications_', 'If yes, provid', 'Engagement/Communications_0', 'If yes, provid0',
          'Current Mitigation A', 'Time to Recover', 'Strategic Mitigation', 'Trigger Points', 'Milestone #1 D0', 'Milestone #1',
          'Milestone #1 P0', 'Milestone #1 S', 'Milestone #2 D0', 'Milestone #2', 'Milestone #2 P0', 'Milestone #2 S', 'Milestone #3 D0',
          'Milestone #3', 'Milestone #3 P0', 'Milestone #3 S', 'Milestone #4 D0', 'Milestone #4', 'Milestone #4 P0', 'Milestone #4 S',
          'Current Status Revis', 'Potential Help Neede', 'Monitoring Frequency', 'Date Reviewed in', 'ID', 'Modified', 'Created']]

    df['Load Date'] = date.today()
    # print(df)

    # Convert UTC datetimes to proper format
    date_columns = ['Milestone #1 D0', 'Milestone #2 D0', 'Milestone #3 D0', 'Milestone #4 D0', 'Date Reviewed in', 'Modified', 'Created']

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce').dt.date
    # print(df)

    insert_succeeded, error_msg = uploadDFtoSQL(table='stage.stg_BCP_WR_Risk_Events', data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")  # overwrite the global SQL Driver parameter
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
        error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {} rows into stage.stg_BCP_WR_Risk_Events'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
    else:
        print(error_msg)

    # Execute bcp.Load_WR_Risk_Events Stored Proc
    execute_succeeded, error_msg = executeStoredProcedure('bcp.Load_WR_Risk_Events', 'N')
    if execute_succeeded:
        print('Successfully executed the bcp.Load_WR_Risk_Events stored procedure')
    else:
        print(error_msg)
    log(execute_succeeded, project_name=project_name, data_area='Command Center Risk Events Stored Proc', row_count=1,
        error_msg=error_msg)

    # print("--- %s seconds ---" % (time() - start_time))