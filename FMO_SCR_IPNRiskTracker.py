__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = """This script is managed by the DMO team to create an exempt list (flag) in the GSM_FMO_SCR model which 
                     will remove an IPN / Site pair from being falsely flagged as high risk. There is logic downstream to 
                     re-add the IPNs back if the business has not verified its continued status as exempt every 90 days.
                     Loads FMO IPN Risk Tracker.xlsx from Shared Drive to SQL by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181
                     """
__schedule__ = "7:27 AM PST daily"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
from datetime import datetime
import pandas as pd
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


project_name = 'FMO Supply Chain Risk'
sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/gscfmospo/bcp/Shared%20Documents/A-%20Supply%20Chain%20Risk%20Dashboard/FMO%20IPN%20Risk%20Tracker.xlsx?d=wdfcb93c5604a4889980ac14bbc069ec5&csf=1&web=1&e=hIvZ0L"
sheet_name = 'Direct Materials'

df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0)
print(df.columns)
insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.IPNRiskTracker', data=df, categorical=['IPN'], truncate=True, driver="{SQL Server}")
log(insert_succeeded, project_name=project_name, data_area='FMO IPN Risk Tracker', row_count=df.shape[0], error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {} rows into fmo.IPNRiskTracker'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns

    sheet_name = 'Exempt IPNs'
    df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0)

    # try:
    #     df["Last Update Date"] = df["Last Update Date"].apply(lambda x: datetime.strptime(x, '%m/%d/%Y') if isinstance(x, str) else x if isinstance(x, datetime) else None)
    # except KeyError:
    #     log(False, project_name=project_name, data_area='FMO IPN Exempt List', row_count=0, error_msg="Column missing/changed in IPN Risk Tracker - IPN Exempt Master file.")

    insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.IPNExempt', data=df, categorical=['IPN'], truncate=True, driver="{SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area='FMO IPN Exempt List', row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {} rows into fmo.IPNExempt'.format(df.shape[0]))
    else:
        print(error_msg)  # Exempt List bombed
else:
    print(error_msg)  # IPN Risk Tracker bombed
