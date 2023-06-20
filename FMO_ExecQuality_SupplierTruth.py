__author__ = "Angela Baltes"
__email__ = "angela.baltes@intel.com"
__description__ = "This script loads the Supplier Truth Table from the Internal GSC QnR Sharepoint to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 6:45 AM / 12:45 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
from datetime import datetime
import pandas as pd
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

params['EMAIL_ERROR_RECEIVER'].extend(['wayne.chen@intel.com', 'wilfred.m.onwo@intel.com', 'angela.baltes@intel.com'])
project_name = 'FMO Quality Incidents Trends and Supplier Ranking'
sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/gscqnrfmoallspo/Shared%20Documents/Power%20BI%20Folder/Supplier_Truth_Table.xlsx?d=w374eeff5acbb4fe195ff4d357b3466e2&csf=1&web=1&e=NOCCdA"

sheet_name = 'Supplier Mapping'

# start_time = time()

df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0)
pd.set_option('display.max_columns', None)

if len(df.index) == 0:  # DataFrame is empty:
    log(False, project_name=project_name, data_area='GSC QnR FMO ALL SPO', row_count=0, error_msg="File Does Not Exist / Moved")
    exit(1)

df = df[df['Supplier-Name'].notna()]
print(df)

insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.ExecQuality_SupplierTruth', data=df, categorical=None, truncate=True, driver="{ODBC Driver 17 for SQL Server}")  # overwrite the global SQL Driver parameter
log(insert_succeeded, project_name=project_name, data_area='GSC QnR FMO ALL SPO', row_count=df.shape[0],
    error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {} rows into fmo.ExecQuality_SupplierTruth'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
else:
    print(error_msg)  # Load

#print("--- %s seconds ---" % (time() - start_time))
