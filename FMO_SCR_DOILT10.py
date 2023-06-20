__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script loads TAC Ops from the Internal/External DMO SP to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
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


params['EMAIL_ERROR_RECEIVER'].extend(['wayne.chen@intel.com', 'erin.harmon@intel.com'])
project_name = 'FMO DOI Events LT10'
# sharepoint_excel_link_oldspinternal = "https://intel.sharepoint.com/:x:/r/sites/gscfmogfmdmo/Shared%20Documents/DMO%20Daily%20Ops/DAILY%20OPS%20Meeting.Q3_2021.xlsx?d=w5c6509fccf9749bda90c53e1d1f0ab84&csf=1&web=1&e=3LMgG1"
sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/gsemgfmdmonatmm/Shared%20Documents/Daily%20Ops/DAILY%20OPS%20Meeting%20Load.xlsx?d=w45ef62efa18a45999e6c9ebad66164cf&csf=1&web=1&e=IjmYFg"

sheet_name = 'Daily Ops Notes'

# start_time = time()

df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=3)
pd.set_option('display.max_columns', None)

if len(df.index) == 0:  # DataFrame is empty:
    log(False, project_name=project_name, data_area='FMO DOI Events LT10 New', row_count=0, error_msg="File Does Not Exist / Moved")
    exit(1)

df.drop(['Tech Node - DO NOT PUBLISH', 'Lowest Projected DOI (and Date)'], axis=1, inplace=True)

# pd.set_option('display.max_colwidth', 255)

df = df[df['IPN'].notna()]
print(df)

insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.DOIEvents_DailyOps2021', data=df, categorical=['IPN'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")  # overwrite the global SQL Driver parameter
log(insert_succeeded, project_name=project_name, data_area='FMO DOI Events LT10 New', row_count=df.shape[0],
    error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {} rows into fmo.DOIEvents_DailyOps2021'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns

######### Only need to load 2020 once, if needed to load, we can load using this commented out block
    # sharepoint_excel_link_2020 = "https://intel.sharepoint.com/:x:/r/sites/gscfmogfmdmo/Shared%20Documents/DMO%20Daily%20Ops/OldRevs%20Daily%20OPS%20Files/DAILY%20OPS%20Meeting.Q4_2020.xlsx?d=wb25b38dc32d946cfbcdb22c2cfca90b4&csf=1&web=1&e=PJnUW2"
    # df1 = loadExcelFile(sharepoint_excel_link_2020, sheet_name=sheet_name, header_row=3, credentials=credentials)
    # # Convert the dictionary into DataFrame to delete
    # delete = 'Tech Node - DO NOT PUBLISH'
    # for col in df1.columns:
    #     if delete in col:
    #         del df1[col]
    # insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.DOIEvents_DailyOps2020', data=df1, categorical=['IPN'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    # log(insert_succeeded, project_name=project_name, data_area='FMO DOI Events LT10 2020', row_count=df1.shape[0],
    #     error_msg=error_msg)
    # if insert_succeeded:
    #     print('Successfully inserted {} rows into fmo.DOIEvents_DailyOps2020'.format(df1.shape[0]))
    # else:
    #     print(error_msg)  # 2020 Load
#########
else:
    print(error_msg)  # 2021 Load

# print("--- %s seconds ---" % (time() - start_time))
