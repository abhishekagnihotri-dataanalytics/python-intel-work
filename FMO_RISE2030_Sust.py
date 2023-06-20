__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script lands chemical footprint weighting factor CFWF information from sharepoint file into sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 6:55 AM"

#import required libraries
import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import email
from datetime import datetime, timezone
import sys
from time import time
from datetime import datetime
import numpy as np
import pandas as pd
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getLastRefresh
from Logging import log

params['EMAIL_ERROR_RECEIVER'].extend(['wayne.chen@intel.com', 'kayode.ogunsusi@intel.com'])
project_name = 'RISE 2030 Chemical Footprint Weighting Factor'

# Read Master CFP file from sharePoint
file = 'https://intel.sharepoint.com/:x:/r/sites/gscchemicalfootprint/Shared%20Documents/General/Baseline/Master%20Reference%20Tools/Master%20CFP%20Calculators%20and%20Reference%20Tables%20-%20PBI%20Input.xlsx?d=wa2809b3caca34e928b4c540306452651&csf=1&web=1&e=Vl5ZND'
sheet = 'Impact (x-axis) by IPN'
df = loadExcelFile(file_path=file, sheet_name=sheet, header_row=3, last_upload_time=None)
if len(df.index) == 0:  # DataFrame is empty:
    log(False, project_name=project_name, data_area='RISE 2030 Chemical Footprint', row_count=0, error_msg="File Does Not Exist / Moved")
    exit(1)

# print(df)
keep_columns = ['IPN', 'Chemical/Formulation', 'Supplier', 'Chemical Footprint Weighting Factor', 'Chemical Footprint Weighting Factor (controls)', 'Tier I Comments']
df.drop(df.columns.difference(keep_columns), axis=1, inplace=True)
# print(df.head(10))
df = df[df['IPN'].notna()]

insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.RISE2030ChemicalFootprintWeightingFactor', data=df, categorical=['IPN'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")  # overwrite the global SQL Driver parameter
log(insert_succeeded, project_name=project_name, data_area='RISE 2030 Chemical Footprint', row_count=df.shape[0],
    error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {} rows into fmo.RISE2030ChemicalFootprintWeightingFactor'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
else:
    print(error_msg)
