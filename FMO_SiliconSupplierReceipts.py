__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script loads data for the GSM_FMOSilicon tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 4:13 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
from datetime import datetime
import pandas as pd
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'FMO Silicon'
    data_area = 'FMO Silicon Supplier Receipts'
    sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/VMSPFSFSFM10_FMO_SILICON/Shared%20Documents/Silicon_Only/Commercial/300mm/300mm%20Inventory%20sheets/Supplier%20Shipments/Supplier%20Shipments%20File.xlsx?d=wd0072648f4d15cc3bac4dbec53a09232&csf=1&web=1&e=zr02i7"
    sheet_name = 'Sheet2'
    table = 'fmo.SiliconSupplierReceipts'

    start_time = time()

    df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0)
    df = df[df['Part Group'].notna()]  # filter to only rows where Part Group is not blank
    df['Plant Extension'] = ""
    df['Load Date'] = datetime.today()

    pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_colwidth', 255)
    print(df)

    insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")  # overwrite the global SQL Driver parameter
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
    else:
        print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
