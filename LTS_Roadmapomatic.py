__author__ = "Pratha Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads from an Excel file on SharePoint Online to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 3:00 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import date
import pandas as pd
from Project_params import params
from Helper_Functions import getLastRefresh, loadExcelFile, uploadDFtoSQL
from Logging import log
from Password import decrypt_password

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # Initialize variables
    project_name = 'Roadmapomatic'
    data_area = 'Product Data'
    sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/gscinfrastructureandplatformsteam/Shared%20Documents/Supplier_Roadmap/fullroadmap.xlsx?d=w1b34078f93e74c8f8a86dc724ae711de&csf=1&web=1&e=GGZ3wH"
    sheet_name = 'Product Data'
    table = 'dbo.ProductData'
    refresh_date = date.today()

    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

    # SharePoint Online Application ID -- DEPRECATED, use Azure Application for new code
    credentials = {
        "client_id": "1ad0e1a2-f678-4508-a31c-dfa44fb21daa",
        "client_secret": decrypt_password(b'gAAAAABiIp3WWCIjFi5kHwQqQr0nQwu_51AUIbXJqWNU0e-FFdrK11CG6Psg7qGZYVWOGNQj8CQqXD3whpkAJ9FOADWR6CLt3CHeDvkqVpk_l6YQU7OguVcVYRpPcKxMTdrwOubAHrc2'),
    }

    # Get last refresh date from database before attempting to read data
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0, credentials=credentials, last_upload_time=last_refreshed)
    if len(df.index) == 0:  # DataFrame is empty
        print('Skipped {0} as it has not been modified since the last upload.'.format(data_area))
    else:
        # print(df.columns)

        df = df[df['Product'].notna()]  # Remove rows where Product is NULL
        df['RefreshDate'] = refresh_date  # Add refresh date column

        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if not insert_succeeded:
            print(error_msg)
        else:
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
