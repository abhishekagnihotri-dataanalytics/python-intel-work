__author__ = "Jordan Makis"
__email__ = "jordan.makis@intel.com"
__description__ = "This script loads from the VMI Tracker Excel file into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getLastRefresh
from Logging import log, log_warning
from Project_params import params


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'ATS'
    data_area = 'VMI Tracker'
    table = 'dbo.VMITracker'

    params['EMAIL_ERROR_RECEIVER'].append(['jordan.makis@intel.com'])

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file on SharePoint
    df = loadExcelFile('https://intel.sharepoint.com/:x:/r/sites/gscatsspsc-tdprojectintake/Shared%20Documents/General/VMI-Tracker.xlsm?d=w3c6f0e5c4d694298963bdaec8f6c6d0c&csf=1&web=1&e=QBOg60',
                       sheet_name='Tracker', header_row=1, last_upload_time=last_refreshed)
    if len(df) == 0:
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('Excel file has not been updated since last run. Skipping.')
    else:
        # Transform data
        keep_columns = ['Transition \nProgress ', 'OEM AL', 'Communication date to VMI supplier', 'FWP Number(s)',
                        'WP Completed Date', 'TRAX Project IDs', '1272 CEID', '1274 CEID', '1276 CEID','OEM supplier',
                        'OEM IPN', 'OEM SPN','VMI Description','VMI supplier', 'VMI IPN', 'VMI IPN Comment', 'VMI SPN',
                        'OPM Name', 'Repairable/Non-repairable','Dual source','Lead Time','AL Comments','TPT (days)']
        try:
            df = df[keep_columns]  # Remove other columns and reorder to match SQL database
        except KeyError as error:
            log(False, project_name=project_name, data_area=data_area, error_msg='Columns name changed or missing in Excel File. Original error: {}'.format(error))
            exit(0)

        df['WP Completed Date'] = pd.to_datetime(df['WP Completed Date'], format='%Y-%m-%d', errors='coerce').dt.date  # Force "WP Completed Date" to be a date
        df['Lead Time'] = pd.to_numeric(df['Lead Time'], errors='coerce').astype(float)  # Force "Lead Time" column to be numeric

        # add database standards columns to end of DataFrame
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # Load data into SQL Database
        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['VMI IPN'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
