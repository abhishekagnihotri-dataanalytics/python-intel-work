__author__ = "Khushboo Saboo"
__email__ = "khushboo.saboo@intel.com"
__description__ = "This script loads Abbreivated Supplier data  GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path

from Helper_Functions import getLastRefresh, loadExcelFile, getSQLCursorResult, uploadDFtoSQL, map_columns
from Logging import log, log_warning


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":

    ### BEGIN MOR Tool Costs Section ###
    # initialize variables
    project_name = 'FOM DRDP'
    data_area = 'DRDP_Abbreviated_Supplier'
    #file_path = 'https://intel.sharepoint.com/:x:/r/sites/LimaTrainingEnv/Shared%20Documents/FOM_DRDP/Abbreviated%20Supplier%20Mapping.xlsx?d=wd8d97aa31c59481da6d1625fa0ef2783&csf=1&web=1&e=dDEMqF'
    file_path = 'C:\\Users\\ksaboo\\OneDrive - Intel Corporation\\Desktop\\Abbreviated Supplier Mapping.xlsx'
    sheet_name = 'Mapping'
    table = '[dbo].[FOM_Abbreviated_Supplier]'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file
    df = loadExcelFile(file_path, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
    if len(df.index) == 0:
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
    else:
        try:
            # Load data to Database
            df = df[1:]
            print(df)

            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Missing column in the {0} Excel File.'.format(data_area))
