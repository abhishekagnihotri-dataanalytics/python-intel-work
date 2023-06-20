__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SSC_MarketIntelligence tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
import pandas as pd
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log_warning, log


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    start_time = time()
    file_path = r"\\vmsoapgsmssbi06.amr.corp.intel.com\gsmssbi\SSC\Memory\Price_PRF\PRF Price List.xlsx"
    project_name = 'SSC Memory'
    data_area = 'Price PRF'
    table = "stage.stg_MSS_Demand_PRF_RTF"

    try:
        df = loadExcelFile(file_path=file_path, sheet_name="Consolidated")

        try:
            df['Forecast Date'] = pd.to_datetime(df['Year-Month'], format="%Y-%m") # convert 'Year-Month' column to datetime
            df.drop(['Year-Month', 'Year', 'Month'], axis=1, inplace=True)  # remove duplicate column from table
            # print(df.head())

            print(df.columns)

            excel_column_names = ['Forecast Cycle', 'Forecast Date', 'Data Type', 'IPN', 'MPN',  'Supplier', 'BU', 'Memory Tech', 'Memory Tech 3', 'Density (Gb)', 'Width', 'PPU']
            df = df[excel_column_names]  # manually change sorting to database ordering
        except KeyError:
            log(False, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg='Missing column in the {0} Excel file.'.format(os.path.basename(file_path)))
            exit(0)

        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name='Load_MSS_Price_PRF', data_area='MSS Price PRF', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
        else:
            # TODO: Email Error message to user
            print('Error loading data.')

    except FileNotFoundError:
        log_warning(project_name=project_name, data_area=data_area, file_path=file_path, warning_type='Missing')
    finally:
        print("--- %s seconds ---" % (time() - start_time))
        # 12.716149 seconds total to load from Excel file, format, and insert into SQL database
