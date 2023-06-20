# To switch from prod / dev: Run -> Edit Configurations -> Parameters -prod
__author__ = ""
__email__ = ""
__description__ = "Loads FMO Supply Chain Risk spike_notification_historic file to sql"
__schedule__ = "6:58 AM PST daily"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import xlrd
import shutil
from datetime import datetime
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    project_name = 'FMO Supply Chain Risk'
    shared_drive_folder_path = r"\\limashare-dm.cps.intel.com\lima\Spike\Spike_Report"
    excel_sheet_name = 'spike_notification_historic'

    # Load Historic Table
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            if excel_file == 'spike_notification_historic.csv':  # explict file comparison
                df = pd.read_csv(os.path.join(shared_drive_folder_path, excel_file), delimiter=',')
                pd.set_option('display.max_columns', None)
                print(df)

                insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.SPIKE_Notification_Historic', data=df, categorical=['ipn'], truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='FMO SCR SPIKE Notification Historic', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {} rows into fmo.SPIKE_Notification_Historic'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
                else:
                    print(error_msg)  # bombed

            elif excel_file == 'spike_summary_data.csv':  # explict file comparison
                df = pd.read_csv(os.path.join(shared_drive_folder_path, excel_file), delimiter=',')
                pd.set_option('display.max_columns', None)
                print(df)

                insert_succeeded, error_msg = uploadDFtoSQL(table='fmo.SPIKE_Notification_Summary', data=df, categorical=['ipn'], truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='FMO SCR SPIKE Notification Summary', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {} rows into fmo.SPIKE_Notification_Summary'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
                else:
                    print(error_msg)  # bombed
