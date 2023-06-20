__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script is used for manually uploads of the entire PSI Forecast table"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import numpy as np
from re import sub
from datetime import datetime
from time import time
import shutil
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getSQLCursorResult
from Logging import log


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def convert(tup, dictionary):
    for a, b in tup:
        dictionary[a] = b
    return dictionary


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'ATS Operations Dashboard'

    query_succeeded, result, error_msg = getSQLCursorResult("""SELECT CONCAT(LEFT(Intel_WW, 4), '.', RIGHT(Intel_WW, 2), '.', CASE WHEN day_of_ww_nbr < 10 THEN CONCAT('0', day_of_ww_nbr) ELSE day_of_ww_nbr END), clndr_dt
                                                     FROM dbo.Intel_Calendar
                                                     WHERE fscl_yr_int_nbr > 2018 AND fscl_yr_int_nbr <= 2021 AND (day_of_ww_nbr IS NOT NULL AND day_of_ww_nbr <> 'NULL') AND day_of_ww_nbr < 6"""
                                                            )
    dates = dict()
    if not query_succeeded:
        print(error_msg)
        exit(1) # quit the program reporting error
    else:
        convert(result, dates)

    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\ATS_SCCI_Data"
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            if 'PSI' in excel_file and 'Forecast' in excel_file:

                parsed_date = excel_file.split('_')[-1][:10]
                print(parsed_date)
                upload_dt = dates[parsed_date]
                print(upload_dt)

                if upload_dt < datetime(2021, 2, 17):
                    excel_sheet_name = 'RDD Table'
                    df1 = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                    df = df1
                    # print(df.columns)

                    df['CRO'] = None
                    df['Checkpoint'] = None
                    df['Quarter'] = None

                else:
                    excel_sheet_name = 'RDD Table'
                    df1 = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                    df2 = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), 'RDD Table_CRO_4th Quarter', header_row=0)
                    df = pd.concat([df1, df2])  # Append second Excel tab to first
                    # print(df.columns)

                keep_columns = ['Product', 'Tool', 'Cycle', 'Site', 'ProcureBy', 'Name', 'Type', 'Material',
                                'Supplier Name', 'Supplier Number', 'OaDescription', 'Quantity', 'UOM', 'Date', 'CND',
                                'Subcategory', 'Repairable', 'FundType', 'Forecast$', 'OAUnit$', 'OaLeadTime',
                                'CPA Month', 'Fund Loaded?', 'Status', 'Line Item', 'Remark', 'CRO', 'Checkpoint',
                                'Quarter']
                df = df[keep_columns]  # manually change column order to match database table

                # Data massaging for SQL table
                df['Product'] = df['Product'].apply(lambda x: x.split(',')[0] if isinstance(x, str) else None)  # Only keep product info before the first comma

                try:
                    df['Date'] = df['Date'].apply(lambda x: x if isinstance(x, datetime) else datetime.strptime(x, '%d %b %Y'))  # Convert text "9 Apr 2021" to Datetime object
                except ValueError as error:  # Catch errors with different format of date text
                    print(error)
                    log(False, project_name=project_name, data_area='ATMBO PSI Forecast', row_count=0, error_msg=error)
                    continue

                money_columns = ['OAUnit$', 'Forecast$']
                for col in money_columns:
                    df[col] = df[col].apply(lambda x: float(sub(r'[^\d.]', '', x)) if isinstance(x, str) else float(x))  # Remove text " (OA)" after decimal number

                tf_columns = ['Repairable', 'Fund Loaded?', 'CRO']
                for col in tf_columns:
                    df[col] = df[col].apply(lambda x: True if isinstance(x, str) and x.lower() == 'yes' else False if isinstance(x, str) and x.lower() == 'no' else None)  # Change Yes/No column to True/False

                df['Module'] = df['Tool'].apply(lambda x: x.split('#')[0] if isinstance(x, str) else None)  # Create new column "Module" that takes everything from the "Tool" column before the first
                df['Upload_Date'] = [upload_dt] * len(df.index)  # append modified date

                # for val in df['Fund Loaded?']:
                #     if not isinstance(val, bool) and val is not None:
                #         print(val)
                #         print(type(val))

                # determine max length of characters in column
                for col1 in df.columns:
                    temp = df[col1].map(lambda x: len(x) if isinstance(x, str) else None).max()
                    print("Column: {0}, dtype: {1}, max length: {2}".format(col1, df[col1].dtype, temp))
                    try:
                        print(min(df[col1]))
                        print(max(df[col1]))
                    except TypeError:
                        continue

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_PSI_Forecast'], data=df, categorical=['Material'], truncate=False, driver="{SQL Server}")
                # log(insert_succeeded, project_name=project_name, data_area='ATMBO PSI Forecast', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_PSI_Forecast']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file)
                else:
                    print(error_msg)

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    print("--- %s seconds ---" % (time() - start_time))
