__author__ = "Aysegul Demirtas"
__email__ = "aysegul.demirtas@intel.com"
__description__ = "This script loads from an Excel file on the 06 VM File Share into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import shutil
from datetime import datetime
from time import time
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getLastRefresh
from Logging import log, log_warning


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    # initialize variables
    project_name = 'IDOI Transport Media'
    data_area = 'Transport Media PRF RTF'
    table = 'stage.stg_iDOI_TransportMediaPRFRTF'

    successfully_loaded_files = list()
    renamed_files = list()

    # determine the last refresh datetime for each file
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # last_refreshed = None  # set this to None if you'd like to force a reload of the data regardless of last modified time on the file

    shared_drive_folder_path = r"\\vmsoapgsmssbi06.amr.corp.intel.com\gsmssbi\AT\iDOI"
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            ##### Transport Media PRF-RTF #####
            if 'Transport Media PRF-RTF' in excel_file:
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), sheet_name='PRF-RTF Vertial', header_row=0, last_upload_time=last_refreshed)
                if len(df.index) == 0:
                    log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
                    print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
                    continue

                # temp = pd.read_excel(file, sheet_name=x, usecols="A:B", nrows = 2)
                # df['Upload_Date'] = datetime.today()

                # Map Columns from CSV to SQL
                keep_columns = ['Month-Year', 'Commodity Type', 'Vendor Name', 'Part Number', 'PRF', 'RTF']

                try:
                    df = df[keep_columns]  # manually change column order to match database table
                except KeyError as error:
                    log(False, project_name=project_name, data_area=data_area, error_msg="Column missing/changed in Transport Media PRF RTF file. Full error: {0}".format(error))
                    continue
                print(df.columns)

                #df['Month-Year'] = df['Month-Year'].apply(lambda x: x if isinstance(x, datetime) else datetime.strptime(x, '%d %b %Y'))
                df['Month-Year'] = pd.to_datetime(df['Month-Year']).dt.date
                df['PRF'] = df['PRF'].apply(lambda x: x if isinstance(x, float) or isinstance(x, int) else None)  # delete any values that are not numbers
                df['RTF'] = df['RTF'].apply(lambda x: x if isinstance(x, float) or isinstance(x, int) else None)  # delete any values that are not numbers
                #print(df)

                # df['Sample Column'] = df['Sample Column'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p') if isinstance(x, str) else x)
                # df['Sample Column'].replace(r'^\s*$', np.nan, regex=True, inplace=True)  # replace field that's entirely spaces (or empty) with NaN)
                # df['Sample Column'].clip(upper=pd.Timestamp(2100,1,1), inplace=True)  # replaces all values over 01/01/2100 with that date
                # df['Sample Column'].replace({pd.Timestamp(2100,1,1), np.nan}, inplace=True)  # replaces all dates 01/01/2100 with NaN
                # df['Sample Column'] = df['Sample Column'].apply(lambda x: True if type(x) == str and x.lower() == 'late' else False) #change a column to True/False
                # df[Sample Column'].replace(r'^\s\$\s*\-\s*$', np.nan, regex=True, inplace=True)  # remove random space dash combinations
                # df['Sample Column'] = df['Sample Column'].apply(lambda x: int(float(x)) if isinstance(x, str) and 'e' in x else x[-9:] if isinstance(x, str) and '-' not in x else x)  # convert IPN strings to correct 9 digit format

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['Month-Year','Commodity Type', 'Vendor Name', 'Part Number'], truncate=True)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.csv')

    # if successfully_loaded_files:  # load was successfully for at least one file
    #    for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
    #        try:
    #            shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
    #        except PermissionError:
    #            print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    print("--- %s seconds ---" % (time() - start_time))
