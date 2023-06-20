__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = """This script provides an override functionality for due dates in the CM Learning Certification dashboard 
                     (https://sqlbiprd.intel.com/reports/powerbi/GSM/Sourcing/Professional%20Excellence/LearningCertification). 
                     This was to correct incorrect values found in SABA due to mass job re-codes and movement back and forth into SC. 
                     Loads CMLearning.xlsx from Shared Drive to SQL by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181
                     There is not a job scheduled for this script, it is be used ad hoc."""
__schedule__ = "N/A"


# To switch from prod / dev: Run -> Edit Configurations -> Parameters -prod
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
    project_name = 'CM Learning Certificate'
    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\ProfessionalExcellence\LearningCertification"

    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            if excel_file == 'CMLearning.xlsx':  # explict file comparison
                excel_sheet_name = 'Not completed'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)
                insert_succeeded, error_msg = uploadDFtoSQL(table='TalExc.CMFoundationalOverwrite', data=df, categorical=['WWID'], truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='CM Learning Override', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {} rows into TalExc.CMFoundationalOverwrite'.format(df.shape[0]))  # or add files to list of correctly loaded files, df.shape[0] for row count [1] columns
                    shutil.move(os.path.join(shared_drive_folder_path, excel_file), os.path.join(shared_drive_folder_path, 'Archive', excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx'))  # Move Excel file to Archive folder after it has been loaded successfully
                else:
                    print(error_msg)
