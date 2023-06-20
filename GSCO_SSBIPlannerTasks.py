__author__ = "Pratha Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads from the CSI Requests SharePoint List into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 6:30 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path

from datetime import datetime
from Helper_Functions import loadExcelFile, uploadDFtoSQL
from Logging import log

#remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\OperationalAnalytics"
excel_sheet_name = 'Tasks'
project_name = 'Skynet'
(_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
for excel_file in file_list:
    if not excel_file.startswith('~'):  # ignore open files
        if excel_file == 'SS Analytics Kanban.xlsx': # explict file comparison
        # if 'Some Partial Str' in excel_file: # implicit file comparison (i.e. matching a file that has date appended to it)
            df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)


df['LastLoadDtm'] = datetime.today()
tableNm = 'dbo.SSBIPlannerTasks'
insert_succeeded, error_msg = uploadDFtoSQL(table=tableNm, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
log(insert_succeeded, project_name=project_name, data_area='PlannerTasks', row_count=df.shape[0], error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {0} records into {1}'.format(df.shape[0], 'dbo.SSBIPlannerTasks'))
else:
    print(error_msg)

# destination_server = 'sql2377-fm1-in.amr.corp.intel.com,3180'
# destination_db = 'SCDA'
# tableNm = 'dbo.Tasks'
# insert_succeeded, error_msg = uploadDFtoSQL(table=tableNm, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}", server=destination_server, database=destination_db)
# log(insert_succeeded, project_name=project_name, data_area='PlannerTasks', row_count=df.shape[0], error_msg=error_msg)
# if insert_succeeded:
#     print('Successfully inserted {0} records into {1}'.format(df.shape[0], 'dbo.SSBIPlannerTasks'))
# else:
#     print(error_msg)
