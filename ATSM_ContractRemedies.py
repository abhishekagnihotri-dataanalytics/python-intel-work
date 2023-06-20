__author__ = "shweta.v.aurangabadkar"
__email__ = "shweta.v.aurangabadkar@intel.com"
__description__ = "This script loads data for the ContractRemedies from CSV to Sql"
__schedule__ = "Once Daily at 09:00 AM PST"

import os

import pandas as pd
from datetime import datetime
from time import time
from Helper_Functions import uploadDFtoSQL, executeSQL, executeStoredProcedure
import pandas as pd
from Logging import log
from Project_params import params


# remove the current file's parent directory from sys.path
# try:
#     sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
# except ValueError:  # Already removed
#     pass


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    # initialize variables
    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'AT Biz Ops'

    shared_drive_folder_path = r"\\ch2idmcenter-DM.cps.intel.com\GSCO_datashare\SSAS\sharepoint_conn"


    excel_file ='Contract Remedies Table.csv'
    data_area = 'Contract Remedies Table'
    file_path = os.path.join(shared_drive_folder_path, excel_file)
    sheet_name = 'Contract Remedies Table'
    stage_table = 'stage.brdContractRemediesTable'
    base_table = 'base.brdContractRemediesTable'
    LoadBy = 'AMR\\' + os.getlogin().upper()
    print(file_path)
    # Extract data from Excel file
    # df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=1)
    df = pd.read_csv(os.path.join(shared_drive_folder_path, excel_file), delimiter=',', quotechar='"')

    # Transform data
    df['LoadDtm'] = datetime.today()
    df['LoadBy'] = LoadBy

    # Load data to Database
    insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, LoadBy,
                                                                              params['GSMDW_DB']))

        # Clear base table before attempting to copy data from staging there
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table)
        if truncate_succeeded:
            print("Successfully truncated table {}".format(base_table))

            # Copy data from Stage table to Base table
            insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table,
                                                                                        copy_from=stage_table)
            insert_succeeded, error_msg = executeSQL(insert_query)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)  # log regardless of success or failure
            if insert_succeeded:
                print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table,
                                                                                      copy_from=stage_table))

            # Clear stage table after successful insert into Base table
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table)
            if truncate_succeeded:
                print("Successfully truncated table {}".format(stage_table))
            else:
                log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


