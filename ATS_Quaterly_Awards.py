__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script supports the Assembly Materials Quarterly Awards rollup (ask Daniel Vasquez <daniel.vasquez.contreras@intel.com> for details)
                     by loading data from Excel files on SharePoint Online to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"""
__schedule__ = "Daily at 3:55 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import time
from datetime import datetime, timedelta
from Helper_Functions import uploadDFtoSQL, getSQLCursorResult
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time.time()
    # print(start_time)

    # initialize variables
    project_name = 'Assembly Materials Quarterly Awards'
    shared_drive_folder_path = r"\\VMSPFSFSCH002.amr.corp.intel.com\AM_Awards"  # r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\AT"
    file_name = 'AMO Quarterly Awards.csv'

    # # get Worker WWID mapping from EDW
    # query = """SELECT *
    #            FROM OPENQUERY(APPL_GSM_BI, 'SELECT CONCAT(RTRIM(wrkr_last_nm), '', '', RTRIM(wrkr_frst_nm)) AS WorkerName
    #                                                 ,wrkr_wwid AS WWID
    #                                          FROM Worker.v_wrkr
    #                                          WHERE wrkr_sts_cd <> ''T''')"""
    #
    # query_succeeded, workers, error_msg = copySQLtoDF(query)
    # if not query_succeeded:
    #     print(error_msg)
    #     error_msg = 'Unable to return EDW query. Did not attempt to load data. ' + error_msg
    #     log(False, project_name=project_name, data_area='Award Recipients', error_msg=error_msg)
    #     exit(1)  # quit the program reporting error
    # # else:
    #     # workers.set_index('WorkerName', inplace=True)  # Change the index to Last Name, First Name
    #     # print(workers.shape)
    #     # print(workers.head(10))

    # get date and time of previous upload
    query = """SELECT [Last Refresh Date]
               FROM audit.Last_Refresh_Date
               WHERE [Project Name] = 'Assembly Materials Quarterly Awards' AND [Data Area] = 'Awards'"""
    query_succeeded, result, error_msg = getSQLCursorResult(query)
    if query_succeeded:
        last_upload_time = result[0][0]
    else:  # if unable to determine last upload, check if file was updated in the past day
        print(error_msg)
        last_upload_time = datetime.today() - timedelta(days=1)
    # print("Last uploaded: {}".format(last_upload_time))

    # check when file was last modified
    try:
        mod_time = time.ctime(os.path.getmtime(os.path.join(shared_drive_folder_path, file_name)))  # get last modified datetime from File
        mod_time = datetime.strptime(mod_time, '%a %b %d %H:%M:%S %Y')  # convert string datetime to datetime object
        # print("Last modified: {}".format(mod_time))

        if mod_time >= last_upload_time:  # check if file was modified since the last time it was loaded
            # print('File modified since last upload')

            # read .csv file from Shared Drive
            df = pd.read_csv(os.path.join(shared_drive_folder_path, file_name))
            df_recip = df.drop(df.columns.difference(['Title', 'Name of Recipient_x0']), axis=1)  # remove other columns (Power Query) python equivalent
            df_recip.rename(columns={'Title': 'AwardTitle', 'Name of Recipient_x0': 'RecipientName'}, inplace=True)  # rename columns (Power Query) python equivalent

            # remove extra columns and reorder columns for awards attribute table
            keep_columns = ['ID', 'Title', 'WhatistheQuarterlyAwardCategory_', 'What is the de', 'What quarter is_x002',
                            'Created By:', 'Approved?', "Martha's Approval_x0"]
            try:
                df = df[keep_columns]
            except KeyError as error:
                print(error)
                log(False, project_name=project_name, data_area='Awards', error_msg=error)
                exit(1)

            # change Yes/No column to True/False
            df['Approved?'] = df['Approved?'].apply(lambda x: True if type(x) == str and x.lower() == 'yes' else False)

            # split RecipientName column on every other comma
            span = 2
            recipients = []
            for val in df_recip['RecipientName']:
                if val.startswith('GSC '):
                    temp = val.upper().split(', ')[1:]  # remove the first entry to avoid uneven splitting
                else:
                    temp = val.upper().split(', ')
                recipients.append([', '.join(temp[i:i + span]) for i in range(0, len(temp), span)])
            # print(recipients)
            df_recip['RecipientName'] = recipients

            # explode (create new row for each entry in list) the RecipientName column
            df_recip = df_recip.explode('RecipientName')

            # # join recipients dataframe to workers dataframe to get WWID mapping
            # df_recip = df_recip.merge(workers, how='left', left_on='RecipientName', right_on='WorkerName')
            # df_recip.drop(['WorkerName'], axis=1, inplace=True)
            # print(df_recip.columns)
            # print(df_recip.head(10))

            insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_AM_Qtrly_Awards'], data=df, truncate=True, driver="{SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area='Awards', row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_AM_Qtrly_Awards']))

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_AM_Award_Recip'], data=df_recip, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Award Recipients', row_count=df_recip.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df_recip.shape[0], params['Table_AM_Award_Recip']))
                else:
                    print(error_msg)
            else:
                print(error_msg)

        else:
            print('Old file. Skipping upload...')
    except FileNotFoundError as error:
        print(error)
        log(False, project_name=project_name, data_area='Awards', error_msg=error)

    print("--- %s seconds ---" % (time.time() - start_time))
