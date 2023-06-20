__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script supports the New Supplier Integration (NSI) project by loading data from Excel files on SharePoint Online to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"""
__schedule__ = "Daily at 2:01 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh
from Logging import log
from Project_params import params


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    # initialize variables
    sp_site = 'https://intel.sharepoint.com/sites/Self-ServiceAnalyticsTeams'
    project_name = 'NSI Checklist'

    ### BEGIN Load Checklist information ###
    list_name = 'NSI Checklist'
    last_refreshed = getLastRefresh(project_name=project_name, data_area='NSI Checklist')

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=True, last_upload_time=last_refreshed)
    # print(df.columns)

    if len(df.index) > 0:
        df['Pre-Requisite'] = df['Pre-Requisite'].apply(lambda x: None if isinstance(x, str) and x.lower() == 'n/a' else x)
        # df['Typical Duration'] = df['Typical Duration'].apply(lambda x: int(x) if isinstance(x, str) and x.isdigit() else None)  # change data type from string to int
        df['Critical'] = df['Critical'].apply(lambda x: True if isinstance(x, str) and x.lower() == 'yes' else False)  # convert yes/no column to True/False

        # Insert data to SQL database
        insert_succeeded, error_msg = uploadDFtoSQL(params['Table_NSI_Checklist'], data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area='NSI Checklist', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_NSI_Checklist']))
        else:
            print(error_msg)
    ### END Load Checklist information ###

    ### BEGIN Load Supplier information ###
    list_name = 'NSI Suppliers'
    last_refreshed = getLastRefresh(project_name=project_name, data_area='NSI Suppliers')

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=True, last_upload_time=last_refreshed)
    # print(df.columns)

    if len(df.index) > 0:
        df['DispositionDate'] = df['DispositionDate'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').date() if isinstance(x, str) else x.date() if isinstance(x, datetime) else None)

        # Insert data to SQL database
        insert_succeeded, error_msg = uploadDFtoSQL(params['Table_NSI_Suppliers'], data=df, categorical=['global_id', 'local_id'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area='NSI Suppliers', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_NSI_Suppliers']))
        else:
            print(error_msg)
    ### END Load Supplier information ###

    ### BEGIN Load Supplier Response information ###
    list_name = 'NSI Repository'
    last_refreshed = getLastRefresh(project_name=project_name, data_area='NSI Tracker')

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)

    if len(df.index) > 0:
        # Remove metadata columns and reorder columns to match SQL database
        keep_columns = ['Title', 'supplier_esdid_local', 'task_number', 'status', 'status_comment', 'modified_by', 'modified_on']
        try:
            df = df[keep_columns]
        except KeyError as error:
            print(error)
            log(False, project_name=project_name, data_area='NSI Tracker', error_msg=error)
            exit(1)
        # print(df.columns)

        # Parse datetime from string in modified_on column
        df['modified_on'] = pd.to_datetime(df['modified_on'], format='%m/%d/%Y %I:%M %p', errors='coerce')
        # print(type(df['modified_on'][0]))

        # Insert data to SQL database
        insert_succeeded, error_msg = uploadDFtoSQL(params['Table_NSI_Tracker'], data=df, categorical=['supplier_esdid_local'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area='NSI Tracker', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_NSI_Tracker']))
        else:
            print(error_msg)
    ### END Load Supplier Response information ###=
