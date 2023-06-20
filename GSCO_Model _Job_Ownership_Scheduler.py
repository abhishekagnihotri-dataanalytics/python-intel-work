__author__ = "Pratha Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads from the Model Ownership SharePoint List into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 6:30 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh, map_columns
from Logging import log, log_warning
from Project_params import params

#remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')
    sp_site = 'https://intel.sharepoint.com/sites/Self-ServiceAnalyticsTeams'
    project_name = 'Skynet'
    data_area = 'ModelOwnership'

    list_name = 'ModelOwnership'
    tableNm = 'dbo.ModelOwnership'

    # Get Last Refresh information
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    # Extract data from SharePoint Online List
    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        exit(0)

    # Transform data
    for col in ['LastApproved', 'Modified', 'Created']:  # datetime columns
        df[col] = pd.to_datetime(df[col].str.slice(stop=10), format='%Y-%m-%d')

    df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                  'ContentTypeId',  'FileSystemObjectType','ServerRedirectedEmbedUrl',
                  'ServerRedirectedEmbedUri', 'ID', 'LastTRG', 'LastTRGStatus'], axis=1, errors='ignore')

    df['LastLoadDate'] = datetime.today()

    # # Debugging - uncomment the following line if there is an error with the number of columns not matching the SQL table
    # map_columns(tableNm, df=df)

    # Load data into SQL Server database
    insert_succeeded, error_msg = uploadDFtoSQL(table=tableNm, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], tableNm))
    else:
        print(error_msg)

    # Copy data to SQL2377 server
    destination_server = 'sql2377-fm1-in.amr.corp.intel.com,3180'
    destination_db = 'SCDA'
    tableNm = 'dbo.ModelOwnership'
    data_area = 'ModelOwnership SQL2377 Copy'

    insert_succeeded, error_msg = uploadDFtoSQL(table=tableNm, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}", server=destination_server, database=destination_db)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], tableNm))
    else:
        print(error_msg)