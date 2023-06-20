__author__ = "Pratha Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = """This script loads from the SSBI Requests SharePoint List on the GSC_Analytics Teams site into the 
                     GSMDW database"""
__schedule__ = "Daily once at 6:30 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh, map_columns
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')
    sp_site = 'https://intel.sharepoint.com/sites/Self-ServiceAnalyticsTeams'
    project_name = 'Skynet'
    list_name = 'SSBI Requests'

    # Extract data from SharePoint Online List
    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        # Transform data
        for col in ['SSA Start Date', 'Modified', 'Created', 'Completion Date']:  # date columns
            df[col]=pd.to_datetime(df[col], errors='coerce').dt.date

        df['Assigned To'] = df['Assigned To'].apply(lambda x: x[0] if isinstance(x, dict) and len(x.keys()) == 1 else ','.join(sorted(x.values())) if isinstance(x, dict) else 'None')

        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId',  'FileSystemObjectType','ActualizedBusinessValue(', 'ServerRedirectedEmbedUrl',
                      'ServerRedirectedEmbedUri', 'vrrw', 'ID', 'IDPID', 'BusinessProcessLink', 'PowerBILink',
                      'RobertTran'], axis=1, errors='ignore')

        df['LastLoadDate'] = pd.to_datetime('today')

        # destination_server = 'sql2377-fm1-in.amr.corp.intel.com,3180'
        # destination_db = 'SCDA'
        table_name = 'dbo.SSBIIntakeRequests'

        # map_columns(table_name, df=df, server=destination_server, database=destination_db)

        insert_succeeded, error_msg = uploadDFtoSQL(table=table_name, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area='SSBI Requests', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table_name))

        destination_server = 'sql2377-fm1-in.amr.corp.intel.com,3180'
        destination_db = 'SCDA'
        table_name = 'dbo.SSBIIntakeRequests'

        # map_columns(table_name, df=df, server=destination_server, database=destination_db)

        insert_succeeded, error_msg = uploadDFtoSQL(table=table_name, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}", server=destination_server, database=destination_db)
        log(insert_succeeded, project_name=project_name, data_area='SSBI Requests', row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table_name))