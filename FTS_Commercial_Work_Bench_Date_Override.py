__author__ = "Jordan Makis"
__email__ = "jordan.makis@intel.com"
__description__ = "This script loads from the CSI Baseline Override SharePoint List into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 4:26 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
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
    params['EMAIL_ERROR_RECEIVER'].append('jordan.makis@intel.com')
    sp_site = 'https://intel.sharepoint.com/sites/gscmasterdata/MDAudit'
    project_name = 'Commercial WorkBench'
    data_area = 'CSI Baseline Date Reset'
    table = 'dbo.CWBBaselineDateOverride'

    list_name = 'CSI Baseline Date Reset'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['FileSystemObjectType',  'ServerRedirectedEmbedUri', 'ServerRedirectedEmbedUrl', 'AuthorId', 'EditorId',
                      'OData__UIVersionString', 'Attachments', 'GUID', 'ComplianceAssetId', 'DateCompleted', 'ApproverComments',
                      'Approved (L2 u', 'ID', 'ContentTypeId',], axis=1, errors='ignore')
        # print(df.columns)

        df['LastLoadDate'] = datetime.today()

        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
