__author__ = "Jordan Makis"
__email__ = "jordan.makis@intel.com"
__description__ = "This script loads from the CSI Requests SharePoint List into the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Three times daily at 7:00 AM, 11:30 AM, and 3:30 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh
from Logging import log
from Project_params import params


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('jordan.makis@intel.com')
    project_name = 'CWB'
    lists = ['CSI Requests', 'CSI OEM Requests']

    for list_name in lists:
        if list_name == 'CSI Requests':
            sp_site = 'https://intel.sharepoint.com/sites/GFM_SLM/uti-external'
            data_area = 'CSI Requests'
            table = 'dbo.CSIRequests'
        elif list_name == 'CSI OEM Requests':
            sp_site = 'https://intel.sharepoint.com/sites/gscmasterdata/TMEspecs'
            data_area = 'CSI Requests OEM Only'
            table = 'dbo.CSIRequestsOEMOnly'

        last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
        # print(last_refreshed)

        df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
        if len(df) == 0:
            print('SharePoint List has not been updated since last run. Skipping.')
        else:
            if list_name == 'CSI Requests':
                df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'Editor', 'Author',
                              'Title', 'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl',
                              'ServerRedirectedEmbedUri', 'Hyperlink', 'Image', 'Approverr', 'DateDif', 'Days old'
                              ], axis=1, errors='ignore')
            elif list_name == 'CSI OEM Requests':
                df = df.drop(['FileSystemObjectType', 'ServerRedirectedEmbedUri', 'ServerRedirectedEmbedUrl', 'ID',
                              'ContentTypeId', 'Title', 'Author', 'Editor', 'OData__UIVersionString', 'Attachments',
                              'GUID', 'ComplianceAssetId', 'IntelCycles', 'DSVCycles', 'ToolOwner', 'Hyperlink',
                              'field_Ariba ID', 'field_CRFQ', 'field_CRFQ Creation Date', 'field_IMS Comments',
                              'field_Priority', 'Requestor', 'Hyperlink', 'Image', 'FileSystemObjectType', 'Approverr',
                              'DateDif', 'Days old'], axis=1, errors='ignore')
            # print(df.columns)

            df['LastLoadDate'] = datetime.today()

            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
