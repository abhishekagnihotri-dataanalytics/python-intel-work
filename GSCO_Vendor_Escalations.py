__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data by staging it in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh, map_columns
from Logging import log


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def parse_html(body: str) -> str:
    result = 'nan'
    if body is not None:
        result = BeautifulSoup(body, features="html.parser").get_text()  # type: str
    return result


if __name__ == "__main__":
    # initialize variables
    sp_site = 'https://intel.sharepoint.com/sites/gscmitcollaborationteamsite/'
    project_name = 'MIT Collaboration'
    data_area = 'Vendor Escalation System'
    list_name = 'GSC Vendor Escalation System'
    table = 'dbo.VendorEscalationSystem'

    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=True, last_upload_time=last_refreshed)
    if len(df.index) > 0:
        try:
            df.drop(['GSM Owner'], axis=1, inplace=True)  # Remove duplicate column (the GSCOwner column has the same data)

            # format date columns from text
            date_cols = ['Date Created ', 'Planned Close Date', 'Date of GSM Up', 'Date of Closure']
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce').dt.date

            # parse WWID from AssignedTo field
            df.rename({'Assigned to ': 'AssignedToWWID'}, axis=1, inplace=True)
            df['AssignedToWWID'] = df['AssignedToWWID'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) else x)

            # convert update append column to True/False
            df['GSMUpdateAppend_Yes'] = df['GSMUpdateAppend_Yes'].apply(lambda x: True if isinstance(x, str) and x.lower().startswith('congrats') else
                                                                                  False if isinstance(x, str) else
                                                                                  None)

            df['StatusUpdateText'] = df['GSM Status Update'].apply(parse_html)  # Convert HTML formatting to text

            # round Days columns to a whole number
            day_cols = ['Days Open', 'Days Late']
            for col in day_cols:
                df[col] = df[col].apply(lambda x: round(x) if isinstance(x, float) else int(float(x)) if isinstance(x, str) else x)

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column name changed in SharePoint List.')
            exit(0)

        # add database standards columns to end of DataFrame
        df['LoadDtm'] = datetime.now()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # insert into SQL by column name (ignore column order)
        sql_columns = ['ProblemId', 'VEorPMO', 'AssignedToWWID', 'ShortDescription', 'Notes', 'Vendor', 'CreatedDate', 'PlannedCloseDate',
                       'DispositionState', 'Priority', 'StatusUpdateRaw', 'StatusUpdateDate', 'ClosureDate', 'RemainingItems', 'Status',
                       'AssignedToName', 'OwnerName', 'DaysOpen', 'LastUpdate', 'UpdateAppendedFlag', 'DaysLate', 'StatusUpdateText', 'LoadDtm', 'LoadBy']
        print(df.columns)

        # # Uncomment the below line of code to debug truncation error in SQL insert
        # map_columns(table=table, df=df, sql_columns=sql_columns, display_result=True)

        # Insert data to SQL database
        insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, columns=sql_columns, truncate=True)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
        else:
            print(error_msg)
