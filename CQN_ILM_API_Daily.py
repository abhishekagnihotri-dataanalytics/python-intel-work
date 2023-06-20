__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads from the CWB Odata feed to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily (excluding Sunday because the Weekly Loader runs then) at 3:30 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime, timedelta
from time import time
from Helper_Functions import queryAPIPortal, uploadDFtoSQL, executeStoredProcedure, getLastRefresh
from Logging import log
from CQN_ILM_API_Weekly import prepMQI

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()

    ### BEGIN ILM MQI section ###
    # initialize variables
    project_name = 'ILM MQI API Daily Script'
    data_area = 'ILM MQI'

    last_load = getLastRefresh(project_name=project_name, data_area=data_area)
    if last_load is None:
        temp = datetime.now() - timedelta(hours=8)
    else:
        temp = datetime.strftime(last_load, '%Y-%m-%dT%H:%M:%S')
    last_load = pd.Timestamp(temp).replace(minute=00, second=00)

    row_count = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$select=EventId&$filter=\"ModifiedDate\">='{}'&$format=JSON".format(last_load)).shape[0]

    # Get data from API
    ### IMPORTANT - the same API call will not return the rows in the same order by default, ORDERBY must be used to appropriately get all rows
    df_mat_issue = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$filter=\"ModifiedDate\">='{}'&$orderby=EventId&$format=JSON".format(last_load))
    print('Loaded {} records from the API into DataFrame'.format(df_mat_issue.shape[0]))

    # Transform data
    df = prepMQI(df_mat_issue)
    print('Data prep completed!')

    # Load data into SQL Server database
    insert_succeeded, error_msg = uploadDFtoSQL(table="stage.stg_API_ILM_MQI", data=df, chunk_size=500, truncate=True)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

    # Execute stored procedure for mqi.speedStg table
    sp_succeeded, error_msg = executeStoredProcedure('mqi.sp_API_ILM_MQI_Merge')
    log(sp_succeeded, project_name=project_name, package_name="SQL: mqi.sp_API_ILM_MQI_Merge", data_area=data_area, error_msg=error_msg)
    ### END ILM MQI section ###

    ### BEGIN Root cause table
    project_name = 'ILM MQI API Daily -Root Cause'
    data_area = 'RootCauseDetails'     
    row_count = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-root-causes?$select=EventId&$format=JSON").shape[0]
    df_root = pd.DataFrame()

    # Get data from API
    for i in range(0, row_count, 2000):
        # print(i)
        temp = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-root-causes?$start_index={}&$count=1000&$orderby=EventId&$format=JSON".format(i))
        if i == 0:
            df_root = temp
        else:
            df_root = pd.concat([df_root, temp], ignore_index=True)
    print('Loaded {} records from the API into DataFrame'.format(df_root.shape[0]))

    # Transform data in DataFrame
    df_root = df_root[['EventId', 'Category', 'Details', 'KeyFailure', 'Systemic', 'PrimaryIndicator', 'ModifiedBy', 'ModifiedDate']]
    df_root['ModifiedDate'] = df_root['ModifiedDate'].apply(lambda x: x if isinstance(x, datetime) else datetime.strptime(x.split(".")[0], '%Y-%m-%dT%H:%M:%S') if isinstance(x, str) else None)

    # Load data into SQL Server database
    insert_succeeded, error_msg = uploadDFtoSQL(table="stage.stg_API_ILM_MQI_RootCauseDetails", data=df_root, truncate=True)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df_root.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error

    # Execute Stored Procedure
    sp_succeeded, error_msg = executeStoredProcedure('mqi.sp_API_ILM_MQI_RootCauseDetails')
    log(sp_succeeded, project_name=project_name, package_name="SQL: mqi.sp_API_ILM_MQI_RootCauseDetails", data_area=data_area, error_msg=error_msg)

    print("--- %s seconds ---" % (time() - start_time))
