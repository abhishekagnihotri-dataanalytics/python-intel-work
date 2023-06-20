__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data from the Issue Lifecycle Management catalog on the API Portal to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Weekly on Sunday at 8:00 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime, timedelta
from time import time
from bs4 import BeautifulSoup
from typing import Union
import warnings
from Helper_Functions import queryAPIPortal, uploadDFtoSQL, executeStoredProcedure
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def concat_list(x) -> Union[str, None]:
    temp = x.tolist()
    if len(temp) > 0:
        new_list = [x for x in temp if pd.isnull(x) == False]
        return ', '.join(new_list)
    else:
        return None


def parse_cell(content: str) -> Union[str, None]:
    if content is None:
        return None
    else:
        return BeautifulSoup(content, features="html.parser").get_text()


def prepMQI(df_start: pd.DataFrame) -> pd.DataFrame:
    # Remove columns from DataFrame
    df_start = df_start.drop(columns=['ClosureTPT', 'ProblemOwnerWwid'])

    # Add new column to track data source
    df_start['Source'] = 'MQI_API'

    ### TODO: the below columns need to be calculated
    df_start['Open TPT'] = None
    df_start['ContainerNumber'] = None
    df_start['Reopen TPT'] = None

    # Parse text from HTML tags within columns
    warnings.filterwarnings("ignore", category=UserWarning, module='bs4', message='.*looks like a filename.*')  # suppress BeautifulSoup warnings when passing text directly to Constructor
    for col in ['ProblemDescription', 'RootCauseDescription', 'EscapeRootCause', 'RootCauseFixSummary']:  # html columns
        df_start[col] = df_start[col].apply(parse_cell)

    # Convert datetime text strings to datetime.date objects
    for col in ['IssueDetectedDate', 'CreatedDate', 'ModifiedDate', 'DispositionDate', 'ClosedDate', 'ReopenDate']:  # date columns
        df_start[col] = pd.to_datetime(df_start[col].str[:20], format='%Y-%m-%dT%H:%M:%S', errors='coerce').dt.date

    # Calculate time delta between two dates
    df_start['Input Lag'] = (df_start['CreatedDate'] - df_start['IssueDetectedDate']).dt.days
    df_start['Close TPT'] = (df_start['ClosedDate'] - df_start['IssueDetectedDate']).dt.days
    df_start['Days Open'] = (df_start['ClosedDate'].where(pd.notnull, datetime.today().date()) - df_start['CreatedDate']).dt.days  # coalesce today's date if no close date

    df_start['SupplierBusinessGroupName'] = df_start['BusinessArea'] + "-" + df_start['Supplier']

    df_start['PriorityId'] = df_start['EventRiskLevel'].apply(lambda x: x[-1] if isinstance(x, str) else None)
    df_start['MQIWeight'] = df_start['MQIPriority'].apply(lambda x: 6 if isinstance(x, str) and x == 'A' else 3 if isinstance(x, str) and x == 'B' else 1 if isinstance(x, str) and x == 'C' else None)
    df_start['SupplierResponsible'] = df_start['SupplierResponsible'].apply(lambda x: 'Yes' if isinstance(x, str) and x.lower() == 'y' else 'No' if isinstance(x, str) else None)

    # join to get affected site names
    df_sites = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/sites?$select=EventId,Site&$format=JSON")
    temp = df_sites.groupby(['EventId'])['Site'].apply(', '.join)
    temp = pd.DataFrame(temp)
    temp.rename(columns={'Site':'AffectedSiteNames'}, inplace=True)
    df = df_start.merge(temp, how='left', on='EventId')

    # join to get items
    df_mat_item = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-items?$select=EventId,ItemCode,ItemDescription&$format=JSON")
    temp = df_mat_item.groupby(by="EventId", as_index=False, sort=False).agg({"ItemCode" : concat_list, "ItemDescription": concat_list})
    df = df.merge(temp, how='left', on='EventId')

    # join to get affected technologies
    df_event = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/event-technologies?$select=EventId,Technology&$format=JSON")
    temp = df_event.groupby(['EventId'])['Technology'].apply(', '.join)
    temp = pd.DataFrame(temp)
    temp.rename(columns={'Technology':'AffectedTechnologyNames'}, inplace=True)
    df = df.merge(temp, how='left', on='EventId')

    # join to get category (bucket name)
    df_mat_root = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-root-causes?$select=EventId,Category,Details&$format=JSON")
    temp = df_mat_root.groupby(by="EventId", as_index=False, sort=False).agg({"Category" : concat_list, "Details" : concat_list})
    df = df.merge(temp, how='left', on='EventId')

    # join to get lot number and affected quantities
    df_supplier = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-supplier-lot?$select=EventId,LotNumber,AffectedQuantity&$format=JSON")
    temp = df_supplier.groupby(by="EventId", as_index=False, sort=False).agg({"LotNumber" : concat_list, "AffectedQuantity": 'sum'})
    df = df.merge(temp, how='left', on='EventId')

    # trim unusually long columns
    df['LotNumber'] = df['LotNumber'].apply(lambda x: x[0:3999] if isinstance(x, str) else "")
    df['ProblemDescription'] = df['ProblemDescription'].apply(lambda x: x[0:3999] if isinstance(x, str) else "")
    df['RootCauseFixSummary'] = df['RootCauseFixSummary'].apply(lambda x: x[0:3999] if isinstance(x, str) else "")
    df['RootCauseDescription'] = df['RootCauseDescription'].apply(lambda x: x[0:3999] if isinstance(x, str) else "")

    # reorder columns
    column_order = ['EventId', 'ResponsibleOrg', 'SupplierBusinessGroupName', 'Supplier', 'SupplierEsdId', 'PriorityId',
                    'MQIPriority', 'MQIWeight', 'Commodity', 'Title', 'ProblemDescription', 'Category', 'Details',
                    'ImpactDetail', 'ImpactArea', 'IssueSourceEventID', 'ReportingSite', 'ReportingTechnology',
                    'AffectedSiteNames', 'AffectedTechnologyNames', 'ItemCode', 'ItemDescription', 'LotNumber',
                    'ContainerNumber', 'AffectedQuantity', 'RootCauseFixSummary', 'MQIType', 'DispositionDate',
                    'RootCauseDescription', 'RepeatEvent', 'YieldDesignator', 'ProblemOwner', 'Originator',
                    'CreatedDate', 'IssueDetectedDate', 'ClosedDate', 'ReopenDate', 'ContainmentDescription',
                    'ContainmentStatus', 'ModifiedBy', 'ModifiedDate', 'Days Open', 'Input Lag', 'SafetyEvent',
                    'EventType', 'SupplierSite', 'ResponsibleFunction', 'SupplierResponsible', 'PLCStage',
                    'EscapeRootCause', 'Open TPT', 'Reopen TPT', 'Close TPT', 'Source', 'Status', 'AllowCca',
                    'BusinessArea', 'DefectType', 'EventRiskLevel', 'FunctionalArea', 'FurthestEscapePoint', 'HLVE',
                    'HoldReason', 'IcfEvent', 'ModifiedByWwid', 'OriginatorWwid', 'PHQReason', 'PotentialReleaseDate',
                    'PotentialScrapRisk', 'RootCauseIdentifiedDate', 'RootCauseNotFound', 'RootCauseTpt',
                    'TotalToolDowntime', 'UnitScrapped', 'WafersImpacted', 'Yield']
    return df[column_order]


if __name__ == "__main__":
    start_time = time()

    ### BEGIN ILM MQI section ###
    # initialize variables
    project_name = 'ILM MQI API Weekly Script'
    data_area = 'ILM MQI'

    # Extract Data from Issue Lifecycle Management API
    start_year = datetime.now().year - 4
    row_count = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$select=EventId&$filter=\"CreatedDate\">='{}-01-01'&$format=JSON".format(start_year)).shape[0]
    print('Expecting {} records to be returned by the API'.format(row_count))

    ### IMPORTANT - the same API call will not return the rows in the same order by default, ORDERBY must be used to appropriately get all rows
    df_mat_issue = pd.DataFrame()
    for i in range(0, row_count, 1000):
        # print(i)
        temp = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$start_index={}&$filter=\"CreatedDate\">='{}-01-01'&$orderby=EventId&$count=1000&$format=JSON".format(i, start_year))
        if i == 0:
            df_mat_issue = temp
        else:
            df_mat_issue = pd.concat([df_mat_issue, temp], ignore_index=True)
    print('Loaded {} records from the API into DataFrame'.format(df_mat_issue.shape[0]))

    print("--- %s seconds ---" % (time() - start_time))
    start_time2 = time()  # restart start time

    # Transform data
    df = prepMQI(df_mat_issue)
    print('Data prep completed!')

    print("--- %s seconds ---" % (time() - start_time2))
    start_time2 = time()  # restart start time

    # Load data into SQL Server database
    insert_succeeded, error_msg = uploadDFtoSQL(table="stage.stg_API_ILM_MQI", data=df, chunk_size=500, truncate=True)  # Truncate entire table prior to loading new data
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

    print("--- %s seconds ---" % (time() - start_time2))

    # execute stored procedure for mqi.speedStg table
    sp_succeeded, error_msg = executeStoredProcedure('mqi.sp_API_ILM_MQI_Merge')
    log(sp_succeeded, project_name=project_name, package_name="SQL: mqi.sp_API_ILM_MQI_Merge", data_area=data_area, error_msg=error_msg)

    # execute stored procedure for qlty.MQIWeekly
    sp_succeeded, error_msg = executeStoredProcedure('mqi.sp_API_MQIWeeklyMerge')
    log(sp_succeeded, project_name=project_name, package_name="SQL: mqi.sp_API_MQIWeeklyMerge", data_area=data_area, error_msg=error_msg)
    ### END ILM MQI section ###

    ### BEGIN MQI Users table ###
    row_count = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$select=EventId&$format=JSON").shape[0]
    start = pd.DataFrame()
    for i in range(0, row_count, 2000):
        # print(i)
        temp = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/material-issues?$start_index={}&$select=EventId,ResponsibleOrg,ProblemOwnerWwid&$orderby=EventId&$count=2000&$format=JSON".format(i))
        if i == 0:
            start=temp
        else:
            start = pd.concat([start, temp], ignore_index=True)

    techs = queryAPIPortal(url="https://apis-internal.intel.com/ilm/mqi/v1/event-technologies?$select=EventId,Technology&$format=JSON")
    users = start.merge(techs, how='left', on='EventId')
    users.rename(columns={'ProblemOwnerWwid': 'Wwid'}, inplace=True)

    # get Worker WWID to IDSID mapping
    df_workers = queryAPIPortal(url="https://apis-internal.intel.com/worker/v6/worker-snapshot-details?$filter=\"EmployeeStatusCd\"<>'T'&$select=Wwid,Idsid&$format=JSON")
    df_workers = df_workers.astype({'Wwid': float})
    # print(df_workers)

    # join to get IDSID from WWID
    df_UP = users.merge(df_workers, how='left', on='Wwid')
    df_UP = df_UP[['EventId', 'Idsid', 'Technology', 'ResponsibleOrg']]  # Reorder columns and remove other columns
    df_UP = df_UP[(df_UP['Technology'].notnull()) & (df_UP['Idsid'].notnull())]  # Remove rows where technology or idsid is NULL
    df_UP = df_UP.drop_duplicates()

    # land DataFrame in sql database
    insert_succeeded, error_msg = uploadDFtoSQL(table="stage.stg_API_ILM_MQI_UserProcess", data=df_UP, truncate=True)
    log(insert_succeeded, project_name=project_name, data_area='User Process', row_count=df_UP.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error

    # execute stored procedure for qlty.MQIWeekly
    sp_succeeded, error_msg = executeStoredProcedure('mqi.sp_API_ILM_MQI_UserProcess')
    log(sp_succeeded, project_name=project_name, package_name="SQL: mqi.sp_API_ILM_MQI_UserProcess", data_area='User Process', error_msg=error_msg)

    print("--- %s seconds ---" % (time() - start_time))
