__author__ = "Pratha Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads SLM data from sharepoint to GSCDW"
__schedule__ = "Daily at 6:00 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Helper_Functions import loadSharePointList, uploadDFtoSQL, getLastRefresh, executeSQL, executeStoredProcedure
from Logging import log
from Project_params import params


#remove the current file's directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

dest_db = 'GSMDW'
LoadBy = 'SharepointList'
if __name__ == "__main__":
    # initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')
    sp_site = 'https://intel.sharepoint.com/sites/gscfabccms-ManageBI'
    project_name = 'SLM Files'
    data_area = 'SLM Data'

    ### SLM Ownership Matrix ###
    list_name = 'SLM Ownership Matrix'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl','Id','Modified','Created',
                      'ServerRedirectedEmbedUri'], axis=1, errors='ignore')
        # print(df.columns)
        df.rename(columns={"Title": "SupplierID", "field_1": "SupplierName", "field_2": "SLMOwner", "field_3": "Group"
                           , "field_4": "NameStatus", "field_5": "POType", "field_6": "FAB_AT", "field_7": "Common_Name"
                           , "field_8": "MasterData", "field_9": "LetterSuppliers"})

        toTable = 'slm.OwnershipMatrix'
        insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, sp_site, dest_db))
            log(insert_succeeded, project_name=project_name, data_area='brdSLMOwnershipMatrix', row_count=df.shape[0],
                error_msg=error_msg)
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=toTable, row_count=df.shape[0], error_msg=error_msg)


##### SLM Letters ####
    list_name = 'Letters'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl','Id','Modified','Created',
                      'ServerRedirectedEmbedUri'], axis=1, errors='ignore')

        df.rename(columns={"Title": "Member", "field_1": "Letters", "field_2": "TransitioningTo"})
        toTable = 'slm.Letters'
        insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, sp_site, dest_db))
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=toTable, row_count=df.shape[0], error_msg=error_msg)

##### Buyer Names Codes ####
    list_name = 'Buyer Names Codes'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl','Id','Modified','Created',
                      'ServerRedirectedEmbedUri'], axis=1, errors='ignore')

        #df['field_5'] = df['field_5'].astype(int)
        df['field_5'] = pd.to_numeric(df['field_5'], errors='coerce')
        df.rename(columns={"Title": "BuyerCode", "field_1": "BuyerNameCode", "field_2": "ExceptionConvention", "field_3": "ExceptionOwner", "field_4": "StandardName", "field_5": "WWID"})
        toTable = 'slm.BuyerNamesCodes'
        insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, sp_site, dest_db))
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=toTable, row_count=df.shape[0], error_msg=error_msg)



##### POrg ####
    list_name = 'POrg'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl','Id','Modified','Created',
                      'ServerRedirectedEmbedUri'], axis=1, errors='ignore')

        df.rename(columns={"Title": "PurchaseOrgNumber", "field_1": "PurchasingOrganization", "field_2": "PoType"})
        toTable = 'slm.POrg'
        insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, sp_site, dest_db))
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=toTable, row_count=df.shape[0], error_msg=error_msg)



#####GSM Supplier Tool Family BU####
    list_name = 'GSM Supplier Tool Family BU'
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # print(last_refreshed)

    df = loadSharePointList(sp_site=sp_site, list_name=list_name, remove_metadata=False, last_upload_time=last_refreshed)
    if len(df) == 0:
        print('SharePoint List has not been updated since last run. Skipping.')
    else:
        df = df.drop(['ComplianceAssetId', 'GUID', 'Attachments', 'OData__UIVersionString', 'EditorId', 'AuthorId',
                      'ContentTypeId', 'ID', 'FileSystemObjectType', 'ServerRedirectedEmbedUrl','Id','Modified','Created',
                      'ServerRedirectedEmbedUri'], axis=1, errors='ignore')
        #df['Title'] = df['Title'].astype(float)
        df['Title'] = pd.to_numeric(df['Title'], errors='coerce')
        df.rename(columns={"Title": "Supplier #", "field_1": "Supplier Name", "field_2": "Supplier", "field_3": "Tool Family"
                           , "field_4": "BU", "field_5": "PrimaryKey"})
        toTable = 'slm.GSMSupplierToolFamilyBU'
        insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, sp_site, dest_db))
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=toTable, row_count=df.shape[0], error_msg=error_msg)
