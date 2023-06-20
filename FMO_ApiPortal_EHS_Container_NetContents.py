__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script loads from the API Portal to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 7:53 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import requests
import json
import pandas as pd
from Helper_Functions import uploadDFtoSQL, queryAPIPortal
from Logging import log
from Password import accounts

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


# Initialize Variable
project_name = 'FMO API Item Characteristics v2 EHS Container Net Contents'
table = 'fmo.API_ItemChar_v2_EHSContainerNetContents'

# set Intel proxy parameters and load them into an object that can be used by any further HTTP requests
token_url = "https://apis-internal-sandbox.intel.com/v1/auth/token"
test_api_url = "https://apis-internal.intel.com/item/v2/user-defined-attribute-item-revision-details?$filter=\"UserDefinedAttributeId\" IN ('11905','11910','11952')&$format=JSON&$select=ItemId,UserDefinedAttributeId,ValueTxt"

df = queryAPIPortal(test_api_url)

# # Move JSON response in pandas DF
df = df.drop_duplicates()
df = pd.pivot_table(df, index=['ItemId'], values='ValueTxt', aggfunc=lambda x: ','.join(x), columns=['UserDefinedAttributeId']).reset_index('ItemId')
df.columns = ['PDMId', 'EHSNumber', 'Container', 'NetContents']
print(df)


insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['PDMId'], truncate=True, driver="{SQL Server}")
log(insert_succeeded, project_name=project_name, data_area='FMO API Portal EHS Num, Container, Net Contents', row_count=df.shape[0], error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
else:
    print(error_msg)


##### Old Loop Method #####

# # Initialize Looping UDA Variables
# AttributeList = ['11905', '11910', '11952']
# FinalDF = pd.DataFrame()
#
# for Attribute in AttributeList:
#     token_url = "https://apis-internal-sandbox.intel.com/v1/auth/token"
#     test_api_url = "https://apis-internal.intel.com/item/v2/user-defined-attribute-item-revision-details?&UserDefinedAttributeId={Attribute}&$format=JSON&$select=ItemId,UserDefinedAttributeId,ValueTxt".format(
#         Attribute=Attribute
#     )
#     # step A, B - single call with client credentials as the basic auth header - will return access_token
#     data = {'grant_type': 'client_credentials', 'client_id': accounts['Apigee'].client_id, 'client_secret': accounts['Apigee'].client_secret}
#
#     access_token_response = requests.post(token_url, data=data, proxies=proxyDict, verify=False, allow_redirects=False)
#     # print(access_token_response.headers)
#     # print(access_token_response.text)
#     tokens = json.loads(access_token_response.text)
#     # print("access token: " + tokens['access_token'])
#
#     # step B - with the returned access_token we can make as many calls as we want
#
#     api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
#     api_call_response = requests.get(test_api_url, headers=api_call_headers, proxies=proxyDict, verify=False)
#     # print(api_call_response.text)
#     # print(api_call_response)
#
#     x = api_call_response.json()
#
#     # Move JSON response in pandas DF
#     # pd.set_option('display.max_columns', None)
#     df = pd.DataFrame(x['elements'])
#     df = df.drop_duplicates()
#     if len(FinalDF.index) == 0:
#         FinalDF = df
#     else:
#         FinalDF = FinalDF.append(df)
# # print(FinalDF)
#
# pd.set_option('display.max_columns', None)
# df_pivot = pd.pivot_table(FinalDF, index=['ItemId'], columns=['UserDefinedAttributeId'], values='ValueTxt', aggfunc=lambda x: ','.join(x))
# print(df_pivot)