__author__ = ""
__email__ = ""
__description__ = "Loads worker-department-snapshot-details into sql"
__schedule__ = "5:21 AM PST each day"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import requests, json
import urllib3
import pandas as pd
from Project_params import params
from Helper_Functions import uploadDFtoSQL, queryAPIPortal
from Logging import log
import os
import numpy as np

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

# Initialize Variable
project_name = 'Supply Chain Organization Data'
table = 'dbo.SupplyChainOrgData'

# set Intel proxy parameters and load them into an object that can be used by any further HTTP requests
# URL in API Portal: https://api-portal-internal.intel.com/docs/worker/1/routes/v2/worker-department-snapshot-details/get

token_url = "https://apis-sandbox.intel.com/v1/auth/token"
test_api_url = "https://apis-internal.intel.com/worker/v2/worker-department-snapshot-details?$format=JSON&$select=" \
               "DepartmentCd," \
               "DepartmentLevelThreeCd," \
               "DepartmentLevelFourCd," \
               "DepartmentLevelFiveCd," \
               "DepartmentLevelFourNm," \
               "DepartmentLevelFiveNm," \
               "DepartmentLevelSixNm," \
               "DepartmentLevelSevenNm," \
               "DepartmentLevelEightNm," \
               "DepartmentLevelNineNm," \
               "DepartmentLevelTenNm"

df = queryAPIPortal(test_api_url)
# print(df)

                      ## Current Supply Chain Orgs
df['supply_chain_org'] = np.where(df['DepartmentLevelFourCd'] == '17029', 'Yes', # GSCO
                         np.where(df['DepartmentLevelFourCd'] == '14987', 'Yes', # CPLG
                         np.where(df['DepartmentLevelFourCd'] == '101033', 'Yes', # GSEM
                         np.where(df['DepartmentLevelFourCd'] == '12633', 'Yes', # WCS
                         np.where(df['DepartmentLevelFourCd'] == '12262', 'Yes', # GSEM ATS
                         np.where(df['DepartmentLevelFiveCd'] == '74980', 'Yes', # GSM Professional Excellence

                       ## Former GSC Orgs
                         np.where((df['DepartmentLevelFourCd'] == '101964') & (df['DepartmentLevelFiveCd'] != '64335'), 'Former GSC', # CPG not including BMG
                         np.where(df['DepartmentLevelFourCd'] == '94692', 'Former GSC', # RDSE/PDE
                         ## np.where(df['DepartmentLevelFourCd'] == '00997', 'Former GSC', # formerly ICF and no longer an appropriate former GSC org

                       ## Matrixed Orgs
                         np.where(df['DepartmentLevelFiveCd'] == '16808', 'Matrix', # GSM Quality
                         'No')))))))))

pd.set_option('display.max_columns', None)
print(df)

# Test Above Functions
# tst_df = df.loc[df['DepartmentLevelFiveCd'] == '64335']
# print(tst_df)

insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{SQL Server}")
log(insert_succeeded, project_name=project_name, data_area='Worker API SC Organization Data', row_count=df.shape[0], error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
else:
    print(error_msg)
