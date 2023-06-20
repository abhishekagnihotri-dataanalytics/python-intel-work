__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = "This script loads from the API Portal to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 7:54 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Helper_Functions import uploadDFtoSQL, queryAPIPortal
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass



if __name__ == "__main__":
    # Initialize Variable
    project_name = 'FMO API Item Characteristics v4 Net Content Qty'
    table = 'fmo.API_ItemChar_v4_NetContentQtyUoM'

    pd.set_option('display.max_columns', None)

    # Load data from the API Portal
    df = queryAPIPortal(url="https://apis-internal.intel.com/item/v4/item-characteristic-details?&ProductDataManagementItemCharacteristicNm=NET_CONTENT_UoM&$format=JSON&$select=ProductDataManagementItemId,CharacteristicValueTxt")
    df = df.drop_duplicates()
    # print(df)

    df1 = queryAPIPortal(url="https://apis-internal.intel.com/item/v4/item-characteristic-details?&ProductDataManagementItemCharacteristicNm=NET_CONTENT_QTY&$format=JSON&$select=ProductDataManagementItemId,CharacteristicValueTxt")
    df1 = df1.drop_duplicates()
    # print(df1)

    df = df.merge(df1, on='ProductDataManagementItemId', how='left')
    df.rename(columns={'CharacteristicValueTxt_x': 'NetContentUoM', 'CharacteristicValueTxt_y': 'NetContentQty'}, inplace=True)
    print(df)

    insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['ProductDataManagementItemId'], truncate=True, driver="{SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area='FMO API Portal Net Content', row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    else:
        print(error_msg)
