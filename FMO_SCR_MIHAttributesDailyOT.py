__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = """This script grabs the last 6 months of inventory data daily from MIH (min, max, available inventory, 
                     DOI, consumption [maxplan outputs], unit cost, comments) and made available in GSM_FMO_SCR cube / 
                     FMO Supply Chain Risk Dashboard"""
__schedule__ = "6:56 AM PST daily"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import querySQL, uploadDFtoSQL, executeStoredProcedure
from Project_params import params
from datetime import date
from Project_params import params
import pandas as pd
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    source_server = 'sql2515-fm1-in.amr.corp.intel.com,3180'
    source_db = 'MIH'
    dest_table = 'stage.stg_FMO_MIH_Attributes_Daily_OT'
    project_name = 'FMO Supply Chain Risk'
    data_area = 'FMO MIH Attributes Daily OT'

    # generate query (or enter your own query here)
    query = """SELECT
                   [site_name]
                  ,[part_nbr]
                  ,[min]
                  ,[max]
                  ,[qtyAvailableWarehouse]
                  ,[qtyAvailableWarehouseAndWiingsOnsite]
                  ,[daysInventoryCoversCurWeek]
                  ,[consumptionPrevMonth]
                  ,[consumptionCurMonth]
                  ,[forecastCurMonth]
                  ,[forecastNextMonth]
                  ,[unitCost]
                  ,[load_date]
                  ,[comments]
              FROM [Mih].[dbo].[tbl_SupplyChainData]
              WHERE site_name <> 'Dalian'"""
    query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        # df['Load Date'] = date.today()
        pd.set_option('display.max_columns', None)
        print(df)

        insert_succeeded, error_msg = uploadDFtoSQL(dest_table, df, truncate=True, driver="{SQL Server}", server=params['GSMDW_SERVER'])  # upload dataframe to SQL
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], dest_table, source_server, params['GSMDW_SERVER']))
        else:
            print(error_msg)

    execute_succeeded, error_msg = executeStoredProcedure('fmo.MIH_Attributes_Daily_OT_Load', 'N')
    log(execute_succeeded, project_name='SQL: fmo.MIH_Attributes_Daily_OT_Load', data_area=data_area, row_count=1, error_msg=error_msg)
    if execute_succeeded:
        print('Successfully executed the fmo.MIH_Attributes_Daily_OT_Load stored procedure')
    else:
        print(error_msg)
