__author__ = "Wayne Chen"
__email__ = "wayne.chen@intel.com"
__description__ = """This script is used to calculate FMO Direct Material Fill Rate weekly (at the IPN and site level 
                     and aggregated in GSM_FMO_SCR as a measure) for the FMO Executive Dashboard and FMO Supply Chain Risk Dashboard. 
                     It takes into consideration the exempt list for that week and flags each week whether the IPN is exempt or not."""
__schedule__ = "8:30 PM PST every Saturday of every week"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import date
import pandas as pd
from Helper_Functions import querySQL, uploadDFtoSQL, executeStoredProcedure
from Project_params import params
from Logging import log


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    params['EMAIL_ERROR_RECEIVER'].append('wayne.chen@intel.com')

    # initialize variables
    source_server = 'sql2515-fm1-in.amr.corp.intel.com,3180'
    source_db = 'MIH'
    source_server_e = 'sql1717-fm1-in.amr.corp.intel.com,3181'
    source_db_e = 'GSMDW'
    dest_table = 'stage.stg_FMO_DOI_FillRate_OT'
    project_name = 'FMO DOI Fill Rate OT'
    data_area = 'FMO DOI Fill Rate OT'

    # generate query (or enter your own query here)
    query = """SELECT [site_name]
          ,[part_nbr]
          ,[part_dsc]
          ,[daysInventoryCoversCurWeek] AS "DOI"
          ,[currentDOIRisk]
          ,[qtyAvailableWarehouse] -- Rinchem
          ,[wiingsAvailableQtyTotal] -- WIINGS Total Avail
          ,[minCovered] -- Total 
          ,[min]
          ,[max]
    FROM [Mih].[dbo].[tbl_SupplyChainData]"""
    query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
    if not query_succeeded:
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        df['Load Date'] = date.today()

        # generate query (or enter your own query here)
        query1 = """SELECT Distinct [IPN]
              ,[Site]
              ,[PartDescription]
              ,[Buyer]
              ,[LastUpdateDate]
              ,case when getdate() >= dateadd(day,90,t1.LastUpdateDate)
                      then 'n'
                      else 'y'
          end as 'exemptFlag'
          FROM [gsmdw].[fmo].[IPNExempt] as t1"""
        query_succeeded, df1, error_msg = querySQL(query1, server=source_server_e, database=source_db_e)  # load exempt IPNs into DF1
        if not query_succeeded:
            print(error_msg)
            log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
        else:
            df_final = df.merge(df1, how='left', left_on=['part_nbr', 'site_name'], right_on=['IPN', 'Site'])  # multiple join columns
            pd.set_option('display.max_columns', None)

            df_final.drop(['IPN', 'Site', 'PartDescription', 'Buyer'], axis=1, inplace=True)
            df = df_final
            print(df)

            insert_succeeded, error_msg = uploadDFtoSQL(dest_table, df, truncate=True, driver="{SQL Server}", server=params['GSMDW_SERVER'])  # upload dataframe to SQL
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], dest_table, source_server, params['GSMDW_SERVER']))
            else:
                print(error_msg)

    execute_succeeded, error_msg = executeStoredProcedure('fmo.DOI_FillRate_OT_Load', 'N')
    log(execute_succeeded, project_name='SQL: fmo.DOI_FillRate_OT_Load', data_area=data_area, row_count=1, error_msg=error_msg)
    if execute_succeeded:
        print('Successfully executed the fmo.DOI_FillRate_OT_Load stored procedure')
    else:
        print(error_msg)

