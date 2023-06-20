__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads XRate data from EDW to GSCDW DB"
__schedule__ = "Once Daily at 4AM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Logging import log
from Helper_Functions import queryTeradata, uploadDFtoSQL, executeSQL, executeStoredProcedure
import pandas as pd
from Project_params import params

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


project_name='FabConsolidation'
data_area='XRate'
Table = 'Stage.brdXRATE'
LoadBy= 'EDW'
source = 'procurement_analysis.v_dim_cur_rte_shr'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

query = """Select
B.exchg_rte_state_cd
,Substr (CAL.fscl_yr_ww_nbr,0,5)||'-WW'||Substr (CAL.fscl_yr_ww_nbr,5,7) as Intel_WW
,B.cur_fr_cd
,B.cur_to_cd
,B.exchg_rte_vld_fr_dt
,B.exchg_rte_vld_to_dt
,B.abs_exchg_rte_pct
,B.calc_exchg_rte_pct
FROM	procurement_analysis.v_dim_cur_rte_shr as B
LEFT JOIN Calendar_Analysis.v_clndr_day as CAL
ON (B.exchg_rte_vld_fr_dt = CAL.clndr_dt)
where B.cur_to_cd ='USD'
-- and B.cur_fr_cd in('JPY','EUR','SGD')
and (B.exchg_rte_vld_fr_dt-B.exchg_rte_vld_to_dt) <>-363
and B.exchg_rte_type_cd not in('R','P')  --and exchg_rte_state_cd ='H'
           """


df=queryTeradata(query)

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, source,
                                                                          dest_db))

    table_name =  '[Base].[brdXRATE]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name,table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[brdXRATE]")
        Insert_query = """insert into [GSCDW].[Base].[brdXRATE]
        SELECT *  FROM [GSCDW].[Stage].[brdXRATE]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base table")
        else:
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[Stage].[brdXRATE]'
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name,table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[brdXRATE]")
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

