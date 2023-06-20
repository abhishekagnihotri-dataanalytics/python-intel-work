__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Tool master data from EDW to GSCDW DB on sql2943-fm1-in.amr.corp.intel.com,3181"
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
data_area='Main Tool List'
Table = 'Stage.MaterialMasterMainTool'
LoadBy= 'EDW'
source = 'Procurement_Analysis_nrs.v_fact_mtrl_mstr_pln_itm'

dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')
query = """select 
            itm_id
            ,copy_exact_id
            ,cap_prcss_cd
            ,outline_agrmnt_id
            ,outline_agrmnt_line_id
            ,mfr_id
            ,intrnl_model_id
            ,itm_type_cd
            ,itm_type_dsc
            ,lgc_del_ind
            ,captl_cat_cd
            ,purch_team_cd
            ,cmdt_cd
            ,cmdt_dsc_shrt_txt
            ,itm_prcss_ctrl_sts_cd
            ,itm_prcss_ctrl_sts_dsc
            from Procurement_Analysis_nrs.v_fact_mtrl_mstr_pln_itm a
            where a.captl_cat_cd = 'MT' and a.captl_ind = 'Y' and lgc_del_ind <> 'Y'
         and   a.copy_exact_id is not null  and a.copy_exact_id <> '*'
           """


df=queryTeradata(query)

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, source,
                                                                          dest_db))

    table_name = '[Base].[MaterialMasterMainTool]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[MaterialMasterMainTool]")
        Insert_query = """insert into [GSCDW].[Base].[MaterialMasterMainTool]
        SELECT *  FROM [GSCDW].[Stage].[MaterialMasterMainTool]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base table")
        else:
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[Stage].[MaterialMasterMainTool]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[MaterialMasterMainTool]")
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

