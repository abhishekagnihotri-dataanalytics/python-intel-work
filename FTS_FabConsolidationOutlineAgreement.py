__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads OA data from EDW to GSCDW DB"
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
data_area='Outline Agreement'
Table = 'Stage.OutlineAgreement'
LoadBy= 'EDW'
source = 'procurement_analysis_nrs.v_fact_outline_agrmnt'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

query = """SELECT	
		outline_agrmnt_id, outline_agrmnt_line_id, right(itm_id,9) as itm_id, supl_id, purch_org_cd,
		purch_grp_cd, pmt_term_cd, cur_cd, hdr_inco_term_cd,hdr_inco_term_dsc, hdr_vld_fr_dt, hdr_vld_to_dt, hdr_trgt_val_amt, hdr_trgt_val_USD_amt,
		lgl_ctrct_nbr, cre_dt, outline_agrmnt_type_cd, co_cd, hdr_del_ind,
		 supl_mtrl_id, outline_agrmnt_uom_cd, line_trgt_qty,
		line_net_prc_amt, line_net_prc_USD_amt, pln_dlv_day_cnt, line_del_ind,
		shp_instr_cd, shp_instr_dsc, line_vld_fr_dt, line_vld_to_dt
FROM	procurement_analysis_nrs.v_fact_outline_agrmnt
where line_del_ind <> 'L' and line_vld_to_dt >= current_date
           """


df=queryTeradata(query)

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, source,
                                                                          dest_db))

    table_name = '[Base].[OutlineAgreement]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[OutlineAgreement]")
        Insert_query = """insert into [GSCDW].[Base].[OutlineAgreement]
        SELECT *  FROM [GSCDW].[Stage].[OutlineAgreement]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base")
        else:
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[Stage].[OutlineAgreement]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[OutlineAgreement]")
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

