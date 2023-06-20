__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script pulls fresh equipment tool inventory (avail and moves) snapshot "
__schedule__ = "Every Saturday Once at 4:30PM IST GAR"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryTeradata, uploadDFtoSQL, map_columns, executeStoredProcedure
import pandas as pd
from Logging import log
from Project_params import params


try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    # initialize variables
	project_name = 'HFE Forecasting'
	data_area = 'EDWInventory'
	base_table = '[Base].[EDWHFEFactoryEquipmentReuse]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'Procurement_Analysis_NRS.v_fact_equip_ruse'
	destination_db = 'GSCDW'
	

query = """
SELECT 
		asof_src_dt, 
		asof_src_ts, 
		xfrm_asof_dt, 
		xfrm_asof_ts, 
		src_sys_nm,
		ueid_cd, 
		equip_pln_id, 
		asset_nbr, 
		co_cd, 
		prcur_equip_itm_id,
		pln_itm_id, 
		equip_itm_id, 
		pln_site_cd, 
		equip_site_cd, 
		pln_bldg_cd,
		equip_bldg_cd, 
		cap_free_dt_yr_ww_nbr, 
		pln_plnt_cd, 
		equip_plnt_cd,
		pln_mfg_type_cd, 
		equip_mfg_type_cd, 
		pln_func_area_cd, 
		equip_func_area_cd,
		pln_copy_exact_id, 
		equip_copy_exact_id, 
		pln_cap_prcss_cd, 
		equip_cap_prcss_cd,
		pln_co_cd, 
		equip_co_cd, 
		ruse_ind, 
		pln_phys_loc_type_cd, 
		equip_phys_loc_type_cd,
		pln_ent_cd, 
		equip_ent_cd, 
		pln_intrnl_model_id, 
		equip_intrnl_model_id,
		last_rcpt_dt, 
		orig_rcpt_dt, 
		tool_stg_nm, 
		pln_tool_ent_cd, 
		cap_free_dt,
		bdgt_area_cd, 
		mthd_of_rtire_nm, 
		hand_orient_cd, 
		draft_dir_cd,
		high_altd_ind_cd, 
		allct_cnstrt_nm, 
		equip_audt_dt, 
		co_ownr_expr_dt,
		tool_cmnt_txt, 
		orig_wbs_nbr, 
		pln_rqst_dlvr_dt, 
		equip_inv_del_ind,
		fulfil_type_cd, 
		actv_equip_pln_ind, 
		pln_strg_loc_cd, 
		equip_strg_loc_cd,
		equip_pln_tool_ent_cd, 
		equip_bdgt_area_cd, 
		equip_pltfrm_cd
FROM	Procurement_Analysis_NRS.v_fact_equip_ruse"""

df = queryTeradata(query) #(server='TDPRD1.intel.com', credentials=None)

df['loaddtm'] = pd.to_datetime('today')
df['loadby'] = load_by

map_columns(base_table,df)

#Clear base table before attempting to copy data 
sp_name = '[ETL].[spTruncateTable]'
truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if truncate_succeeded:
	print("Successfully truncated table {}".format(base_table))
else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)


#Insert into base table the freshly pulled data 
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=False, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)