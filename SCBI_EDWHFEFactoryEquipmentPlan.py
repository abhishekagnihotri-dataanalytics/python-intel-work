__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script pulls afresh the complete Factory Equipment Plant data for each week from EDW"
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
	base_table = '[Base].[EDWHFEFactoryEquipmentPlan]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'Procurement_Analysis.v_dim_fctry_equip_pln'
	destination_db = 'GSCDW'

query = """ 
			SELECT	
			asof_src_dt, 
			asof_src_ts, 
			xfrm_asof_dt, 
			xfrm_asof_ts, 
			src_sys_nm,
			equip_pln_id, 
			copy_exact_id, 
			cap_prcss_cd, 
			equip_func_area_cd,
			itm_id, 
			equip_site_cd, 
			equip_bldg_cd, 
			strg_loc_cd, 
			plnt_cd, 
			equip_pln_upd_ts,
			equip_pln_cre_dt, 
			mfg_type_cd, 
			ruse_ind, 
			ueid_cd, 
			equip_pln_inactv_dt,
			equip_ent_cd, 
			fulfil_type_cd, 
			pln_tool_ent_cd, 
			wfr_strt_tie_cd,
			bdgt_area_cd, 
			pltfrm_cd, 
			fund_prcss_cd, 
			rqst_dlvr_dt, 
			cap_free_dt,
			draft_dir_cd, 
			hand_orient_cd, 
			high_altd_ind_cd, 
			fctry_lyot_cd,
			geo_pwr_rqr_cd, 
			allct_cnstrt_nm, 
			phys_loc_type_cd
			FROM	
			Procurement_Analysis.v_dim_fctry_equip_pln""".format(base_table)

df = queryTeradata(query) #(server='TDPRD1.intel.com', credentials=None)

df['loaddtm'] = pd.to_datetime('today')
df['loadby'] = load_by

map_columns(base_table,df)

# Clear the table before attempting to copy data 
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