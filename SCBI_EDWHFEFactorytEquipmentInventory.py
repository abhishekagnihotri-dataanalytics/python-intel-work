__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads Factory Equipment Inventory data from EDW for HFE project"
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
	base_table = '[Base].[EDWHFEFactoryEquipmentInventory]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'Procurement_Analysis.v_dim_fctry_equip_inv'
	destination_db = 'GSCDW'
	

query = """
SELECT 
		asof_src_dt, 
		asof_src_ts, 
		xfrm_asof_dt, 
		xfrm_asof_ts, 
		src_sys_nm,
		ueid_cd, 
		copy_exact_id, 
		cap_prcss_cd, 
		equip_func_area_cd, 
		itm_id,
		equip_site_cd, 
		equip_bldg_cd, 
		equip_ent_cd, 
		mfg_type_cd, 
		equip_upd_ts,
		strg_loc_cd, 
		plnt_cd, 
		cmps_cd, 
		equip_inactv_dt, 
		itm_dsc, 
		orig_wbs_nbr,
		orig_rcpt_dt, 
		last_rcpt_dt, 
		tool_stg_nm, 
		tool_cmnt_txt, 
		hand_orient_cd,
		draft_dir_cd, 
		high_altd_ind_cd, 
		fctry_lyot_cd, 
		geo_pwr_rqr_cd,
		allct_cnstrt_nm, 
		mthd_of_rtire_nm, 
		equip_audt_dt, 
		co_ownr_expr_dt,
		intrnl_model_id, 
		captl_cat_cd, 
		purch_team_cd, 
		tool_mfr_id, 
		phys_loc_type_cd,
		mfr_id, 
		co_cd
FROM	Procurement_Analysis.v_dim_fctry_equip_inv"""

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


#Insert into base table the Base table data 
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=False, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)