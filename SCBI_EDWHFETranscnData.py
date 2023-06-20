__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads a snapshot of the transaction data for inventory for the HFE project"
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
	data_area = 'HFEConsumption'
	base_table = '[Base].[EDWHFETranscnData]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'FACTORY_MATERIALS_ANALYSIS.v_fact_fctry_mtrl_trns_dtl'
	destination_db = 'GSCDW'

query = """
	SELECT                   asof_src_dt AS asof_src_dt
                        ,xfrm_asof_dt AS xfrm_asof_dt
                        ,itm_id AS itm_id
                        ,purch_grp_cd AS purch_grp_cd
                        ,curr_supl_id AS curr_supl_id
                        ,cmdt_cd AS cmdt_cd
                        ,fctry_mtrl_srvr_id AS fctry_mtrl_srvr_id
                        ,fctry_trns_id AS fctry_trns_id
                        ,fctry_trns_line_id AS fctry_trns_line_id
                        ,fctry_trns_line_iss_id AS fctry_trns_line_iss_id
                        ,fctry_trns_type_nm AS fctry_trns_type_nm
                        ,strg_loc_cd AS strg_loc_cd
                        ,cnsm_ind AS cnsm_ind
                        ,fctry_trns_ts AS fctry_trns_ts
                        ,fctry_trns_qty AS fctry_trns_qty
                        ,fctry_trns_rsn_nm AS fctry_trns_rsn_nm
                        ,cust_id AS cust_id
                        ,stor_id AS stor_id
                        ,wiings_ent_id AS wiings_ent_id
                        ,site_redefined AS site_redefined
 			,RIGHT( a1.itm_id,9)  ||  a1.site_redefined  ||  a1.fctry_trns_type_nm AS IPNstockroomTrans
                        ,a1.fctry_trns_id  ||  a1.fctry_trns_line_id  ||  ABS( a1.fctry_trns_qty ) AS UniqTrans_TransID_Line_QTY
                        ,ABS( a1.fctry_trns_qty ) AS ABS_fctry_trns_qty
                        ,a1.fctry_trns_ts AS fctry_trns_dt2
                        ,a1.strg_loc_cd AS stockroomid
FROM
                                             (
                                             SELECT 
                                        a0.asof_src_dt AS asof_src_dt
                                       ,a0.xfrm_asof_dt AS xfrm_asof_dt
                                       ,a0.itm_id AS itm_id
                                       ,a0.purch_grp_cd AS purch_grp_cd
                                       ,a0.curr_supl_id AS curr_supl_id
                                       ,a0.cmdt_cd AS cmdt_cd
                                       ,a0.fctry_mtrl_srvr_id AS fctry_mtrl_srvr_id
                                       ,a0.fctry_trns_id AS fctry_trns_id
                                       ,a0.fctry_trns_line_id AS fctry_trns_line_id
                                       ,a0.fctry_trns_line_iss_id AS fctry_trns_line_iss_id
                                       ,a0.fctry_trns_type_nm AS fctry_trns_type_nm
                                       ,a0.strg_loc_cd AS strg_loc_cd
                                       ,a0.cnsm_ind AS cnsm_ind
                                       ,a0.fctry_trns_ts AS fctry_trns_ts
                                       ,a0.fctry_trns_qty AS fctry_trns_qty
                                       ,a0.fctry_trns_rsn_nm AS fctry_trns_rsn_nm
                                       ,a0.cust_id AS cust_id
                                       ,a0.stor_id AS stor_id
                                       ,a0.wiings_ent_id AS wiings_ent_id
                                                      ,CASE  
                                                      WHEN  a0.strg_loc_cd  = 26 THEN 'Chandler' 
                                                      WHEN  a0.strg_loc_cd = 196 THEN 'Austin' 
                                                      WHEN  a0.strg_loc_cd = 107 THEN 'Chengdu' 
                                                      WHEN  a0.strg_loc_cd = 103 THEN 'Costa Rica Test' 
                                                      WHEN  a0.strg_loc_cd = 178 THEN 'Costa Rica MVE' 
                                                      WHEN  a0.strg_loc_cd = 2 THEN 'Israel' 
                                                      WHEN  a0.strg_loc_cd = 33 THEN 'Penang' 
                                                      WHEN  a0.strg_loc_cd = 24 THEN 'Kulim' 
                                                      WHEN  a0.strg_loc_cd = 15 THEN 'Oregon' 
                                                      WHEN  a0.strg_loc_cd = 186 THEN 'Oregon' 
                                                      WHEN  a0.strg_loc_cd = 179 THEN 'Oregon' 
                                                      WHEN  a0.strg_loc_cd = 105 THEN 'Oregon' 
                                                      WHEN  a0.strg_loc_cd = 72 THEN 'Folsom' 
                                                      WHEN  a0.strg_loc_cd = 3 THEN 'Santa Clara' 
                                                      WHEN  a0.strg_loc_cd = 6 THEN 'Santa Clara' 
                                                      WHEN  a0.strg_loc_cd = 210 THEN 'Vietnam' 
                                                      WHEN  a0.strg_loc_cd =  167 THEN 'Vietnam' 
                                                      ELSE 'x' 
                                                                            END AS site_redefined
                                       
                                             FROM 
                                             Factory_Materials_Analysis.v_fact_fctry_mtrl_trns_dtl AS a0
             WHERE
               fctry_trns_rsn_nm <> 'Transfer Cross Site'
               AND fctry_trns_rsn_nm <> 'Stockroom Transfer'
               AND (fctry_trns_type_nm = 'RECEIPT' OR fctry_trns_type_nm = 'ISSUE')
               AND (itm_id like '%500029003' OR itm_id like '%500395897' OR itm_id like '%500040960%' OR itm_id like '%35331770%')
               AND fctry_trns_ts between to_date('11-01-2020','MM-DD-YYYY') and to_date('12-31-2022','MM-DD-YYYY')
        ) AS a1 
"""

df = queryTeradata(query) #(server='TDPRD1.intel.com', credentials=None)

df['loaddtm'] = pd.to_datetime('today')
df['loadby'] = load_by

map_columns(base_table,df)

# #Clear the table before attempting to copy data 
# sp_name = '[ETL].[spTruncateTable]'
# truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
# if truncate_succeeded:
# 	print("Successfully truncated table {}".format(base_table))
# else:
#             log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

#Insert into base table the newly pulled data
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=False, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)