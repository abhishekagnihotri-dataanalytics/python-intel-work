__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads the current snapshot of inventory allocation on a site by site basis for HFE"
__schedule__ = "Every Saturday Once at 4:30PM IST GAR"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryTeradata, uploadDFtoSQL, executeSQL, map_columns, executeStoredProcedure
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
	base_table = '[Base].[EDWHFEAvailData]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'FACTORY_MATERIALS_ANALYSIS.v_fact_fctry_mtrl_inv_dtl'
	destination_db = 'GSCDW'


query = """
		select 
			item_id
			,strg_loc_cd
			,avail_qty
			,SUM(avail_qty) OVER (PARTITION BY  site_redefine  ORDER BY  site_redefine ) AS avail_qty_site_level
			,CAST(CURRENT_DATE AS char(19)) AS report_date
			from 
			(select 
				 a0.itm_id AS item_id
				,a0.strg_loc_cd AS strg_loc_cd
				,a0.avl_qty AS avail_qty
         ,CASE  WHEN  a0.strg_loc_cd = 26 THEN 'Chandler' 
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
		 ELSE 'x' END AS site_redefine
		,CAST(CURRENT_DATE AS char(19)) AS report_date
		FROM 
			Factory_Materials_Analysis.v_fact_fctry_mtrl_inv_dtl a0
			) a1
		WHERE (item_id LIKE '%500029003' 
			OR item_id LIKE '%500040960'
			OR item_id LIKE '%500395897'
			OR item_id LIKE '%35331770') """

df = queryTeradata(query) #(server='TDPRD1.intel.com', credentials=None)

df['loaddtm'] = pd.to_datetime('today')
df['loadby'] = load_by

map_columns(base_table,df)

#Clear the table before attempting to copy data 
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