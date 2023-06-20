__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads storage location codes and plant codes from EDW"
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
	data_area = 'EDWStorageLocation'
	base_table = '[Base].[EDWHFEStrgLocPlntCoMapping]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW'
	source = 'Factory_Materials_Analysis.v_dim_fctry_item_strg_loc'
	destination_db = 'GSCDW'

query = """ 
			SELECT DISTINCT CONCAT(hier.strg_loc_cd,hier.plnt_cd) AS strg_loc_cd_plnt_cd, 
		hier.strg_loc_cd,
		hier.strg_loc_nm,
		hier.mfg_fctry_cd,
		hier.strg_loc_type_cd,
		hier.plnt_cd,
		loc.co_cd
		FROM FACTORY_MATERIALS_ANALYSIS.v_dim_fctry_strg_loc_hier AS hier
		JOIN 
		(SELECT DISTINCT CONCAT(strg_loc_cd,plnt_cd) AS strg_loc_cd_plnt_cd,
					 co_cd 
		FROM  Factory_Materials_Analysis.v_dim_fctry_item_strg_loc)  AS loc
		ON  strg_loc_cd_plnt_cd = concat(hier.strg_loc_cd,hier.plnt_cd)""".format(base_table)

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

#Insert into base table the Base table data 
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=True, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)