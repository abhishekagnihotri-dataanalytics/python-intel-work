__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads storage locations codes and sites for HFE project from excel"
__schedule__ = "Every Saturday Once at 4:30PM IST GAR"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import uploadDFtoSQL, map_columns, executeStoredProcedure, loadExcelFile
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
	data_area = 'HFESiteRedefine'
	base_table = '[Base].[HFESiteRedefineUI]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'Sharepoint_Excel'
	source = 'RequiredIPNsAndStockrooms.xlsx'
	destination_db = 'GSCDW'
	shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\HFE_Demand_Supply"
	excel_sheet_name = 'Table_SiteRedefine_UI'
	excel_file = 'RequiredIPNsAndStockrooms.xlsx'
	
	
df_siteredefine = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)
df_siteredefine.columns.values[:] = ["strg_loc_cd","plnt_cd","site_redefine"]


df_siteredefine['loaddtm'] = pd.to_datetime('today')
df_siteredefine['loadby'] = load_by

map_columns(base_table,df_siteredefine)


# Clear base table before attempting to copy data 
sp_name = '[ETL].[spTruncateTable]'
truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if truncate_succeeded:
	print("Successfully truncated table {}".format(base_table))
else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df_siteredefine.shape[0], error_msg=error_msg)

#Insert into base table the newly pulled data
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df_siteredefine, truncate=True, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df_siteredefine.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df_siteredefine.shape[0], error_msg=error_msg)