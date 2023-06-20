__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads perceived raw material future supply to Intel data from excel sheet for the HFE project"
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
	data_area = 'HFESupply'
	base_table = '[Base].[HFESupplyUI]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'Sharepoint_Excel'
	source = 'RequiredIPNsAndStockrooms.xlsx'
	destination_db = 'GSCDW'
	shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\HFE_Demand_Supply"
	excel_sheet_name = 'Supply_UI'
	excel_file = 'RequiredIPNsAndStockrooms.xlsx'

df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)

df = df.iloc[1: , 0:23] # Relevant columns kept, irrelevant top rows removed
df.reset_index(drop=True, inplace=True) # Reset index to start from 0


df.rename(columns={ df.columns[0]: "month_alloc",
					df.columns[2]:"site",
					df.columns[3]: "ship_wk",
					df.columns[4]: "dock_wk_intel",
					df.columns[12]: "supp_ship_date",
					df.columns[19]: "est_dock_dt_supp",
					df.columns[20]: "actl_dock_dt_supp" }, inplace = True)

df = df.applymap(str) # Convert all Columns to String Type

df['loaddtm'] = pd.to_datetime('today')
df['loadby'] = load_by

map_columns(base_table,df)

# Clear base table before attempting to copy data 
sp_name = '[ETL].[spTruncateTable]'
truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if truncate_succeeded:
	print("Successfully truncated table {}".format(base_table))
else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

#Insert into base table the newly pulled data
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=True, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)