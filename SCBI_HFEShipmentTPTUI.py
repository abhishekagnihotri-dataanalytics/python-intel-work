__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads the Shipment throughput time from excel sheet "
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
	data_area = 'HFEShipmentTPT'
	base_table = '[Base].[HFEShipmentTPTUI]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'Sharepoint_Excel'
	source = 'RequiredIPNsAndStockrooms.xlsx'
	destination_db = 'GSCDW'
	shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\HFE_Demand_Supply"
	excel_sheet_name = 'Table_ShipmentTPT_UI'
	excel_file = 'RequiredIPNsAndStockrooms.xlsx'
	
df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)

all_columns = list(df) # Creates list of all column headers
df[all_columns] = df[all_columns].astype(str) # converts all columns to dtype string

drop_top_rows = 3
df.drop(index=df.index[:drop_top_rows],inplace=True)

colIndexList = [0,1,2,3,4,5] # Column numbers to keep
df = df.iloc[:, colIndexList] # Relevant columns kept
df.reset_index(drop=True, inplace=True) # Reset index to start from 0

# Column names and dtypes changed
df.columns.values[:] = ["country","site","min_supp_tpt_wk","max_supp_tpt_wk","max_supp_sh_tpt_wk","min_supp_sh_tpt_wk"]
df[["min_supp_tpt_wk","max_supp_tpt_wk","max_supp_sh_tpt_wk","min_supp_sh_tpt_wk"]] = df[["min_supp_tpt_wk","max_supp_tpt_wk","max_supp_sh_tpt_wk","min_supp_sh_tpt_wk"]].apply(pd.to_numeric)


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