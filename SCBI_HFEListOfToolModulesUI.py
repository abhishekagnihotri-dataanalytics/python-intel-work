__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads Tool Module Master Table used in HFE from excel file"
__schedule__ = "Every Saturday Once at 4:30PM IST GAR"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import  uploadDFtoSQL, map_columns, executeStoredProcedure, loadExcelFile
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
	data_area = 'ListOfToolModules'
	base_table = '[Base].[HFEListOfToolModulesUI]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'Sharepoint_Excel'
	source = 'RequiredIPNsAndStockrooms.xlsx'
	destination_db = 'GSCDW'
	shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\HFE_Demand_Supply"
	excel_sheet_name = 'Table_List_of_Tool_Modules_UI'
	excel_file = 'RequiredIPNsAndStockrooms.xlsx'
	
	
df_ListOfToolModules = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)

#Appropriate column names
df_ListOfToolModules.columns.values[:] = ["Module_CEID","CEID_first_three_Characters","Use_HFE?","wkly_sus_cons_tool_L","iq_cons_tool_L","wkly_sus_cons_tool_lbs","iq_cons_tool_lbs"]

#Converting given selected column values to dtype numeric and rounding them off
cols = ['iq_cons_tool_L','wkly_sus_cons_tool_lbs','iq_cons_tool_lbs']
pd.to_numeric(cols, errors='coerce')
df_ListOfToolModules[cols] = df_ListOfToolModules[cols].round(3)

df_ListOfToolModules['CEID_first_three_Characters'] = df_ListOfToolModules['CEID_first_three_Characters'].apply(lambda x: x.replace(',', ''))
df_ListOfToolModules['CEID_first_three_Characters'] = df_ListOfToolModules['CEID_first_three_Characters'].apply(lambda x: x.replace("'", ''))
# df_ListOfToolModules = df_ListOfToolModules.fillna("")


df_ListOfToolModules['loaddtm'] = pd.to_datetime('today')
df_ListOfToolModules['loadby'] = load_by

map_columns(base_table,df_ListOfToolModules)

# Clear base table before attempting to copy data 
sp_name = '[ETL].[spTruncateTable]'
truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if truncate_succeeded:
	print("Successfully truncated table {}".format(base_table))
else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df_ListOfToolModules.shape[0], error_msg=error_msg)

#Insert into base table the newly pulled data
insert_succeeded, error_msg = uploadDFtoSQL(base_table, df_ListOfToolModules, truncate=True, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(df_ListOfToolModules.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df_ListOfToolModules.shape[0], error_msg=error_msg)