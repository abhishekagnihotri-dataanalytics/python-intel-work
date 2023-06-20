__author__ = "Akshay Singh"
__email__ = "akshay.singh@intel.com"
__description__ = "This script loads IPN details from EDW required for the HFE project"
__schedule__ = "Every Saturday Once at 4:30PM IST GAR"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryTeradata, uploadDFtoSQL, map_columns, executeStoredProcedure, loadExcelFile
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
	data_area = 'HFEIPNs'
	base_table = '[Base].[EDWHFEIPNUI]'
	params['EMAIL_ERROR_RECEIVER'].append('akshay.singh@intel.com')
	load_by = 'EDW and Sharepoint Excel'
	source = 'FACTORY_MATERIALS_ANALYSIS.v_itm'
	destination_db = 'GSCDW'
	shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\HFE_Demand_Supply"
	excel_sheet_name = 'Table_IPN_UI'
	excel_file = 'RequiredIPNsAndStockrooms.xlsx'
	

df_IPNList = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)
print(df_IPNList)
# converting one column to string dtype
df_IPNList['itm_id'] = df_IPNList['itm_id'].astype(str)

# width of output string, Eg: 000000000500029003
width = 18
# calling method and overwriting series using padding of zeroes
df_IPNList['itm_id'] = df_IPNList['itm_id'].str.zfill(width)
#Conversion to tuple
dftotuple = tuple(df_IPNList['itm_id'])


query = """
SELECT itm_id, itm_dsc
FROM FACTORY_MATERIALS_ANALYSIS.v_itm
where itm_id IN """+str(dftotuple)

df = queryTeradata(query) #(server='TDPRD1.intel.com', credentials=None)

#adding a column by performing an inner join
merged_df = pd.merge(left=df_IPNList, right=df, left_on='itm_id', right_on='itm_id')

merged_df['loaddtm'] = pd.to_datetime('today')
merged_df['loadby'] = load_by

map_columns(base_table,merged_df)

# Clear base table before attempting to copy data 
sp_name = '[ETL].[spTruncateTable]'
truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if truncate_succeeded:
	print("Successfully truncated table {}".format(base_table))
else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=merged_df.shape[0], error_msg=error_msg)

##Insert into base table the freshly pulled data 
insert_succeeded, error_msg = uploadDFtoSQL(base_table, merged_df, truncate=True, server='sql2943-fm1-in.amr.corp.intel.com,3181', database='gscdw')
if insert_succeeded:
	print('Successfully copied {0} records in {1} from {2} to {3}'.format(merged_df.shape[0], base_table, source, destination_db))
else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)