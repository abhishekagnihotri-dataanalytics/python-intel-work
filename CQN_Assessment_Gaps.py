__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads from the MSAT database to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Twice daily at 10:25 AM and 4:25 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import pypyodbc as pyodbc
from datetime import datetime
from Helper_Functions import uploadDFtoSQL, querySQL, executeStoredProcedure
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    table = "msat.GapsWeeklyStg"
    project_name = "GSM_Quality"
    data_area = 'MSAT Assessment Gaps'

    sqlQuery = """SELECT DISTINCT [gapID]
                                 ,[assessmentID]
                                 ,[scrID]
                                 ,[scorecardID]
                                 ,[siteID]
                                 ,[cmdtyName]
                                 ,[cmdtyID]
                                 ,[supName]
                                 ,[elementID]
                                 ,[elementName]
                                 ,[idsid]
                                 ,[siteName]
                                 ,[dispID]
                                 ,[dispositionName]
                                 ,[TPT]
                                 ,[gapTypeID]
                                 ,[gapType]
                                 ,[validationTypeID]
                                 ,[validationType]
                                 ,[GCP Cnt]
                                 ,[srtEntryDt]
                                 ,[entryWW]
                                 ,[srtDueDt]
                                 ,[srtCompletionDt]
                                 ,[IsKey] 
                    FROM [MSAT].[dbo].[v_TMEGaps]"""

    WWQuery = """SELECT DISTINCT [fscl_yr_ww_nbr]
                               , [clndr_dt] 
                 FROM [dm].[DimIntelDate]
                 WHERE clndr_dt = CONVERT(DATE, GETDATE())"""

    success_bool, df, error_msg = querySQL(statement=sqlQuery, server="sql1081-fm1-sz.ed.cps.intel.com,3181", database="MSAT")
    if not success_bool:
        print(error_msg)
        log(success_bool, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        ww_success_bool, ww_df, ww_error_msg = querySQL(statement=WWQuery)
        if not ww_success_bool:
            print(error_msg)
            log(ww_success_bool, project_name=project_name, data_area=data_area, error_msg=ww_error_msg)
        else:
            df['YYYYWW'] = ww_df['fscl_yr_ww_nbr'][0]
            df['LoadDT'] = datetime.now()

            success, error_msg = uploadDFtoSQL(table=table, data=df, columns=df.columns.values.tolist(), truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(success, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error
            if not success:
                print(error_msg)
            else:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

                try:
                    success, error_msg = executeStoredProcedure(procedure_name="qlty.MSATGapsWeeklyMerge")
                except pyodbc.DataError as error:
                    success = False
                    error_msg = error
                finally:
                    log(success, project_name=project_name, package_name="SQL: qlty.MSATGapsWeeklyMerge", data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
