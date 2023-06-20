__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Worker Organization data from HANA to GSCDW DB"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeSQL, executeStoredProcedure,map_columns
import pandas as pd
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'SCBI Core Data'
    data_area = 'Worker'
    stage_table = 'Stage.Worker'
    base_table = 'Base.Worker'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

    query = """SELECT
                "WV"."WWID",
                "EW"."IDSID",
                "EW"."WorkerFullNm",
                "EW"."CorporateEmailTxt",
                "WV"."WorkerStatusCd",
                "WV"."WorkerTypeCd",
                "WV"."EmployeeClassCode",
                "EW"."EmployeeTypeCd",
                "WV"."JobCd",
                "EW"."JobTypeNm",
                "EW"."ManagerWWID",
                "EW"."OrgUnitCd",
                "EW"."OrgUnitNm",
                "OH"."DepartmentLevel3Cd",
                "OH"."DepartmentLevel3Nm",
                "OH"."DepartmentLevel4Cd",
                "OH"."DepartmentLevel4Nm",
                "OH"."DepartmentLevel5Cd",
                "OH"."DepartmentLevel5Nm",
                "OH"."DepartmentLevel6Cd",
                "OH"."DepartmentLevel6Nm",
                "OH"."DepartmentLevel7Cd",
                "OH"."DepartmentLevel7Nm",
                "OH"."DepartmentLevel8Cd",
                "OH"."DepartmentLevel8Nm",
                "OH"."DepartmentLevel9Cd",
                "OH"."DepartmentLevel9Nm",
                "OH"."DepartmentLevel10Cd",
                "OH"."DepartmentLevel10Nm"
                FROM  "_SYS_BIC"."intel.masterdata.worker/WorkerView" "WV"
                left join "_SYS_BIC"."intel.masterdata.worker/EnterpriseWorkerView" "EW" on "WV"."WWID" = "EW"."WWID"
                left join "_SYS_BIC"."intel.masterdata.worker/EnterpriseOrgHierarchyView" "OH" on  "EW"."OrgUnitCd"="OH"."DepartmentCd"
                where  "WV"."WorkerStatusCd" in ('A','L','P') 
            """
    df = queryHANA(query, environment='Production')

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = load_by

   # map_columns(stage_table,df)

    # upload dataframe to SQL
    insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, load_by, params['GSMDW_DB']))

        # Clear base table before attempting to copy data from staging there
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table)
        if truncate_succeeded:
            print("Successfully truncated table {}".format(base_table))

            # Copy data from Stage table to Base table
            insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table, copy_from=stage_table)
            insert_succeeded, error_msg = executeSQL(insert_query)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # log regardless of success or failure
            if insert_succeeded:
                print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table, copy_from=stage_table))

                # Clear stage table after successful insert into Base table
                truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table)
                if truncate_succeeded:
                    print("Successfully truncated table {}".format(stage_table))
                else:
                    log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
