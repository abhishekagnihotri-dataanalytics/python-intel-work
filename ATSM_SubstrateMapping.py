__author__ = "Chaitra Venkatesan"
__email__ = "chaitra.venkatesan@intel.com"
__description__ = "This script combines Substrate/AssemblyUPI with IPN/AssemblyUPI data for iDOI from HANA to GSCDW DB"

import os
import sys;

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeSQL, executeStoredProcedure
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
    project_name = 'Substrate/AssemblyUPI/IPN data'
    data_area = 'iDOI'
    base_table = 'Base.MappsSubstrateAssemblyUPIIPN'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('chaitra.venkatesan@intel.com')

    query = """
        with a as (with cte as (select distinct b1."EnterpriseParentItemId" as "RootItemId"
       , case 
              when i1."CommodityCd" in ('2540', '7711') then b1."EnterpriseChildItemId"
              when i2."CommodityCd" in ('2540', '7711') then b2."EnterpriseChildItemId"
              when i3."CommodityCd" in ('2540', '7711') then b3."EnterpriseChildItemId"
         else '' end as "IPN"
       from "_SYS_BIC"."b.Item/ItemDetail" i0
       join "_SYS_BIC"."b.Item/ItemBillOfMaterial" b1
              on b1."EnterpriseParentItemId" = i0."EnterpriseItemId"
              and b1."ParentItemRevisionCd" = i0."CurrentRevisionCd"
       join "_SYS_BIC"."b.Item/ItemDetail" i1
              on i1."EnterpriseItemId" = b1."EnterpriseChildItemId"
       join "_SYS_BIC"."b.Item/ItemBillOfMaterial" b2
              on b2."EnterpriseParentItemId" = b1."EnterpriseChildItemId"
              and b2."ParentItemRevisionCd" = i1."CurrentRevisionCd"
       join "_SYS_BIC"."b.Item/ItemDetail" i2
              on i2."EnterpriseItemId" = b2."EnterpriseChildItemId"
       join "_SYS_BIC"."b.Item/ItemBillOfMaterial" b3
              on b3."EnterpriseParentItemId" = b2."EnterpriseChildItemId"
              and b3."ParentItemRevisionCd" = i2."CurrentRevisionCd"
       join "_SYS_BIC"."b.Item/ItemDetail" i3
              on i3."EnterpriseItemId" = b3."EnterpriseChildItemId"
       --where i0."EnterpriseItemId" IN ('2000-279-905','2000-094-003')
       where (
              i1."CommodityCd" IN ('2540', '7711') or
              i2."CommodityCd" IN ('2540', '7711') or
              i3."CommodityCd" IN ('2540', '7711')
       )
       )
    select * from cte 
    where "RootItemId" like '200%')

    SELECT distinct "SubstrateItemId", "AssyItemId", a."IPN"
    FROM "_SYS_BIC"."intel.scidp.supply.public/MaterialsPlanningSubstrateAssemblyMappingView" b
    inner join a on b."AssyItemId" = a."RootItemId"
            """
    df = queryHANA(query, environment='Production')

    print(dict(df.dtypes))

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = load_by

    # upload dataframe to SQL
    insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, load_by,
                                                                              params['GSMDW_DB']))
        # Execute SP to update [Integ].[SubstrateMapping] table
        sp_name = 'ETL.spLoadSubstrateMapping'
        update_succeeded, error_msg = executeStoredProcedure(sp_name)
        if update_succeeded:
            print("Successfully updated table [Integ].[SubstrateMapping]")
        else:
            log(update_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)
