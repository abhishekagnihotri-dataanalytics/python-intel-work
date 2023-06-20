__author__ = "Shweta Aurangabadkar"
__email__ = "shweta.v.aurangabadkar@intel.com"
__description__ = "This script loads BOM hierarchy from EDW to GSCDW DB on sql2943-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Once Daily at 8PM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Logging import log
from Helper_Functions import queryTeradata, uploadDFtoSQL, executeSQL, executeStoredProcedure
import pandas as pd
from Project_params import params

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


project_name='ATSM'
data_area='BOM Hierarchy'
Table = 'Stage.EDWFlattenedBOMHierarchy'
LoadBy= 'EDW'
source = 'Procurement_Analysis.flattened_bom_hierarchy'

dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('shweta.v.aurangabadkar@intel.com')
query = """WITH RECURSIVE flattened_bom_hierarchy (bom_root, parnt_old_mtrl_id, parnt_itm_rvs_cd, chld_old_mtrl_id, chld_itm_rqr_amt, do_not_expld_ind, bom_itm_assoc_type_cd, depth)
AS
(
SELECT 
    root.parnt_old_mtrl_id AS bom_root, root.parnt_old_mtrl_id, root.parnt_itm_rvs_cd, root.chld_old_mtrl_id, root.chld_itm_rqr_amt, root.do_not_expld_ind, root.bom_itm_assoc_type_cd, 1 as depth
FROM 
    BOM.v_itm_spd itm 
    INNER JOIN BOM.v_dsgn_bom root ON itm.old_mtrl_id = root.parnt_old_mtrl_id AND itm.mfg_itm_rvs_cd = root.parnt_itm_rvs_cd
        WHERE root.parnt_old_mtrl_id 
        IN ( 
            --below items are for Historical Extensions
            SELECT DISTINCT
            A.itm_cd as "IPN"
            FROM pdm_analysis.v_itm_plnt_extn as A
            LEFT OUTER JOIN Procurement_Analysis.v_dim_itm as K ON A.itm_cd = K.itm_trim_id
            LEFT OUTER JOIN Procurement_Analysis.v_dim_itm as E ON A.itm_cd = E.itm_trim_id
            WHERE 
                A.sap_plnt_cd IN( 'AZ04')
            and EXTRACT(YEAR FROM A.itm_extn_cmpl_dt) >= EXTRACT(YEAR FROM DATE)  - 1
            and E.cmdt_dsc_shrt_txt IN ( 'unified assy item','HALB Int Assy','Transport Media')
        )
UNION ALL
SELECT 
    flattened_bom_hierarchy.bom_root, dsgn_bom.parnt_old_mtrl_id, dsgn_bom.parnt_itm_rvs_cd, dsgn_bom.chld_old_mtrl_id, dsgn_bom.chld_itm_rqr_amt, dsgn_bom.do_not_expld_ind, dsgn_bom.bom_itm_assoc_type_cd
, flattened_bom_hierarchy.depth+ 1 as depth
FROM
    flattened_bom_hierarchy
    INNER JOIN BOM.v_dsgn_bom dsgn_bom ON dsgn_bom.parnt_old_mtrl_id = flattened_bom_hierarchy.chld_old_mtrl_id
    INNER JOIN BOM.v_itm_spd itm ON itm.old_mtrl_id = dsgn_bom.parnt_old_mtrl_id AND itm.mfg_itm_rvs_cd = dsgn_bom.parnt_itm_rvs_cd
Where flattened_bom_hierarchy.depth< 3    --just 3 levels
     )
SELECT DISTINCT
    flattened_bom_hierarchy.bom_root ,
    K.itm_dsc as bom_dsc,
    chld_old_mtrl_id ,
    E.itm_dsc ,
    chld_itm_rqr_amt ,
    E.itm_type_dsc,
    E.itm_type_cd ,
    G.itm_rvs_cd ,
    bom_itm_assoc_type_cd ,
    H.itm_rlse_lvl_nm ,
    E.cmdt_dsc_shrt_txt ,
    F.itm_char_val_txt
FROM 
    flattened_bom_hierarchy
    LEFT OUTER JOIN Procurement_Analysis.v_dim_itm as K ON flattened_bom_hierarchy.bom_root  = K.itm_trim_id
     LEFT OUTER JOIN Procurement_Analysis.v_dim_itm as E ON chld_old_mtrl_id = E.itm_trim_id
     LEFT OUTER JOIN item.v_itm_char as F on flattened_bom_hierarchy.bom_root = F.itm_id
    LEFT OUTER JOIN pdm_analysis.v_itm_rvs as G ON chld_old_mtrl_id = G.old_mtrl_id
    LEFT OUTER JOIN pdm_analysis.v_itm_rlse_lvl as H ON G.itm_rlse_lvl_id = H.itm_rlse_lvl_id
WHERE
        (bom_itm_assoc_type_cd in ('A','C','E')
        AND
        E.itm_type_dsc IN('Raw Materials', 'Semifinished Product','Capital, NTM & Spares')
        AND
        F.itm_char_nm IN ('MARKET_CODE_NAME')
     --  AND
       -- "Assembly UPI" = '2000-254-336'
        AND
        h.itm_rlse_lvl_nm not in ('INACTIVE','OBSOLETE','UN QUAL')
        --AND
        --IPN = 'G26000-002'
)"""
#print(query)

df=queryTeradata(query)

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, source,
                                                                          dest_db))

    table_name = '[Base].[EDWFlattenedBOMHierarchy]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[EDWFlattenedBOMHierarchy]")
        Insert_query = """insert into [GSCDW].[Base].[EDWFlattenedBOMHierarchy]
        SELECT *  FROM [GSCDW].[Stage].[EDWFlattenedBOMHierarchy]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base table")
        else:
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[Stage].[EDWFlattenedBOMHierarchy]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[EDWFlattenedBOMHierarchy]")
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

