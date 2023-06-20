__author__ = "Dan.K"
__email__ = "William.D.Kniseley@intel.com"
__description__ = "This script loads DDM Pricing Data from EDW to GSCDW DB"
__schedule__ = "Once Daily at 830AM"

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
data_area='DM Pricing'
Table = '[Stage].[brdATSMEDWDMPricingData]'
LoadBy = 'EDW'
source = 'Inv_Factory_Materials_EIL'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('William.D.Kniseley@intel.com')

query = """SELECT 
'Standard Cost' as "Pricing Model",
MAX(std_cst.asof_src_ts) as "Last Updated",
TRIM(std_cst.itm_id) as itm_id,
std_cst.plnt_cd as plnt_code,
CAST(Average(tot_prc * 
(CASE WHEN cur_cd = 'USD' then 1 else 
(CASE WHEN cur_cd = 'USD' then 1 else forex_sch_wk.calc_exchg_rte_pct end)
end)
/ (CASE WHEN plnt_code = 'VNA2' THEN 1 else prc_un END)) as DECIMAL(18,6)) as "Unit Cost (USD)",
"Unit Cost (USD)" * 100 as "Unit Cost x 100 (USD)"
from inv_direct_material_eil.v_dm_inv_eoh_curr as std_cst
LEFT OUTER JOIN Procurement_Analysis.v_dim_clndr_day as clndr
on std_cst.asof_src_ts = clndr.clndr_dt
LEFT OUTER JOIN 
(SELECT forex_sch.*, clndr.fscl_yr_ww_nbr
FROM procurement_analysis.v_dim_cur_rte_shr as forex_sch
LEFT OUTER JOIN Procurement_Analysis.v_dim_clndr_day as clndr 
on clndr.clndr_dt = forex_sch.exchg_rte_vld_fr_dt) as forex_sch_wk
on ( clndr.fscl_yr_ww_nbr = forex_sch_wk.fscl_yr_ww_nbr 
and
std_cst.cur_cd = forex_sch_wk.cur_fr_cd
AND
forex_sch_wk.cur_to_cd = 'usd'	)	
where 
std_cst.plnt_cd IN ('AZ04','AZ08','VNA2' ,'MYA8','MYB2','CNB1','MYA3','OR01','OR10','OR06','CRA2','CRA7')
and 
std_cst.asof_src_ts > ADD_MONTHS(DATE - EXTRACT(DAY From DATE) +1, -84)
and
itm_stndr_prc_amt > 0
GROUP BY
"Pricing Model", itm_id, plnt_code
UNION ALL
SELECT DISTINCT
'Weighted Average' as "Pricing Model",
DATE as "Last Updated",
C.itm_id,
C.plnt_code,
SUM(C.unit_price*(C.qty_received/C.ttl_qty)) as "Unit Cost (USD)",
"Unit Cost (USD)" * 100 as "Unit Cost x 100 (USD)"
FROM
(
SELECT 
A.plnt_id, 
A.mtrl_id,
A.unit_price,
A.qty_received,
(A.unit_price * (A.qty_received / NULLIFZERO(B.ttl_qty))) as wght_prc,
b.*
FROM 
(
SELECT DISTINCT
plnt_id,
mtrl_id,
SUM(purch_line_invc_qty) as qty_received,
(purch_line_invc_usd_amt/NULLIFZERO(purch_line_invc_qty)) as unit_price
FROM 
Procurement_Analysis_nrs.v_fact_purch_ord_line
WHERE
purch_line_invc_sts_cd IN ( 'Complete' , 'PARTIAL' )
AND
purch_org_cd IN ( '0039', '0047', '0052', '0054', '0049')
AND
plnt_id IN ('AZ04','AZ08','VNA2' ,'MYA8','MYB2','CNB1','MYA3','OR01','OR10','OR06','CRA2','CRA7')
AND
purch_doc_line_del_ind = 'N'
AND
purch_line_frst_aprv_dt > ADD_MONTHS(DATE - EXTRACT(DAY From DATE) +1, -84)
AND
mtrl_id <> '*'
GROUP BY
plnt_id,
mtrl_id,
unit_price
) as A
LEFT OUTER JOIN
(
SELECT DISTINCT
plnt_id as plnt_code,
TRIM(mtrl_id) as itm_id,
SUM(purch_line_invc_qty) as ttl_qty
FROM 
Procurement_Analysis_nrs.v_fact_purch_ord_line
WHERE
purch_line_invc_sts_cd IN ( 'Complete' , 'PARTIAL' )
AND
purch_org_cd IN ( '0039', '0047', '0052', '0054', '0049')
AND
plnt_id IN ('AZ04','AZ08','VNA2' ,'MYA8','MYB2','CNB1','MYA3','OR01','OR10','OR06','CRA2','CRA7')
AND
purch_doc_line_del_ind = 'N'
AND
purch_line_frst_aprv_dt > ADD_MONTHS(DATE - EXTRACT(DAY From DATE) +1, -84)
AND
itm_id <> '*'
GROUP BY
plnt_code,
itm_id
) as B
ON 
A.plnt_id = B.plnt_code
AND
A.mtrl_id = B.itm_id
) as C
GROUP BY
C.plnt_code,
C.itm_id
UNION ALL
SELECT 
'WIINGs FMV' as "Pricing Model",
DATE as "Last Updated",
sap_itm.itm_trim_id as itm_id,
CASE 
WHEN fctry_mtrl_srvr_id ='OR1' THEN 'AL'
WHEN fctry_mtrl_srvr_id ='CD1' THEN 'CD'
ELSE fctry_mtrl_srvr_id END as plnt_code,
AVERAGE(fair_mkt_val_amt) as "Unit Cost (USD)",
"Unit Cost (USD)" * 100 as "Unit Cost x 100 (USD)"
FROM 
Inv_Factory_Materials_EIL.v_fact_fctry_mtrl_trns as wiings
LEFT OUTER JOIN Procurement_Analysis.v_dim_itm as sap_itm ON wiings.itm_id = sap_itm.itm_id
WHERE 
strg_loc_cd IN ('107', '15')
AND fctry_trns_ts > ADD_MONTHS(DATE - EXTRACT(DAY From DATE) +1, -72)
AND fctry_trns_rsn_nm IN ( 'Scrap', 'Scrap - Fail on Install', 'Scrap - Obsolete', 'Scrap - Other', 'Scrap - Repair')
GROUP BY fctry_mtrl_srvr_id, sap_itm.itm_trim_id
           """


df=queryTeradata(query)

# print(df.dtypes)
# print(df)

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, source,
                                                                          dest_db))

    table_name = '[Base].[brdATSMEDWDMPricingData]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[brdATSMEDWDMPricingData]")
        Insert_query = """insert into [GSCDW].[Base].[brdATSMEDWDMPricingData]
        SELECT *  FROM [GSCDW].[Stage].[brdATSMEDWDMPricingData]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base")
        else:
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[Stage].[brdATSMEDWDMPricingData]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[brdATSMEDWDMPricingData]")
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)


else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)

