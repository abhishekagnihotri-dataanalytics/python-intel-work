__author__ = "Khushboo Saboo"
__email__ = "khushboo.saboo@intel.com"
__description__ = "This script loads RBM data into the SQL table"
__schedule__ = "2:05 AM PST each day"

import os
import sys;

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
import numpy
from Helper_Functions import queryTeradata, uploadDFtoSQL, querySQL, querySSAS, executeSQL
from Logging import log
from Project_params import params
import time

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

params['EMAIL_ERROR_RECEIVER'].append('khushboo.saboo@intel.com')  # add to failure email notifications

server_idm = "GSM_IDMReports.intel.com"
model = "GSM_IDMReports"
project = 'Repairable Below Max'
driver = "{ODBC Driver 17 for SQL Server}"

InHouseRepair = "In House Repair"
Disposition0 = "0-No Action"
Disposition1 = "1-Release Repair PO"
Disposition2 = "2-Facilitate NRR Returns"
Disposition3 = "3-Initiate Excess Transfer"
Disposition4 = "4-Release New Buy PO"
Disposition5 = "5-Require CostRep Disposition (NB/Algo/Backorder)"
not_aging = "NotAging(<7days)"
aging = "Aging"
aging_and_not_aging = "AGING and NOT_AGING"

query_date = "select ww_nbr,fscl_yr_nbr,fscl_qtr_nbr,fscl_mo_nbr,day_of_mo_nbr,clndr_dt from [dbo].[Intel_Calendar] where CAST(clndr_dt as Date) = CAST(getUTCDate() as Date) "
query_nrr = """
    select 
    fctry_cust_rtrn_id as cust_rtrn_id, fctry_cust_rtrn_type_nm as cust_rtrn_name, fail_txt as fail_txt,
    last_upd_ts as last_upd_ts,itm_id,strg_loc_cd
    from Inv_Factory_Materials_EIL.v_fctry_cust_rtrn_line_cmpl
    where fctry_cust_rtrn_line_cmpl_ind = 'N' 
    and last_upd_ts > Date - 740
    and fctry_mtrl_srvr_id IN ('AZ1', 'MA1', 'VN1', 'OR1', 'CR1', 'SC1', 'CD1', 'IS1')
    """

query_rbm = """SELECT 
f.itm_id as iitm_id, f.supl_id as supplier, sppl.busns_org_prty_id ,sppl.busns_org_nm,f.purch_grp_cd, f.strg_loc_cd as stockroom_id,f.minm_qty as min_qty, 
COALESCE(f.max_qty,0) as max_qty_a, COALESCE(f.avl_qty,0) as avail_qty, f.rej_qty as reject_qty, f.cust_gl_acct_nbr as itm_gl_number, f.rord_pnt_qty as rop_qty,
f.fctry_mtrl_srvr_id as wiings_loc,COALESCE(f.open_purch_ord_itm_qty,0) as open_purchase_order_qty,COALESCE(f.open_fctry_cust_ord_qty,0) as open_cust_order_qty,
i.itm_dsc as item_desc,
s.curr_supl_itm_nbr as spn,s.rpr_prc_amt as repair_price, s.new_buy_prc_amt as new_buy_price,s.new_buy_prc_usd_amt as new_buy_price_usd,
s.un_cost_amt as unit_cost_amount, s.new_buy_cur_cd as new_buy_currency, s.rpr_prc_cur_cd as repair_price_currency,
s.un_cost_cur_cd as unit_cost_currency, s.actl_lead_tm_day_cnt as actual_lead_time, s.rpr_lead_tm_day_cnt as repair_lead_time,
s.byr_note_txt as buyer_note, s.mchn_type_txt as machine_type, s.itm_cat_txt as category_type,
s.rpr_type_nm as repair_type,
sh.strg_loc_nm as stockroom,
p.purch_grp_nm as purchase_grp_nm,
sup.mfr_nm as supplier_name,
avail_qty +open_purchase_order_qty as Supply,
max_qty_a+open_cust_order_qty as Demand,
Demand - Supply as missing_qty,
(SELECT SUM(fctry_cust_rtrn_qty) 
from (
SELECT itm_id,strg_loc_cd,fctry_cust_rtrn_qty, last_upd_ts , (CURRENT_TIMESTAMP - v_fctry_cust_rtrn_line_cmpl.last_upd_ts) DAY(4) as date_diff
      FROM Inv_Factory_Materials_EIL.v_fctry_cust_rtrn_line_cmpl
      WHERE (v_fctry_cust_rtrn_line_cmpl.fctry_cust_rtrn_line_cmpl_ind = 'N')
      AND (v_fctry_cust_rtrn_line_cmpl.fctry_mtrl_srvr_id IN ('CD1', 'MA1', 'VN1', 'AZ1', 'OR1', 'CR1'))
      AND v_fctry_cust_rtrn_line_cmpl.last_upd_ts > DATE - 740 
      AND date_diff > 14    
) as subQ1
where itm_id = iitm_id and strg_loc_cd = stockroom_id
group by itm_id,strg_loc_cd) as NRR_QTY_AGING,
(SELECT  SUM(fctry_cust_rtrn_qty) 
from (
SELECT itm_id,strg_loc_cd,fctry_cust_rtrn_qty, last_upd_ts , (CURRENT_TIMESTAMP - v_fctry_cust_rtrn_line_cmpl.last_upd_ts) DAY(4) as date_diff
      FROM Inv_Factory_Materials_EIL.v_fctry_cust_rtrn_line_cmpl
      WHERE (v_fctry_cust_rtrn_line_cmpl.fctry_cust_rtrn_line_cmpl_ind = 'N')
      AND (v_fctry_cust_rtrn_line_cmpl.fctry_mtrl_srvr_id IN ('CD1', 'MA1', 'VN1', 'AZ1', 'OR1', 'CR1'))
      AND v_fctry_cust_rtrn_line_cmpl.last_upd_ts > DATE - 740 
      AND date_diff < 14    
) as subQ2
where itm_id = iitm_id and strg_loc_cd = stockroom_id
group by itm_id,strg_loc_cd) as NRR_QTY_NOT_AGING,  
(SELECT min(fctry_cust_ord_line_cre_ts)
FROM    Inv_Factory_Materials_EIL.v_fctry_cust_ord_line
Where fctry_mtrl_srvr_id in ('AZ1', 'MA1', 'VN1', 'OR1', 'CR1', 'SC1', 'CD1', 'IS1')
and Fctry_cust_ord_line_sts_nm in ('Open','In Process')  
and itm_id = iitm_id and strg_loc_cd = stockroom_id ) as oldest_date
from Factory_Materials_Analysis.v_fact_fctry_mtrl_inv_dtl as f
left join Factory_Materials_Analysis.v_itm as i ON f.itm_id = i.itm_id
left join Procurement_Analysis.v_dim_mfr as sup ON f.supl_id = sup.mfr_id
left join Factory_Materials_Analysis.v_dim_fctry_item_strg_loc as s ON f.itm_id = s.itm_id and f.strg_loc_cd = s.strg_loc_cd
left join Factory_Materials_Analysis.v_purch_grp_nm as p ON p.purch_grp_cd = f.purch_grp_cd
left join Factory_Materials_Analysis.v_dim_fctry_strg_loc_hier as sh on sh.strg_loc_cd = f.strg_loc_cd
left join Factory_Materials_Analysis.v_dim_entprs_supl_hier as sppl on f.supl_id = sppl.busns_org_prty_id
where s.itm_strg_loc_sts_ind = 'Y' and s.rpr_type_nm <> 'Non Repairable' 
and missing_qty > 0
and  f.fctry_mtrl_srvr_id in ('AZ1', 'MA1', 'VN1', 'OR1', 'CR1', 'SC1', 'CD1', 'IS1')"""


def setBackorderAgingCategory(row) -> str:
    if row['open_cust_order_qty'] == 0 or pd.isnull(row['oldest_date']):
        return ""
    elif (datetime.now() - row['oldest_date']).days < 8:
        return not_aging
    else:
        return aging


def setInHouseRepair(row) -> str:
    if not pd.isnull(row['category_type']) and InHouseRepair in row['category_type']:
        return InHouseRepair
    else:
        return ""


def setNRR(row) -> str:
    if row['NRR_QTY_AGING'] != 0 and row['NRR_QTY_NOT_AGING'] != 0:
        return aging_and_not_aging
    elif row['NRR_QTY_AGING'] != 0 and row['NRR_QTY_NOT_AGING'] == 0:
        return aging
    elif row['NRR_QTY_AGING'] == 0 and row['NRR_QTY_NOT_AGING'] != 0:
        return not_aging
    else:
        return ""


def setMissingRejectQty(row) -> int:
    if row['Demand'] - (row['Supply'] + row['reject_qty']) > 0:
        return row['Demand'] - (row['Supply'] + row['reject_qty'])
    else:
        return 0


def setMissingRejectNRRQty(row) -> int:
    if row['Demand'] - (row['Supply'] + row['reject_qty'] + row['NRRQuantity']) > 0:
        return row['Demand'] - (row['Supply'] + row['reject_qty'] + row['NRRQuantity'])
    else:
        return 0


def setMissingRejectNRRExcessQty(row) -> int:
    temp = row['Demand'] - (
            row['Supply'] + row['reject_qty'] + row['NRRQuantity'] + row['qry_MSBIdata[Total_Excess_QTY_TF]'])
    if temp > 0:
        return temp
    else:
        return 0


def setFinalMissingQty(row) -> int:
    if row['D0'] != Disposition0:
        return 0
    else:
        return row['Missing_reject_NRR_ExcessQTY']


def setD0(row) -> str:
    if row['InHouseRepair'] == InHouseRepair and row['open_cust_order_qty'] == 0:
        return Disposition0
    else:
        return ""


def setD1(row) -> str:
    if row['D0'] != Disposition0 and row['missing_qty'] > 0 and row['reject_qty'] > 0:
        return Disposition1
    else:
        return ""


def setD2(row) -> str:
    if row['D0'] != Disposition0 and row['MissingQTY_rejectQTY'] > 0 and row['NRRQuantity'] > 0:
        return Disposition2
    else:
        return ""


def setD3(row) -> str:
    if row['D0'] != Disposition0 and row['Missing_reject_NRR'] > 0 and row['qry_MSBIdata[Total_Excess_QTY_TF]'] > 0:
        return Disposition3
    else:
        return ""


def setD4(row) -> str:
    if row['D0'] != Disposition0 and row['Missing_reject_NRR_ExcessQTY'] > 0 and 0 < row['missingAmount'] < 2000 and \
            row['InHouseRepair'] != InHouseRepair:
        return Disposition4
    else:
        return ""


def setD5(row) -> str:
    if row['D0'] != Disposition0 and row['Missing_reject_NRR_ExcessQTY'] > 0 and row['missingAmount'] > 2000:
        return Disposition5
    else:
        return ""


def setD5InHouse(row) -> str:
    if row['InHouseRepair'] == InHouseRepair and row['open_cust_order_qty'] > 0:
        return Disposition5


def setBuyerDisposition(row) -> str:
    if row['D0'] == Disposition0:
        return Disposition0
    else:
        return "{0}{1}{2}{3}{4}".format((Disposition1 if row['D1'] == Disposition1 else ""),
                                        (";" + Disposition2 if row['D2'] == Disposition2 else ""),
                                        (";" + Disposition3 if row['D3'] == Disposition3 else ""),
                                        (";" + Disposition4 if row['D4'] == Disposition4 else ""),
                                        (";" + Disposition5 if row['D5'] == Disposition5 else ""))


def getDateValues():
    # ---------------------------------------------Getting Date Values------------------------------------------------#
    print("Getting Date Values")
    statement_date = query_date
    success, df_date, error_msg = querySQL(statement_date)

    current_work_week = int(df_date['ww_nbr'])
    current_year = int(df_date['fscl_yr_nbr'])
    current_quarter = int(df_date['fscl_qtr_nbr'])
    current_date = df_date['clndr_dt'][0]
    return current_work_week, current_year, current_date, current_quarter


def getNRRData(current_work_week, current_year, current_date, current_quarter):
    # ---------------------------------------------Getting NRR Data----------------------------------------------------#
    print("Getting NRR Data")
    statement_nrr = query_nrr
    df_nrr = queryTeradata(statement_nrr)
    df_nrr['itm_id'] = df_nrr['itm_id'].str[-9:]
    df_nrr['key_N'] = df_nrr['itm_id'].str.cat(df_nrr['strg_loc_cd'])
    df_nrr['key_N'] = pd.to_numeric(df_nrr['key_N'])
    df_nrr['WWNo'] = current_work_week
    df_nrr['YearNo'] = current_year
    df_nrr['QuarterNo'] = current_quarter
    df_nrr['created_date'] = current_date
    return df_nrr


def checkIFNRRDataExists(current_work_week):
    statement_nrr_check = "select count(*) from [at].[NRR] where WWNo = " + str(current_work_week)
    df_n_check = querySQL(statement_nrr_check)
    if df_n_check[0] and int(df_n_check[1].iloc[0]) <= 0:
        return False
    else:  # query succeeded and there is some data matching WWNo
        return True


def insertNRRData(current_year, current_date, current_work_week, current_quarter, flag):
    df_nrr = getNRRData(current_work_week, current_year, current_date, current_quarter)
    columns_nrr = ['cust_rtrn_id', 'cust_rtrn_name', 'fail_txt', 'last_upd_ts', 'itm_id', 'strg_loc_cd', 'key_N',
                   'WWNo', 'YearNo', 'QuarterNo', 'created_date']
    if flag:
        executeSQL("delete from [at].[NRR] where WWno =" + str(current_work_week))
    insert_succeeded, error_msg = uploadDFtoSQL('[at].[NRR]', df_nrr, columns_nrr, truncate=False, driver=driver)
    log(insert_succeeded, project_name=project, data_area='[at].[NRR]', row_count=df_nrr.shape[0], error_msg=error_msg)
    return insert_succeeded


def getRBMData():
    # ---------------------------------------------Getting RBM Data----------------------------------------------------#
    print("Getting RBM Data")
    statement = query_rbm
    df = queryTeradata(statement)
    return df


def transformRBMData(df, current_work_week, current_year, current_date, current_quarter):
    # -------------------------- ETL Operations -------------------------- #
    df['item_id'] = df['iitm_id'].str[-9:]
    df['key'] = df['item_id'].str.cat(df['stockroom_id'])
    df['key'] = pd.to_numeric(df['key'])
    df['RepairableTag'] = "Repairable Below Max"
    df = df.assign(BackorderAgingCategory=df.apply(setBackorderAgingCategory, axis=1))
    df = df.assign(InHouseRepair=df.apply(setInHouseRepair, axis=1))
    df['NRR_QTY_AGING'] = df['NRR_QTY_AGING'].replace(numpy.nan, 0)
    df['NRR_QTY_NOT_AGING'] = df['NRR_QTY_NOT_AGING'].replace(numpy.nan, 0)
    df = df.assign(NRR=df.apply(setNRR, axis=1))
    df['NRRQuantity'] = df['NRR_QTY_AGING'] + df['NRR_QTY_NOT_AGING']
    df = df.assign(MissingQTY_rejectQTY=df.apply(setMissingRejectQty, axis=1))
    df = df.assign(Missing_reject_NRR=df.apply(setMissingRejectNRRQty, axis=1))

    # -------------------------- Merging Data from Tabular Models -------------------------- #
    df_idm_reports = getIDMReportsData()
    df = df.merge(df_idm_reports, how='left', left_on='key', right_on='keyIDM')
    df['qry_MSBIdata[Total_Excess_QTY_TF]'] = df['qry_MSBIdata[Total_Excess_QTY_TF]'].replace(numpy.nan, 0)
    df = df.assign(Missing_reject_NRR_ExcessQTY=df.apply(setMissingRejectNRRExcessQty, axis=1))
    df['missingAmount'] = df['new_buy_price_usd'] * df['Missing_reject_NRR_ExcessQTY']

    # --------------------------  Calculating Dispositions -------------------------- #
    df = df.assign(D0=df.apply(setD0, axis=1))
    df = df.assign(D1=df.apply(setD1, axis=1))
    df = df.assign(D2=df.apply(setD2, axis=1))
    df = df.assign(D3=df.apply(setD3, axis=1))
    df = df.assign(D5=df.apply(setD5InHouse, axis=1))
    df = df.assign(D4=df.apply(setD4, axis=1))
    df = df.assign(D5=df.apply(setD5, axis=1))
    df = df.assign(BuyerDisposition=df.apply(setBuyerDisposition, axis=1))
    df = df.assign(final_missing_qty=df.apply(setFinalMissingQty, axis=1))
    df['WWNo'] = current_work_week
    df['YearNo'] = current_year
    df['created_date'] = current_date
    df['QuarterNo'] = current_quarter
    df.rename(columns={"qry_MSBIdata[Total_Excess_QTY_TF]": "Total_Excess_QTY_TF",
                       "qry_MSBIdata[Stockroom_Scope_TF]": "Stockroom_Scope_TF",
                       "qry_MSBIdata[Single_Site_TF]": "Single_Site_TF",
                       "qry_MSBIdata[Excess_Status_TF]": "Excess_Status_TF",
                       "qry_MSBIdata[Factory_Name]": "FactoryName",
                       "qry_MSBIdata[Harvest_Stockroom_Indicator]": "HarvestStockroomIndicator",
                       "qry_MSBIdata[Usage_Frequency_Class]": "UsageFrequencyClass",
                       "qry_MSBIdata[Eng_Stkrm_Flag]": "EngStkrmFlag",
                       "qry_MSBIdata[EOS/EOL_Flag]": "EOS/EOL Flag",
                       "qry_MSBIdata[Insurance_Flag]": "InsuranceFlag",
                       "qry_MSBIdata[PSI_Flag]": "PSIFlag",
                       "qry_MSBIdata[UZB_DOI_Group2]": "UZB_DOIGroup2",
                       "qry_MSBIdata[VF_UZB_DOI_Group]": "VF_UZB_DOIGroup",
                       "qry_MSBIdata[Team]": "Team",
                       "wiings_loc": "site_loc", "key": "key_P",
                       "oldest_date": "BackorderDate"}, inplace=True)
    df_filtered = df.filter(
        ['WWNo', 'iitm_id', 'supplier', 'busns_org_prty_id', 'busns_org_nm', 'purch_grp_cd', 'stockroom_id',
         'min_qty', 'max_qty_a', 'avail_qty', 'reject_qty', 'itm_gl_number', 'rop_qty', 'site_loc',
         'open_purchase_order_qty', 'open_cust_order_qty', 'item_desc', 'spn', 'repair_price', 'new_buy_price',
         'new_buy_price_usd', 'unit_cost_amount', 'new_buy_currency', 'repair_price_currency',
         'unit_cost_currency',
         'actual_lead_time', 'repair_lead_time', 'buyer_note', 'machine_type', 'category_type', 'repair_type',
         'stockroom', 'purchase_grp_nm', 'supplier_name', 'key_P', 'RepairableTag', 'BackorderAgingCategory',
         'InHouseRepair', 'NRR_QTY_AGING', 'NRR_QTY_NOT_AGING', 'NRR', 'NRRQuantity', 'MissingQTY_rejectQTY',
         'Missing_reject_NRR', 'Total_Excess_QTY_TF',
         'missingAmount', 'D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'BuyerDisposition', 'Stockroom_Scope_TF',
         'Single_Site_TF', 'Excess_Status_TF', 'FactoryName', 'HarvestStockroomIndicator', 'UsageFrequencyClass',
         'EngStkrmFlag', 'EOS/EOL Flag', 'InsuranceFlag', 'PSIFlag', 'UZB_DOIGroup2', 'VF_UZB_DOIGroup', 'Team',
         'Missing_reject_NRR_ExcessQTY', 'YearNo', 'created_date', 'BackorderDate', 'missing_qty',
         'final_missing_qty', 'QuarterNo'])
    return df_filtered


def checkIfRBMDataExists(current_work_week):
    # --------------------------Check for current workweek and flush out data--------------------------------#
    statement_check = "select count(*) from [at].[RepairableBelow] where WWno =" + str(current_work_week)
    success_bool, df_check, error_ms = querySQL(statement_check)
    if int(df_check.iloc[0]) <= 0:
        return False
    else:
        return True


def insertRBMData(flag, current_work_week, current_year, current_date, current_quarter):
    df = getRBMData()
    df_filtered = transformRBMData(df, current_work_week, current_year, current_date, current_quarter)

    if flag:
        executeSQL("delete from [at].[RepairableBelow] where WWno =" + str(current_work_week))

    # -------------------------- Insert in SQL Table -------------------------- #
    columns = ['WWNo', 'iitm_id', 'supplier', 'busns_org_prty_id', 'busns_org_nm', 'purch_grp_cd', 'stockroom_id',
               'min_qty', 'max_qty_a', 'avail_qty', 'reject_qty', 'itm_gl_number', 'rop_qty', 'site_loc',
               'open_purchase_order_qty', 'open_cust_order_qty', 'item_desc', 'spn', 'repair_price', 'new_buy_price',
               'new_buy_price_usd', 'unit_cost_amount', 'new_buy_currency', 'repair_price_currency',
               'unit_cost_currency',
               'actual_lead_time', 'repair_lead_time', 'buyer_note', 'machine_type', 'category_type', 'repair_type',
               'stockroom', 'purchase_grp_nm', 'supplier_name', 'key_P', 'RepairableTag', 'BackorderAgingCategory',
               'InHouseRepair', 'NRR_QTY_AGING', 'NRR_QTY_NOT_AGING', 'NRR', 'NRRQuantity', 'MissingQTY_rejectQTY',
               'Missing_reject_NRR', 'Total_Excess_QTY_TF',
               'missingAmount', 'D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'BuyerDisposition', 'Stockroom_Scope_TF',
               'Single_Site_TF', 'Excess_Status_TF', 'FactoryName', 'HarvestStockroomIndicator', 'UsageFrequencyClass',
               'EngStkrmFlag', 'EOS/EOL Flag', 'InsuranceFlag', 'PSIFlag', 'UZB_DOIGroup2', 'VF_UZB_DOIGroup', 'Team',
               'Missing_reject_NRR_ExcessQTY', 'YearNo', 'created_date', 'BackorderDate', 'missing_qty',
               'final_missing_qty', 'QuarterNo']
    success_bool, error_msg = uploadDFtoSQL('[at].[RepairableBelow]', df_filtered, columns, truncate=False,
                                            driver=driver)
    log(success_bool, project_name=project, data_area='[at].[RepairableBelow]', row_count=df_filtered.shape[0],
        error_msg=error_msg)
    return success_bool


def getIDMReportsData():
    query = """EVALUATE
SUMMARIZECOLUMNS(
    qry_MSBIdata[Item ID],
    qry_MSBIdata[Stockroom ID],
    qry_MSBIdata[Team],
    qry_MSBIdata[Stockroom_Scope_TF],
    qry_MSBIdata[Total_Excess_QTY_TF],
    qry_MSBIdata[Single_Site_TF],
    qry_MSBIdata[Excess_Status_TF],
    qry_MSBIdata[Factory Name],
    qry_MSBIdata[Harvest Stockroom Indicator],
    qry_MSBIdata[Usage Frequency Class],
    qry_MSBIdata[Eng Stkrm Flag],
    qry_MSBIdata[EOS/EOL Flag],
    qry_MSBIdata[Insurance Flag],
    qry_MSBIdata[PSI Flag],
    qry_MSBIdata[UZB_DOI Group2],
    qry_MSBIdata[VF_UZB_DOI Group],
    KEEPFILTERS(FILTER(ALL(qry_MSBIdata[Repair Type]),qry_MSBIdata[Repair Type]<>"Non Repairable"))
)
ORDER BY 
    qry_MSBIdata[Item ID] ASC,
    qry_MSBIdata[Stockroom ID] ASC,
    qry_MSBIdata[Team] ASC,
    qry_MSBIdata[Stockroom_Scope_TF] ASC,
    qry_MSBIdata[Total_Excess_QTY_TF] ASC,
    qry_MSBIdata[Single_Site_TF] ASC,
    qry_MSBIdata[Excess_Status_TF] ASC,
    qry_MSBIdata[Factory Name] ASC,
    qry_MSBIdata[Harvest Stockroom Indicator] ASC,
    qry_MSBIdata[Usage Frequency Class] ASC,
    qry_MSBIdata[Eng Stkrm Flag] ASC,
    qry_MSBIdata[EOS/EOL Flag] ASC,
    qry_MSBIdata[Insurance Flag] ASC,
    qry_MSBIdata[PSI Flag] ASC,
    qry_MSBIdata[UZB_DOI Group2] ASC,
    qry_MSBIdata[VF_UZB_DOI Group] ASC
"""

    df_idm = querySSAS(query, server_idm, model)
    df_idm['keyIDM'] = df_idm['qry_MSBIdata[Item ID]'].str.cat(df_idm['qry_MSBIdata[Stockroom ID]'])
    df_idm['keyIDM'] = pd.to_numeric(df_idm['keyIDM'], errors='coerce')
    df_idm.columns = df_idm.columns.str.replace(' ', '_')
    df_idm['qry_MSBIdata[Total_Excess_QTY_TF]'] = df_idm['qry_MSBIdata[Total_Excess_QTY_TF]'].replace(numpy.nan, 0)
    return df_idm


if __name__ == "__main__":
    start = time.time()
    # Get Current Date Values
    current_work_week, current_year, current_date, current_quarter = getDateValues()

    # Insert NRR Data
    nrr_exists_flag = checkIFNRRDataExists(current_work_week)
    insertNRRData(current_year, current_date, current_work_week, current_quarter, nrr_exists_flag)

    # Insert RBM Data
    rbm_exists_flag = checkIfRBMDataExists(current_work_week)
    insertRBMData(rbm_exists_flag, current_work_week, current_year, current_date, current_quarter)
    end = time.time()
    print("Execution Time", (end - start) // 60)
