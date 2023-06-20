__author__ = "Khushboo Saboo"
__email__ = "khushboo.saboo@intel.com"
__description__ = "Populates CommodityHierarchy and CostCenterHierarchy from Hana into sql3266-fm1-in.amr.corp.intel.com,3181 "
__schedule__ = " N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Helper_Functions import uploadDFtoSQL, map_columns, queryHANA
from Logging import log, log_warning


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # params['EMAIL_ERROR_RECEIVER'].append('khushboo.saboo@intel.com')

    ### BEGIN Cost Center Hierarchy Load ###
    project_name = 'Spends'
    data_area = 'Cost Center Hierarchy'
    table = 'Base.CostCenterHierarchy'

    cost_center_query = """
    SELECT "FinanceHierarchyClassCd"
          ,"FinanceHierarchyClassNm"
          ,"FiscalYearNbr"
          ,"FiscalMonthNbr"
          ,"SuperGroupCd"
          ,"SuperGroupNm"
          ,"SuperGroupDsc"
          ,"SuperGroupBusinessDsc"
          ,"GroupCd"
          ,"GroupNm"
          ,"GroupDsc"
          ,"GroupBusinessDsc"
          ,"DivisionCd"
          ,"DivisionNm"
          ,"DivisionDsc"
          ,"DivisionBusinessDsc"
          ,"ProfitCenterCd"
          ,"ProfitCenterNm"
          ,"CostCenterCd"
          ,"CostCenterNm"
          ,"CostCenterCompanyCd"
          ,"CostCenterDsc"
    FROM "_SYS_BIC"."c.Finance/CostCenterHierarchy"(
        'PLACEHOLDER' = ('$$IP_CostCenterHierarchyVersionCd$$', 'OPS0000.00')
    )
    """
    df = queryHANA(cost_center_query, environment='Production', single_sign_on=True)  # Use environment='Development' for NBI, single_sign_on=False for SYSH_SCES_OPSRPT account
    if len(df.index) == 0:
        print('Unable to read data from HANA')
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
    else:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # map_columns(table, df)

        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error
        if insert_succeeded:  # Query returned no rows
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
    ### END Cost Center Hierarchy Load ###

    ### BEGIN Commodity Hierarchy Load ###
    data_area = 'Commodity Hierarchy'
    table = 'Base.CommodityHierarchy'

    commodity_query = """
    SELECT "HighLevelSupplyChainOrganizationNm"
          ,"HighLevelSupplyChainOrganizationCd"
          ,"SourcingOrganizationNm"
          ,"SourcingOrganizationCd"
          ,"SpendsCategoryNm"
          ,"SpendsCategoryCd"
          ,"CommodityNm"
          ,"CommodityCd"
          ,"CommodityBusinessAreaNm" 
    FROM "_SYS_BIC"."d.SelfService.Commodity/CommodityHierarchy"
    --WHERE "CommodityEffectiveEndDt" >= CURRENT_DATE
    """
    df = queryHANA(commodity_query, environment='Production', single_sign_on=True)  # Use single_sign_on=False for SYSH_SCES_OPSRPT account
    if len(df.index) == 0:
        print('Unable to read data from HANA')
        # log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
    else:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        map_columns(table, df)

        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error
        if insert_succeeded:  # Query returned no rows
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
    ### END Cost Center Hierarchy Load ###
