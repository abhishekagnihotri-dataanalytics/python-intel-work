__author__ = "Sankha"
__email__ = "sankhax.subhra.ghosh@intel.com"
__description__ = "This script loads WIINGS Issue data from HANA to GSCDW DB"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeSQL, executeStoredProcedure, map_columns
import pandas as pd
from Logging import log
from Project_params import params
from datetime import date
from dateutil.relativedelta import relativedelta



# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'WIINGS Issue Data'
    data_area = 'WIINGSIssueDetailQuery'
    stage_table = 'Stage.WIINGSIssueDetailQuery'
    base_table = 'Base.WIINGSIssueDetailQuery'
    load_by = 'HanaEtl'
    params['EMAIL_ERROR_RECEIVER'].append('sankhax.subhra.ghosh@intel.com')

    current_date = date.today()
    current_date = current_date.strftime('%Y%m%d')

    current_date_minus2 = date.today()
    current_date_minus2 = current_date_minus2.today() - relativedelta(years=2)
    current_date_minus2 = current_date_minus2.strftime('%Y%m%d')

    query = """SELECT 
         "SupplierId",
         "MachineTypeNm",
         "IntelPartNbr",
         "CommodityCd",
         "ItemDsc",
         "SupplierNm",
         "PurchaseGroupCd",
         "UniqueEquipmentId",
         "StockroomDeliveryLocationDsc",
         "FactoryMaterialRequestLineNbr",
         "FactoryMaterialRequestLineFulfillmentId",
         "GenearalLedgerAccountNbr",
         "RequestLineFulfillmentDoneByWorldwideId",
         "RequestCreatedByWorldwideId",
         "FactoryMaterialReturnRequestTypeNm",
         "RequestLineFulfillmentDtm",
         "StorageLocationCd",
         "FactoryMaterialRequestId",
         "FactoryMaterialsServerId",
         "MachineModelNbr",
         Sum("CostToIntel")                              AS "CostToIntel",
         Sum("UnitCostAmt")                              AS "UnitCostAmt",
         Sum("FactoryCustomerOrderIssueTransactionAmt")  AS "FactoryCustomerOrderIssueTransactionAmt",
         Sum("FactoryMaterialRequestLineFulfillmentQty") AS "FactoryMaterialRequestLineFulfillmentQty"
FROM     "_SYS_BIC"."intel.factorymaterials.consumption.extracts/WIINGSIssueDetailQuery"
('PLACEHOLDER' = ('$$IP_TransactionFromTimestamp$$', '{current_date_minus2}'), 
'PLACEHOLDER' = ('$$IP_TransactionToTimestamp$$', '{current_date}'), 
'PLACEHOLDER' = ('$$IP_ActiveRecordsOnly$$', 'Y'), 
'PLACEHOLDER' = ('$$IP_FactoryMaterialServerid$$', '''*'''))
GROUP BY "SupplierId",
         "MachineTypeNm",
         "IntelPartNbr",
         "CommodityCd",
         "ItemDsc",
         "SupplierNm",
         "PurchaseGroupCd",
         "UniqueEquipmentId",
         "StockroomDeliveryLocationDsc",
         "FactoryMaterialRequestLineNbr",
         "FactoryMaterialRequestLineFulfillmentId",
         "GenearalLedgerAccountNbr",
         "RequestLineFulfillmentDoneByWorldwideId",
         "RequestCreatedByWorldwideId",
         "FactoryMaterialReturnRequestTypeNm",
         "RequestLineFulfillmentDtm",
         "StorageLocationCd",
         "FactoryMaterialRequestId",
         "FactoryMaterialsServerId",
         "MachineModelNbr" """.format(current_date=current_date, current_date_minus2=current_date_minus2)

    #print(query)
    df = queryHANA(query, environment='Production')

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = load_by

    map_columns(stage_table,df)

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
