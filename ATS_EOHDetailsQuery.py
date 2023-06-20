__author__ = "Shweta"
__email__ = "shweta.v.aurangabadkar@intel.com"
__description__ = "This script loads Contract  data from HANA to GSCDW DB"

import os
import sys;

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeSQL, executeStoredProcedure, map_columns
import pandas as pd
from datetime import date
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    # initialize variables
    project_name = 'AT Biz ops'
    data_area = 'EOHDetailsQuery'
    stage_table = 'Stage.EOHDetailsQuery'
    base_table = 'Base.EOHDetailsQuery'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('shweta.v.aurangabadkar@intel.com')
    current_date = date.today()
    current_date_minus2 = date.today()
    current_date_minus2 = current_date_minus2.replace(year=current_date_minus2.year - 2)


    query = """
    SELECT
	 "LotControlCd",
	 "UnitCostCur",
	 "ZeroBinIndicator",
	 "ReplenishFromExcessInd",
	 "ItemGLAccount",
	 "ConsumableInd",
	 "BuyerNote",
	 "FactoryEquipmentSparePartCategoryId",
	 "HighRiskItemInd",
	 "SupplierId",
	 "ItemId",
	 "StockroomId",
	 "FactoryMaterialServerId",
	 "PurchaseGroupCd",
	 "CostCenter",
	 "GLAccount",
	 "RepairTypeNm",
	 "EffectiveDt",
	 "ShareableInd",
	 "UnitOfMeasureCd",
	 "MinQty",
	 "PlantCd",
	 "ReplenishmentPolicyNm",
	 "ReplenishmentMethodName",
	 "ReplenishmentFrequencyNbr",
	 "ReplenishmentStatusInd",
	 "SupplierNm",
	 "LastUpdateTs",
	 "ItemStockroomStatusInd",
	 "IntelOwnedInd",
	 "ConsignmentInd",
	 "ConsignmentQty",
	 sum("StockRoomDOI") AS "StockRoomDOI",
	 sum("Last5DaysConsumptionQty") AS "Last5DaysConsumptionQty",
	 sum("Last547DaysConsumptionQty") AS "Last547DaysConsumptionQty",
	 max("ContractualLeadTime") AS "ContractualLeadTime",
	 sum("VFAvailQty") AS "VFAvailQty",
	 max("RepairContractLeadTime") AS "RepairContractLeadTime",
	 sum("AvailQty") AS "AvailQty",
	 sum("ReorderPointQty") AS "ReorderPointQty",
	 sum("MaximumQty") AS "MaximumQty",
	 sum("RejectQty") AS "RejectQty",
	 sum("UnitPrice") AS "UnitPrice",
	 sum("Last30DaysConsumptionQty") AS "Last30DaysConsumptionQty",
	 sum("Last90DaysConsumptionQty") AS "Last90DaysConsumptionQty",
	 sum("Last180DaysConsumptionQty") AS "Last180DaysConsumptionQty",
	 sum("Last365DaysConsumptionQty") AS "Last365DaysConsumptionQty",
	 sum("Last730DaysConsumptionQty") AS "Last730DaysConsumptionQty",
	 sum("OnHandQty") AS "OnHandQty",
	 sum("OnHoldQty") AS "OnHoldQty",
	 sum("FixedOrderQty") AS "FixedOrderQty",
	 sum("QtyMultiple") AS "QtyMultiple",
	 sum("EconimicOrderQty") AS "EconimicOrderQty",
	 sum("OpenCustomerOrdQty") AS "OpenCustomerOrdQty" 
FROM "_SYS_BIC"."intel.factorymaterials.consumption.extracts/EOHDetailsQuery"('PLACEHOLDER' = ('$$IP_ProcNm$$',
	 '''ALL'''),
	 'PLACEHOLDER' = ('$$IP_ItemId$$',
	 '''ALL'''),
	 'PLACEHOLDER' = ('$$LastUpdateToTimestamp$$',
	 '{current_date}'),
	 'PLACEHOLDER' = ('$$IP_ProcId$$',
	 '0'),
	 'PLACEHOLDER' = ('$$LastUpdateFromTimestamp$$',
	 '{current_date_minus2}'),
	 'PLACEHOLDER' = ('$$ItemStockroomStatusIndicator$$',
	 'Y'),
	 'PLACEHOLDER' = ('$$IP_MachNm$$',
	 '''ALL'''),
	 'PLACEHOLDER' = ('$$IP_FactoryMaterialServerid$$',
	 '''*''')) 
GROUP BY "LotControlCd",
	 "UnitCostCur",
	 "ZeroBinIndicator",
	 "ReplenishFromExcessInd",
	 "ItemGLAccount",
	 "ConsumableInd",
	 "BuyerNote",
	 "FactoryEquipmentSparePartCategoryId",
	 "HighRiskItemInd",
	 "SupplierId",
	 "ItemId",
	 "StockroomId",
	 "FactoryMaterialServerId",
	 "PurchaseGroupCd",
	 "CostCenter",
	 "GLAccount",
	 "RepairTypeNm",
	 "EffectiveDt",
	 "ShareableInd",
	 "UnitOfMeasureCd",
	 "MinQty",
	 "PlantCd",
	 "ReplenishmentPolicyNm",
	 "ReplenishmentMethodName",
	 "ReplenishmentFrequencyNbr",
	 "ReplenishmentStatusInd",
	 "SupplierNm",
	 "LastUpdateTs",
	 "ItemStockroomStatusInd",
	 "IntelOwnedInd",
	 "ConsignmentInd",
	 "ConsignmentQty"
            """.format(current_date=current_date,current_date_minus2=current_date_minus2)
    df = queryHANA(query, environment='Production')

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = load_by

    # map_columns(stage_table,df)

    # upload dataframe to SQL
    insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, load_by,
                                                                              params['GSMDW_DB']))

        # Clear base table before attempting to copy data from staging there
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table)
        if truncate_succeeded:
            print("Successfully truncated table {}".format(base_table))

            # Copy data from Stage table to Base table
            insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table,
                                                                                        copy_from=stage_table)
            insert_succeeded, error_msg = executeSQL(insert_query)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)  # log regardless of success or failure
            if insert_succeeded:
                print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table,
                                                                                      copy_from=stage_table))

                # Clear stage table after successful insert into Base table
                truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table)
                if truncate_succeeded:
                    print("Successfully truncated table {}".format(stage_table))
                else:
                    log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                        error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
            error_msg=error_msg)
