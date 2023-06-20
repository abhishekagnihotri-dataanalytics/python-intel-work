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
    data_area = 'MSBIDetailQuery'
    stage_table = 'Stage.MSBIDetailQuery'
    base_table = 'Base.MSBIDetailQuery'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('shweta.v.aurangabadkar@intel.com')
    current_date = date.today()
    current_date_minus2 = date.today()
    current_date_minus2 = current_date_minus2.replace(year=current_date_minus2.year - 1)


    query = """
    SELECT
	 "CriticalItemInd",
	 "OldestBackOrderDt",
	 "HeldPOInd",
	 "AlternatePartIndicator",
	 "AlternateOrderUnitInd",
	 "BaseUnitOfMeasureCd",
	 "PurchaseOrderUnitOfMeasureCd",
	 "ContractualLeadTime",
	 "SupplierPartNbr",
	 "RepairPriceCur",
	 "PackSize",
	 "BuyerNote",
	 "RepairContractLeadTime",
	 "HarvestStockroomIndicator",
	 "GLAccountLongName",
	 "GLAccountName",
	 "PurchaseGroupCd",
	 "StockroomNm",
	 "PlantCd",
	 "ShareableInd",
	 "UnitOfMeasureCd",
	 "RepairTypeNm",
	 "ReplenishFromExcessInd",
	 "ZeroBinIndicator",
	 "ItemGLAccount",
	 "ConsumableInd",
	 "HighRiskItemInd",
	 "ReplenishmentPolicyNm",
	 "SupplierId",
	 "CostCenter",
	 "GLAccount",
	 "EffectiveDt",
	 "PurchaseGroupNm",
	 "StockroomId",
	 "FactoryMaterialServerId",
	 "ItemStockroomStatusInd",
	 "ItemDsc",
	 "ItemId",
	 "LastActivationDate",
	 "LastDeactivationDate",
	 "LotControlNm",
	 "CategoryDsc",
	 "MachineTypeNm",
	 "StorageLocationTypeCd",
	 "ActualLeadTimeNbr",
	 "ReplenishmentMethodName",
	 "ReplenishmentFrequencyNbr",
	 "ReplenishmentStatusInd",
	 "SupplierNm",
	 "LastUpdateTs",
	 "IntelOwnedInd",
	 "ConsignmentInd",
	 "AutoAuthorizationInd",
	 "LatestTransactionDt",
	 "LatestIssueDt",
	 "LatestReceiptDt",
	 "LotControlCd",
	 "UnitCostCur",
	 sum("Prdict90DayCnsmptnQty") AS "Prdict90DayCnsmptnQty",
	 sum("CountOfBackOrders") AS "CountOfBackOrders",
	 sum("Last1095DayCnsmptnQty") AS "Last1095DayCnsmptnQty",
	 sum("RepairPrice") AS "RepairPrice",
	 sum("NewBuyPrice") AS "NewBuyPrice",
	 sum("MinQty") AS "MinQty",
	 sum("ConsignmentQty") AS "ConsignmentQty",
	 sum("Last5DaysConsumptionQty") AS "Last5DaysConsumptionQty",
	 sum("Last547DaysConsumptionQty") AS "Last547DaysConsumptionQty",
	 sum("VFAvailQty") AS "VFAvailQty",
	 sum("AvailQty") AS "AvailQty",
	 sum("ReorderPointQty") AS "ReorderPointQty",
	 sum("MaximumQty") AS "MaximumQty",
	 sum("RejectQty") AS "RejectQty",
	 sum("UnitPrice") AS "UnitPrice",
	 sum("Last30DaysConsumptionQty") AS "Last30DaysConsumptionQty",
	 sum("Last90DaysConsumptionQty") AS "Last90DaysConsumptionQty",
	 sum("Last180DaysConsumptionQty") AS "Last180DaysConsumptionQty",
	 sum("Last730DaysConsumptionQty") AS "Last730DaysConsumptionQty",
	 sum("Last365DaysConsumptionQty") AS "Last365DaysConsumptionQty",
	 sum("OnHandQty") AS "OnHandQty",
	 sum("OnHoldQty") AS "OnHoldQty",
	 sum("OpenCustomerOrdQty") AS "OpenCustomerOrdQty",
	 sum("FixedOrderQty") AS "FixedOrderQty",
	 sum("EconimicOrderQty") AS "EconimicOrderQty",
	 sum("QtyMultiple") AS "QtyMultiple",
	 sum("VFDOI") AS "VFDOI",
	 sum("StockRoomDOI") AS "StockRoomDOI" 
FROM "_SYS_BIC"."intel.factorymaterials.consumption.extracts/MSBIDetailQuery"('PLACEHOLDER' = ('$$IP_ProcNm$$',
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
	 'PLACEHOLDER' = ('$$IN_LASTNDAYS$$',
	 '1095'),
	 'PLACEHOLDER' = ('$$IP_MachNm$$',
	 '''ALL'''),
	 'PLACEHOLDER' = ('$$IP_FactoryMaterialServerid$$',
	 '''*''')) 
GROUP BY "CriticalItemInd",
	 "OldestBackOrderDt",
	 "HeldPOInd",
	 "AlternatePartIndicator",
	 "AlternateOrderUnitInd",
	 "BaseUnitOfMeasureCd",
	 "PurchaseOrderUnitOfMeasureCd",
	 "ContractualLeadTime",
	 "SupplierPartNbr",
	 "RepairPriceCur",
	 "PackSize",
	 "BuyerNote",
	 "RepairContractLeadTime",
	 "HarvestStockroomIndicator",
	 "GLAccountLongName",
	 "GLAccountName",
	 "PurchaseGroupCd",
	 "StockroomNm",
	 "PlantCd",
	 "ShareableInd",
	 "UnitOfMeasureCd",
	 "RepairTypeNm",
	 "ReplenishFromExcessInd",
	 "ZeroBinIndicator",
	 "ItemGLAccount",
	 "ConsumableInd",
	 "HighRiskItemInd",
	 "ReplenishmentPolicyNm",
	 "SupplierId",
	 "CostCenter",
	 "GLAccount",
	 "EffectiveDt",
	 "PurchaseGroupNm",
	 "StockroomId",
	 "FactoryMaterialServerId",
	 "ItemStockroomStatusInd",
	 "ItemDsc",
	 "ItemId",
	 "LastActivationDate",
	 "LastDeactivationDate",
	 "LotControlNm",
	 "CategoryDsc",
	 "MachineTypeNm",
	 "StorageLocationTypeCd",
	 "ActualLeadTimeNbr",
	 "ReplenishmentMethodName",
	 "ReplenishmentFrequencyNbr",
	 "ReplenishmentStatusInd",
	 "SupplierNm",
	 "LastUpdateTs",
	 "IntelOwnedInd",
	 "ConsignmentInd",
	 "AutoAuthorizationInd",
	 "LatestTransactionDt",
	 "LatestIssueDt",
	 "LatestReceiptDt",
	 "LotControlCd",
	 "UnitCostCur"
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
