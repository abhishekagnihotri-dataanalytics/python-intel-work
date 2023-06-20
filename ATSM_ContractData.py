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
    data_area = 'ContractData'
    stage_table = 'Stage.ContractData'
    base_table = 'Base.ContractData'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('shweta.v.aurangabadkar@intel.com')
    current_date = date.today()
    current_date_minus2 = date.today()
    current_date_minus2 = current_date_minus2.replace(year=current_date_minus2.year - 2)


    query = """
    SELECT
	 "PurchaseOutlineAgreementNbr",
	 "PurchaseOutlineAgreementLineNbr",
	 "SupplierId",
	 "ContractStartDt",
	 "ContractExpireDt",
	 "ChangeDtm",
	 "IntelPartNbr",
	 "ManufacturerPartMaterialNbr",
	 "ItemCategoryNm",
	 "CurrencyCd",
	 "CreateAgentId",
	 "SupplierPartNbr",
	 "LeadTimeDayCnt",
	 sum("PurchasePriceUnitQty") AS "PurchasePriceUnitQty",
	 sum("PurchaseDocumentLineNetUnitPriceTransactionAmt") AS "PurchaseDocumentLineNetUnitPriceTransactionAmt"

FROM "_SYS_BIC"."d.SelfService.SourceToPay/PurchaseOutlineAgreementDetail"('PLACEHOLDER' = ('$$IP_StorageLocationCd$$',
	 '''*'''),
	 'PLACEHOLDER' = ('$$IP_PlantCode$$',
     '''CNB1'',''VNA2'',''MYA8'',''AZ08'',''MYB2'',''AZ04'',''CRA2'',''MYA3'', ''OR10'',''KMA'', ''CD'',''CRA7'',''AL'''),
	 'PLACEHOLDER' = ('$$IP_ActiveRecordsOnly$$',
	 'Y'),
	 'PLACEHOLDER' = ('$$IP_PurchaseOrganizationCd$$',
	 '''*''')) 
	 where "ContractStartDt">='{current_date_minus2}'
     GROUP BY "PurchaseOutlineAgreementNbr",
	 "PurchaseOutlineAgreementLineNbr",
	 "SupplierId",
	 "ContractStartDt",
	 "ContractExpireDt",
	 "ChangeDtm",
	 "IntelPartNbr",
	 "ManufacturerPartMaterialNbr",
	 "ItemCategoryNm",
	 "CurrencyCd",
	 "CreateAgentId",
	 "SupplierPartNbr",
	 "LeadTimeDayCnt"
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
