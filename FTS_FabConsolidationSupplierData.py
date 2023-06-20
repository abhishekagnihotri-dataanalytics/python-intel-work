__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads OA data from EDW to GSCDW DB"
__schedule__ = "Once Daily at 4AM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeSQL, executeStoredProcedure
import pandas as pd
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

project_name = 'Fab Consolidation'
DataArea = 'Supplier'
LoadBy = 'HANA'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

#SupplierIntegratedHierarchy
query = """select 
	SIH."SupplierId",
	SIH."BusinessOrganizationNm" as "BusinessOrganizationLegalNm",
	SIH."BusinessOrganizationDunsNbr",
	SIH."BusinessOrganizationDunsNm",
	SIH."ParentSupplierID",
	SIH."ParentBusinessOrganizationDunsNbr",
	SIH."ParentBusinessOrganizationDunsNm",
	SIH."GlobalUltimateSupplierID",
	SIH."GlobalBusinessOrganizationDunsNbr",
	SIH."GlobalBusinessOrganizationDunsNm" ,
	coalesce(SBO."BusinessOrganizationNm",SIH."BusinessOrganizationNm") as "BusinessOrganizationeNm"
from "_SYS_BIC"."intel.sourceidp.consumption.procurement/SupplierIntegratedHierarchyQuery" SIH 
LEFT OUTER JOIN  (SELECT "SupplierId"
    ,"BusinessOrganizationNm" 
FROM "_SYS_BIC"."c.Supplier/SupplierBusinessOrganizationName"
WHERE "BusinessNameTypeNm" = 'TRADE'    AND "PreferredNameInd" = 'Y'    AND "EffectiveEndDtm" > CURRENT_DATE and "EffectiveStartDtm" <= CURRENT_DATE) SBO ON 
SIH."SupplierId" = SBO."SupplierId"

        """
df = queryHANA(query, environment='Production')

Table ='Stage.Supplier'
df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, LoadBy,
                                                                          dest_db))

    table_name = '[GSCDW].[Base].[Supplier]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[Supplier]")
        Insert_query = """insert into [GSCDW].[Base].[Supplier]
        SELECT *  FROM [GSCDW].[Stage].[Supplier]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base")
        else:
            log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[GSCDW].[Stage].[Supplier]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[Supplier]")
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
            error_msg=error_msg)
else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0], error_msg=error_msg)

#SupplierNameAddressLifeCycle
query2 = """select _SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."SupplierId",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."BusinessPartyId",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CurrentLifecycleStatusTypeNm",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CurrentLifecycleStatusEffectiveStartDtm",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."PhysicalStreetAddressTxt",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CityNm",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CountrySubdivisionCd",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CountrySubdivisionNm",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CountryCd",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."CountryNm",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."PostalCd",
	_SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"."GeographyCd" 
from _SYS_BIC."c.Supplier/SupplierNameAddressLifecycle"
        """
df = queryHANA(query2, environment='Production')

Table ='Stage.SupplierNameAddressLifecycle'
df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, LoadBy,
                                                                          dest_db))

    table_name = '[GSCDW].[Base].[SupplierNameAddressLifecycle]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[SupplierNameAddressLifecycle]")
        Insert_query = """insert into [GSCDW].[Base].[SupplierNameAddressLifecycle]
        SELECT *  FROM [GSCDW].[Stage].[SupplierNameAddressLifecycle]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base table")
        else:
            log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[GSCDW].[Stage].[SupplierNameAddressLifecycle]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[SupplierNameAddressLifecycle]")
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
            error_msg=error_msg)
else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0], error_msg=error_msg)

#Supplier Segmentation
query3 = """SELECT
	seg."SupplierId" AS "SupplierId",
	 seg."SourcingOrganizationCd" AS "SourcingOrganizationCd",
	 org."HighLevelSupplyChainOrganizationCd" AS "HighLevelSupplyChainOrganizationCd",
	 org."HighLevelSupplyChainOrganizationNm" AS "HighLevelSupplyChainOrganizationNm",
     source_org."SourcingOrganizationNm" AS "SourcingOrganizationNm",
	 seg."SupplierSegmentClassificationNm" AS "SupplierSegmentClassificationNm",
	 seg."SupplierSegmentEffectiveStartDtm" AS "SupplierSegmentEffectiveStartDtm",
	 seg."SupplierSegmentEffectiveEndDtm" AS "SupplierSegmentEffectiveEndDtm",
	 seg."SupplierSegmentationPriorityClassificationNm" AS "SupplierSegmentationPriorityClassificationNm",
	 seg."PriorityClassificationEffectiveStartDtm" AS "PriorityClassificationEffectiveStartDtm",
	 seg."PriorityClassificationEffectiveEndDtm" AS "PriorityClassificationEffectiveEndDtm"
FROM "_SYS_BIC"."c.Supplier/SupplierSegmentation" seg
LEFT OUTER JOIN "_SYS_BIC"."b.Commodity/SourcingOrganization" source_org ON source_org."SourcingOrganizationCd" = seg."SourcingOrganizationCd"
LEFT OUTER JOIN "_SYS_BIC"."b.Commodity/HighLevelSupplyChainOrganization" org ON source_org."HighLevelSupplyChainOrganizationCd" = org."HighLevelSupplyChainOrganizationCd"
WHERE seg."PriorityClassificationEffectiveEndDtm" > CURRENT_DATE AND seg."SupplierSegmentEffectiveEndDtm" > CURRENT_DATE
and seg."PriorityClassificationEffectiveStartDtm" <= CURRENT_DATE AND seg."SupplierSegmentEffectiveStartDtm" <= CURRENT_DATE
        """
df = queryHANA(query3, environment='Production')

Table ='Stage.SupplierSegmentation'
df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy
# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(Table, df, truncate=True)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], Table, LoadBy,
                                                                          dest_db))

    table_name = '[GSCDW].[Base].[SupplierSegmentation]'
    sp_name = 'ETL.spTruncateTable'
    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
    if truncate_succeeded:
        print("Successfully truncated table [Base].[SupplierSegmentation]")
        Insert_query = """insert into [GSCDW].[Base].[SupplierSegmentation]
        SELECT *  FROM [GSCDW].[Stage].[SupplierSegmentation]"""
        insert_succeeded, error_msg = executeSQL(Insert_query)
        if insert_succeeded:
            print("Successfully copied data from staging to base table")
        else:
            log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)

        table_name = '[GSCDW].[Stage].[SupplierSegmentation]'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Stage].[SupplierSegmentation]")
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
                error_msg=error_msg)
    else:
        log(truncate_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0],
            error_msg=error_msg)
else:
    print(error_msg)
    log(insert_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0], error_msg=error_msg)


sp_name = 'ETL.spLoadSupplierwithSegmentation'
sp_succeeded, error_msg = executeStoredProcedure(sp_name)
if sp_succeeded:
    print("Successfully executed stored procedure that loads SupplierwithSegmention table")

else:
    log(sp_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0], error_msg=error_msg)

sp_name = 'ETL.spLoadSupplierwithAddress'
sp_succeeded, error_msg = executeStoredProcedure(sp_name)
if sp_succeeded:
    print("Successfully executed stored procedure that loads SupplierHierarchy Integration table")

else:
    log(sp_succeeded, project_name=project_name, data_area=DataArea, row_count=df.shape[0], error_msg=error_msg)