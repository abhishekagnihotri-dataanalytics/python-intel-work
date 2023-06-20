__author__ = "Khushboo Saboo"
__email__ = "khushboo.saboo@intel.com"
__description__ = "populates PurchaseDocumentScheduleLineView from Hana into sql"
__schedule__ = " N/A"
import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from hdbcli import dbapi
import pandas as pd
from pandas import ExcelFile, DataFrame
import io
import email
from email.message import EmailMessage
from Project_params import params
from Helper_Functions import uploadDFtoSQL
from Password import accounts
from Logging import log, log_warning

params['EMAIL_ERROR_RECEIVER'].append('khushboo.saboo@intel.com')

#Initialize your connection
conn = dbapi.connect(
    address='sapehpdb.intel.com',
    port='31015',
    user=accounts['HANA'].username,
    password=accounts['HANA'].password
)

cursor = conn.cursor()
sched_query = """  SELECT DISTINCT B."PurchaseDocumentNbr", A."DeliveryDt", A."ScheduledQty" AS "asofDeliveryDtScheduledQty"
	FROM "_SYS_BIC"."intel.sourceidp.procurement.public/PurchaseDocumentScheduleLineView"('PLACEHOLDER' = ('$$IP_PurchaseDocumentDeleteInd$$','*'),
		'PLACEHOLDER' = ('$$IP_PurchaseDocumentTypeCd$$','''*'''),
		'PLACEHOLDER' = ('$$IP_PurchaseDocumentCategoryCd$$','''*'''),
		'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateEndDt$$','CURRENT_DATE'),
		'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateStartDt$$','20150101'))  AS A
	INNER JOIN "_SYS_BIC"."intel.sourceidp.procurement.public/PurchaseDocumentHeaderLineView" AS B
	ON A."PurchaseDocumentNbr" = B."PurchaseDocumentNbr"
	WHERE (B."PurchaseDocumentNbr" LIKE '45%' OR B."PurchaseDocumentNbr" LIKE '55%')
	AND B."PurchaseDocumentTypeCd" IN ('ZNB', 'ZLPI', 'ZARM')
	AND B."HeaderCompanyCd" IN ('356','100','342')
	AND B."PurchaseOrganizationCd" in ('0050', '0063')
	AND B."MaterialGroupCd" IN ('0330', '0107', '0108', '0113', '0114', '0115', '0116', '95990350')
	AND (B."PurchaseDocumentValidityEndDt" = '00000000' OR B."PurchaseDocumentValidityEndDt" > add_months(CURRENT_DATE , -6))
	AND A."DeliveryDt" = CURRENT_DATE  """
cursor.execute(sched_query)
sched_result = cursor.fetchall()
if len(sched_result) > 0:
	sched_columns = [x[0] for x in cursor.description]
	sched_df = pd.DataFrame(sched_result, columns=sched_columns)
	success, error_msg = uploadDFtoSQL(table='[dbo].[FOM_DRDP_asofDelivDt_Scheduled]', data=sched_df, columns=sched_columns, categorical=None, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
	log(success, project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area='asofDelivDtScheduledQty', row_count=len(sched_df), error_msg=error_msg)  # row_count is automatically set to 0 if error
else:  # Query returned no rows
	log_warning(project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area='asofDelivDtScheduledQty', warning_type='Not Modified')
cursor.close()

cursor = conn.cursor()
vendor_query = """  SELECT DISTINCT B."PurchaseDocumentNbr", A."VendorConfirmationDeliveryDt" AS "DeliveryDt", A."VendorConfirmedQty" AS "asofDeliveryDtVendorConfirmedQty"
FROM "_SYS_BIC"."intel.sourceidp.procurement.public/PurchaseDocumentVendorConfirmationView" AS A
INNER JOIN "_SYS_BIC"."intel.sourceidp.procurement.public/PurchaseDocumentHeaderLineView" AS B
ON A."PurchaseDocumentNbr" = B."PurchaseDocumentNbr"
WHERE (B."PurchaseDocumentNbr" LIKE '45%' OR B."PurchaseDocumentNbr" LIKE '55%')
AND B."PurchaseDocumentTypeCd" IN ('ZNB', 'ZLPI', 'ZARM')
AND B."HeaderCompanyCd" IN ('356','100','342')
AND B."PurchaseOrganizationCd" in ('0050', '0063')
AND B."MaterialGroupCd" IN ('0330', '0107', '0108', '0113', '0114', '0115', '0116', '95990350')
AND (B."PurchaseDocumentValidityEndDt" = '00000000' OR B."PurchaseDocumentValidityEndDt" > add_months( CURRENT_DATE , -6))
AND A."VendorConfirmationDeliveryDt" = CURRENT_DATE """
cursor.execute(vendor_query)
vendor_result = cursor.fetchall()
if len(vendor_result) > 0:
	vendor_columns = [x[0] for x in cursor.description]
	vendor_df = pd.DataFrame(vendor_result, columns=vendor_columns)
	success, error_msg = uploadDFtoSQL(table='[dbo].[FOM_DRDP_asofDelivDt_VendorConfirmed]', data=vendor_df, columns=vendor_columns, categorical=None, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
	log(success, project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area='asofDelivDtVendorConfirmedQty', row_count=len(vendor_df), error_msg=error_msg)  # row_count is automatically set to 0 if error
else:  # Query returned no rows
	log_warning(project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area='asofDelivDtVendorConfirmedQty', warning_type='Not Modified')
cursor.close()
conn.close()
