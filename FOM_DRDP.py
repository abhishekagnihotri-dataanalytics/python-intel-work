__author__ = "Khushboo Saboo"
__email__ = "khushboo.saboo@intel.com"
__description__ = "populates data into [gsmdw].[dbo].[FOM_DRDP_asofDelivDt_Scheduled] and [gsmdw].[dbo].[FOM_DRDP_asofDelivDt_VendorConfirmed]"
__schedule__ = "11:30 PM daily"
import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from hdbcli import dbapi
import pandas as pd
import io
import os
import email
from email.message import EmailMessage
from Project_params import params
from Helper_Functions import uploadDFtoSQL
from Password import accounts
from Logging import log, log_warning

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


#This python file is meant to run once each day at the very end of the day to populate data into [gsmdw].[dbo].[FOM_DRDP_asofDelivDt_Scheduled] and [gsmdw].[dbo].[FOM_DRDP_asofDelivDt_VendorConfirmed]
#Those tables populate the 'AsOfDeliveryDate Qty' values in the GSM_FOM_DRDP model

params['EMAIL_ERROR_RECEIVER'].append('khushboo.saboo@intel.com')

#Initialize your connection
conn = dbapi.connect(
    address='sapehpdb.intel.com',
    port='31015',
    user=accounts['HANA'].username,
    password=accounts['HANA'].password
)

cursor = conn.cursor()
sched_query = """  SSELECT DISTINCT
	 "PurchaseOrderNbr" AS "PurchaseDocumentNbr",
	 "RequestedDeliveryDt" AS "DeliveryDt",
	 "ScheduledQty" AS "asofDeliveryDtScheduledQty"
	 FROM "_SYS_BIC"."intel.sourceidp.consumption.procurement/PurchaseOrderScheduleLineQuery"('PLACEHOLDER' = ('$$IP_PurchaseDocumentDeleteInd$$',
	 ''),
	 'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateEndDt$$',
	 '01/01/2023'),
	 'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateStartDt$$',
	 '01/01/2015'))
	 WHERE ("PurchaseOrderNbr" like '45%' OR "PurchaseOrderNbr" like '55%')
	 AND "PurchaseOrderTypeCd" in ('ZNB', 'ZLPI', 'ZARM') 
	 AND "CompanyCd" in ('356', '100', '342')
	 AND "CommodityCd" in ('0330', '0113', '0114', '0115', '0116', '0107', '0108', '95990350') 
	 AND "PurchaseOrganizationCd" in ('0050', '0063')
	 AND ("PurchaseOrderValidityEndDate" = '00000000' OR TO_DATE("PurchaseOrderValidityEndDate", 'MM/DD/YYYY') > add_months(CURRENT_DATE, -6))
	 AND TO_DATE("RequestedDeliveryDt", 'MM/DD/YYYY') = CURRENT_DATE
	 """
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
vendor_query = """  SELECT DISTINCT "PurchaseOrderNbr" AS "PurchaseDocumentNbr",
	 "SupplierConfirmationDeliveryDt" AS "DeliveryDt",
	 sum("SupplierConfirmedQty") AS "asofDeliveryDtVendorConfirmedQty"
	 FROM "_SYS_BIC"."intel.sourceidp.consumption.procurement/PurchaseOrderSupplierConfirmationQuery"('PLACEHOLDER' = ('$$IP_PurchaseDocumentDeleteInd$$',
	 ''),
	 'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateEndDt$$',
	 '01/01/2023'),
	 'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateStartDt$$',
	 '01/01/2015')) 
	 WHERE ("PurchaseOrderNbr" like '45%' OR "PurchaseOrderNbr" like '55%')
	 AND "PurchaseOrderTypeCd" in ('ZNB', 'ZLPI', 'ZARM') 
	 AND "CompanyCd" in ('356', '100', '342')
	 AND "CommodityCd" in ('0330', '0113', '0114', '0115', '0116', '0107', '0108', '95990350') 
	 AND "PurchaseOrganizationCd" in ('0050', '0063')
	 AND ("PurchaseOrderValidityEndDt" = '00000000' OR TO_DATE("PurchaseOrderValidityEndDt", 'MM/DD/YYYY') > add_months(CURRENT_DATE, -6))
	 AND TO_DATE("SupplierConfirmationDeliveryDt", 'MM/DD/YYYY') = CURRENT_DATE
	 GROUP BY "PurchaseOrderNbr", "SupplierConfirmationDeliveryDt" """
cursor.execute(vendor_query)
vendor_result = cursor.fetchall()
if len(vendor_result) > 0:
	vendor_columns = [x[0] for x in cursor.description]
	vendor_df = pd.DataFrame(vendor_result, columns=vendor_columns)
	success, error_msg = uploadDFtoSQL(table='[dbo].[FOM_DRDP_asofDelivDt_VendorConfirmed]', data=vendor_df, columns=vendor_columns, categorical=None, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
	log(success, project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area='asofDelivDtVendorConfirmedQty', row_count=len(vendor_df), error_msg=error_msg)  # row_count is automatically set to 0 if error
else:  # Query returned no rows
	log_warning(project_name="FOM_DRDP", package_name="FOMDRDP_asofDelivDtQty.py", data_area="asofDelivDtVendorConfirmedQty", warning_type='Not Modified')
cursor.close()
conn.close()
