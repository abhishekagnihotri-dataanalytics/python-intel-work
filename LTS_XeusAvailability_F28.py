# __author__ = "Pratha Balakrishnan"
# __email__ = "prathakini.balakrishnan@intel.com"
# __description__ = "This script loads ToolAvailability data from XEUS to SCDA database"
# __schedule__ = "Weekly Sunday 3.30PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import PyUber
import pandas as pd
from Logging import log
from Project_params import params
from Helper_Functions import uploadDFtoSQL


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


project_name = 'ESP'
data_area = 'ToolAvailability'
load_by = 'XEUS'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

####Connect to XEUS database####
connXeus_F28 = PyUber.connect(datasource="F28_PROD_XEUS")

####import availability data from XEUS and land it in staging table - start####
data_availability = connXeus_F28.execute("""SELECT
a.WW
,a.AREA
,a.CEID
,b.FACILITY
,a.ENTITY
,a.PARENT_ENTITY
,a.PARENT_CEID
,a.VFMFGID
,a.REV_MODULE
,a.OWNED_BY
,AVG(a.MFG_AVAILABILITY) AS MA
,AVG(a.SUPPLIER_MFG_AVAILABILITY) AS SDUT
,COUNT(a.ROLLUP_START_TIME) AS TOTAL_TIME
,'F28' as XEUS_Site
,current_date as RunDate
FROM LIMA_f_Entity_Availability_Hr  a
LEFT JOIN LIMA_f_entity b ON b.Entity = a.ENTITY AND b.entity_deleted_flag = 'N'
WHERE a.WW >= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date - (7*53) and c.END_DATE >= current_date - (7*53)) 
and a.WW < (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date and c.END_DATE >= current_date) 
and a.INCLUDE_IN_REPORT ='Y' AND a.EXCLUDE_FLAG ='N'
GROUP BY 
          a.WW
          ,a.AREA
          ,a.CEID
          ,b.FACILITY
          ,a.ENTITY
          ,a.PARENT_ENTITY
          ,a.PARENT_CEID
          ,a.VFMFGID
          ,a.REV_MODULE
          ,a.OWNED_BY """)

rows1 = data_availability.fetchall()
names1 = [x[0] for x in data_availability.description]
df_availability = pd.DataFrame(rows1, columns=names1)


####import availability data from XEUS and land it in staging table - start####
data_availability_process = connXeus_F28.execute("""
Select 
a.WW
,a.ENTITY
,a.CALCULATED_PROCESS_LIST

FROM LIMA_f_Entity_Availability_Hr a 
 join (SELECT
EA.WW
,EA.ENTITY
,max(EA.ROLLUP_START_TIME)  ROLLUP_START_TIME_MAX
FROM LIMA_f_Entity_Availability_Hr EA
WHERE EA.WW >= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date - (7*53) and c.END_DATE >= current_date - (7*53)) 
 and EA.WW < (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date and c.END_DATE >= current_date) 
  and EA.INCLUDE_IN_REPORT ='Y' AND EA.EXCLUDE_FLAG ='N'
GROUP BY 
          EA.WW
,EA.ENTITY)  tbl1 on a.WW = tbl1.WW  and a.Entity = tbl1.Entity and a.ROLLUP_START_TIME = tbl1.ROLLUP_START_TIME_MAX
 WHERE a.WW >= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date - (7*53) and c.END_DATE >= current_date - (7*53)) 
  and a.WW < (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date and c.END_DATE >= current_date) 
   and a.INCLUDE_IN_REPORT ='Y' AND a.EXCLUDE_FLAG ='N'
"""
                                                 )

rows1 = data_availability_process.fetchall()
names1 = [x[0] for x in data_availability_process.description]
df_availability_process = pd.DataFrame(rows1, columns=names1)

df_availability_with_process = pd.merge(df_availability, df_availability_process, how='inner', left_on=['WW', 'ENTITY'],
                                        right_on=['WW', 'ENTITY'])

tablenm1 = 'ToolAvailability.XEUS_Availability_stg'
insert_succeeded, error_msg = uploadDFtoSQL(tablenm1, df_availability_with_process, truncate=False, server='sql2377-fm1-in.amr.corp.intel.com,3180', database='SCDA')
log(insert_succeeded, project_name=project_name, data_area=tablenm1, row_count=df_availability_with_process.shape[0], error_msg=error_msg)
####import availability data from XEUS and land it in staging table - end####

####import Tool/Process data from XEUS and land it in staging table - start####
data_ToolProcess = connXeus_F28.execute("""SELECT 
distinct
b.FACILITY
,a.ENTITY
,a.CONFIGURED_PROCESSES
,'F28' as XEUS_DB
FROM LIMA_f_Entity_Availability_Hr  a
LEFT JOIN LIMA_f_entity b ON b.Entity = a.ENTITY AND b.entity_deleted_flag = 'N'
WHERE a.WW >= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date - (7*53) and c.END_DATE >= current_date - (7*53)) 
and a.WW <= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date and c.END_DATE >= current_date)  
and a.INCLUDE_IN_REPORT ='Y' AND a.EXCLUDE_FLAG ='N'
 """)

rows2 = data_ToolProcess.fetchall()
names2 = [x[0] for x in data_ToolProcess.description]
df_ToolProcess = pd.DataFrame(rows2, columns=names2)

tablenm2 = 'ToolAvailability.XEUS_ToolProcess_stg'
insert_succeeded, error_msg = uploadDFtoSQL(tablenm2, df_ToolProcess, truncate=False, server='sql2377-fm1-in.amr.corp.intel.com,3180', database='SCDA')
log(insert_succeeded, project_name=project_name, data_area=tablenm2, row_count=df_ToolProcess.shape[0], error_msg=error_msg)
####import Tool/Process data from XEUS and land it in staging table - end####


####import GSC config from XEUS and land it in staging table - start####
data_GSC_Config = connXeus_F28.execute("""SELECT
a.ENTITY, 
a.FACILITY,
a.PSPEC_TARGET,
a.PSPEC_ROLLUP_WEEKS,
a.SUPPLIER_CONTRACT_STATE,
a.UEID,
a.COMMENTS,
a.LAST_UPDATE_USER,
a.LAST_UPDATE_DATE,
a.SECURITY_CODE
,b.entity_state_list
,a.Warranty_Start_Date
,a.Warranty_End_Date
,current_date as RefreshDate 

FROM LIMA_f_UDF_ENTITY_GSC_CONFIG a
LEFT JOIN LIMA_f_UDF_ENTITY_GSC_STATE b ON a.SUPPLIER_CONTRACT_STATE = b.SUPPLIER_CONTRACT_STATE

WHERE 1=1
 """)

rows3 = data_GSC_Config.fetchall()
names3 = [x[0] for x in data_GSC_Config.description]
df_GSC_Config = pd.DataFrame(rows3, columns=names3)

tablenm3 = 'ToolAvailability.GSCconfig_stg'
insert_succeeded, error_msg = uploadDFtoSQL(tablenm3, df_GSC_Config, truncate=False, server='sql2377-fm1-in.amr.corp.intel.com,3180', database='SCDA')
log(insert_succeeded, project_name=project_name, data_area=tablenm3, row_count=df_GSC_Config.shape[0], error_msg=error_msg)
####import GSCconfig data from XEUS and land it in staging table - end####

####import Non Family Aware from XEUS and land it in staging table - start####
data_Availability_NFA_stg = connXeus_F28.execute("""
SELECT
eah.WW
,eah.AREA
,eah.CEID
,e.FACILITY
,eah.ENTITY
,eah.PARENT_ENTITY
,eah.PARENT_CEID
,eah.VFMFGID
,eah.REV_MODULE
,eah.OWNED_BY
/*,Group_Concat(Distinct eah2.ENTITY ORDER BY eah2.ENTITY ASC SEPARATOR ',') AS REPORT_ENTITIES*/
,1.0 - AVG(eah.DOWN_TIME) AS MA_COMPONENT
,1.0 - AVG(eah.SUPPLIER_DOWN_TIME) AS SDUT_COMPONENT
,COUNT(eah.ROLLUP_START_TIME) AS TOTAL_TIME_REPORT_ENTITIES
,'F28' as XEUS_Site
,current_date as RunDate
FROM LIMA_f_Entity_Availability_Hr eah
LEFT JOIN LIMA_f_entity e ON e.Entity = eah.ENTITY AND e.entity_deleted_flag = 'N'
WHERE eah.EXCLUDE_FLAG = 'N'
and eah.WW >= (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date - (7*53) and c.END_DATE >= current_date - (7*53)) 
 and eah.WW < (select distinct c.WW from LIMA_f_calendar c where c.START_DATE <= current_date and c.END_DATE >= current_date)  
GROUP BY 
    eah.WW
    ,eah.AREA
    ,eah.CEID
    ,e.FACILITY
    ,eah.ENTITY
    ,eah.PARENT_ENTITY
    ,eah.PARENT_CEID
    ,eah.VFMFGID
    ,eah.REV_MODULE
    ,eah.OWNED_BY

 """)

rows4 = data_Availability_NFA_stg.fetchall()
names4 = [x[0] for x in data_Availability_NFA_stg.description]
df_Availability_NFA_stg = pd.DataFrame(rows4, columns=names4)

tablenm4 = 'ToolAvailability.XEUS_Availability_NFA_stg'
insert_succeeded, error_msg = uploadDFtoSQL(tablenm4, df_Availability_NFA_stg, truncate=False, server='sql2377-fm1-in.amr.corp.intel.com,3180', database='SCDA')
log(insert_succeeded, project_name=project_name, data_area=tablenm4, row_count=df_Availability_NFA_stg.shape[0], error_msg=error_msg)
####import Availability_NFA_stg data from XEUS and land it in staging table - end####


####import CEID_Decoder from XEUS and land it in staging table - start####
data_CEID_Decoder = connXeus_F28.execute("""SELECT a.owned_by, a.entity, a.CEID, a.REV_CEID, a.rollup_end_time
, CASE WHEN a.CEID = a.REV_CEID THEN 'Y' WHEN a.REV_CEID IS NULL THEN ' ' ELSE 'N' END AS SAME_CEID_NAMES

FROM LIMA_F_ENTITY_AVAILABILITY_HR a
JOIN LIMA_f_facility c ON a.ww = c.current_ww

 """)

rows8 = data_CEID_Decoder.fetchall()
names8 = [x[0] for x in data_CEID_Decoder.description]
df_CEID_Decoder = pd.DataFrame(rows8, columns=names8)

data_CEID_Decoder_sq = connXeus_F28.execute("""select a.entity, a.ceid, a.owned_by, MAX(a.rollup_end_time) Max_Rollup_End_Time, e.facility, current_date as RunDate
 FROM LIMA_F_ENTITY_AVAILABILITY_HR a
LEFT JOIN LIMA_f_entity e ON e.Entity = a.ENTITY AND e.entity_deleted_flag = 'N'
JOIN LIMA_f_facility c ON a.ww = c.current_ww
GROUP BY a.entity, a.ceid, e.facility, a.owned_by
 """)

rows8_sq = data_CEID_Decoder_sq.fetchall()
names8_sq = [x[0] for x in data_CEID_Decoder_sq.description]
df_CEID_Decoder_sq = pd.DataFrame(rows8_sq, columns=names8_sq)

df_CEID_Decoder_joined = pd.merge(df_CEID_Decoder, df_CEID_Decoder_sq, how='inner', left_on=['ENTITY', 'CEID','OWNED_BY', 'ROLLUP_END_TIME'], right_on=['ENTITY', 'CEID','OWNED_BY', 'MAX_ROLLUP_END_TIME'])

df_CEID_Decoder_joined.rename(columns={'OWNED_BY':'OwnedBy'}, inplace=True)
df_CEID_Decoder_joined.drop(['ROLLUP_END_TIME'], axis=1, inplace=True)

tablenm8 = 'ToolAvailability.XEUS_CEID_Decoder_stg'
insert_succeeded, error_msg = uploadDFtoSQL(tablenm8, df_CEID_Decoder_joined, truncate=False, server='sql2377-fm1-in.amr.corp.intel.com,3180', database='SCDA')
log(insert_succeeded, project_name=project_name, data_area=tablenm8, row_count=df_CEID_Decoder_joined.shape[0], error_msg=error_msg)
####import CEID_Decoder from XEUS and land it in staging table - end####


log('I', project_name=project_name, data_area='XEUS F28 Data', row_count=0)


