__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads OA data from EDW to GSCDW DB"
__schedule__ = "Once Daily at 4AM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from Logging import log
from Helper_Functions import querySQL, uploadDFtoSQL, executeSQL, executeStoredProcedure
from Project_params import params


# remove the current file's parent directory from sys.path
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
   pass


project_name='FabConsolidation'
data_area='CTS'
stage_table = 'Stage.CurrentCTSFull'
base_table =  'Base.CurrentCTSFull'
LoadBy = 'CTS_DATA'
source = 'sql1077'
dest_db = 'GSCDW'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

query = """SELECT [SnapshotDate]
      ,[SnapshotYYYYMMDD]
      ,[CTSKey]
      ,[OriginalRowStatus]
      ,[DeletedSW]
      ,[Id]
      ,[AreaCoordinator]
      ,[Team]
      ,[site]
      ,[EventType(s)]
      ,[EntityCode]
      ,[Ceid]
      ,[SrcCEID]
      ,[SrcProcess]
      ,[SrcSite]
      ,[SrcEntityCode]
      ,[Ueid]
      ,[NeedId]
      ,[Conversion]
      ,[Source]
      ,[Status]
      ,[ProcureStatus]
      ,[SchedID]
      ,[CsiFulfilled]
      ,[CE]
      ,[state]
      ,[public]
      ,[ProcureBy]
      ,[line datetime]
      ,[CND]
      ,[UpdatedAt]
      ,[category]
      ,[ProcurementStatus]
      ,[ProcureByYYYYMMDD]
      ,[PConversion]
      ,[Comment]
      ,[NextAction]
      ,[NextActionChangeDate]
      ,[NextActionPrevValue]
      ,[TSB]
      ,[TSBChangeDate]
      ,[TSBPrevValue]
      ,[RefBOM]
      ,[RefBOMChangeDate]
      ,[RefBOMPrevValue]
      ,[DeleteStatusChangeDate]
      ,[DueBy]
      ,[DueByChangeDate]
      ,[DueByPrevValue]
      ,[CSILoadChangeDate]
      ,[CSILoad]
      ,[Quote]
      ,[QuoteLastChangeDate]
      ,[CancelBuy]
      ,[RowStatus]
      ,[LastChangeDate]
      ,[RowAddedDate]
      ,[RowDeletedDate]
      ,[changeDate_public]

      from [dbo].[CurrentCTS_Full]
           """

#, getdate() as LoadDtm
#, 'SQL1077' as LoadBy
query_succeeded, df, err_msg=querySQL(statement=query, server='sql1077-lc-in.ger.corp.intel.com,3180', database='CTS_DATA')

df['LoadDtm'] = pd.to_datetime('today')
df['LoadBy'] = LoadBy

# upload dataframe to SQL
insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df)
if insert_succeeded:
    print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, source, dest_db))


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
