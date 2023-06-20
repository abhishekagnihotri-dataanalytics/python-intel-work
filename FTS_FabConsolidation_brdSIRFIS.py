__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Business Reference data from GSMDW DB to GSCDW DB "
__schedule__ = "Once Daily at 4AM"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import querySQL, uploadDFtoSQL, executeSQL, executeStoredProcedure
import pandas as pd
from Logging import log
from Project_params import params
import os
#import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Password import accounts, decrypt_password
# remove the current file's parent directory from sys.path
# try:
#     sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
# except ValueError:  # Already removed
#     pass


project_name = 'Fab Consolidation'
data_area = 'PaymentTerm'

# initialize variables
source_server = 'sql1717-fm1-in.amr.corp.intel.com,3181'
source_db = 'gsmdw'
dest_db = 'GSCDW'
LoadBy = 'SQL-1717'
params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

# Payment Term Day count
fromTable = 'dbo.Payment_Term_Day_Count'
toTable = 'Stage.brdPaymentTermDayCount'
query = """SELECT *
           FROM {}""".format(fromTable)


query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
if query_succeeded:
    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = LoadBy
    # upload dataframe to SQL
    insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, source_db,
                                                                              dest_db))
        table_name = '[Base].[brdPaymentTermDayCount]'
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Base].[brdPaymentTermDayCount]")
            Insert_query = """insert into [GSCDW].[Base].[brdPaymentTermDayCount]
            SELECT *  FROM [GSCDW].[Stage].[brdPaymentTermDayCount]"""
            insert_succeeded, error_msg = executeSQL(Insert_query)
            if insert_succeeded:
                print("Successfully copied data from staging to base table")
            else:
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)

            table_name = '[GSCDW].[Stage].[brdPaymentTermDayCount]'
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
            if truncate_succeeded:
                print("Successfully truncated table [Stage].[brdPaymentTermDayCount]")
            else:
                log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)
        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area='PaymentTermDayCount', row_count=df.shape[0], error_msg=error_msg)
else:
    print(error_msg)
    log(query_succeeded, project_name=project_name, data_area='PaymentTermDayCount', row_count=df.shape[0],
        error_msg=error_msg)


# SIRFISSpendsAnalysisCKMTPrice
fromTable = 'slm.SIRFIS_Spends_Analysis_CK_MT_Price'
toTable = 'Stage.brdSIRFISSpendsAnalysisCKMTPrice'
query = """SELECT *
           FROM {}""".format(fromTable)

query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
if query_succeeded:
    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = LoadBy
    insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)  # upload dataframe to SQL
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, source_db,
                                                                              dest_db))
        table_name = '[Base].[brdSIRFISSpendsAnalysisCKMTPrice]'
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Base].[brdSIRFISSpendsAnalysisCKMTPrice]")
            Insert_query = """insert into [GSCDW].[Base].[brdSIRFISSpendsAnalysisCKMTPrice]
            SELECT *  FROM [GSCDW].[Stage].[brdSIRFISSpendsAnalysisCKMTPrice]"""
            insert_succeeded, error_msg = executeSQL(Insert_query)
            if insert_succeeded:
                print("Successfully copied data from staging to base table")
                table_name = '[Stage].[brdSIRFISSpendsAnalysisCKMTPrice]'
                truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
                if truncate_succeeded:
                    print("Successfully truncated table [Stage].[brdSIRFISSpendsAnalysisCKMTPrice]")
                else:
                    log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                        error_msg=error_msg)
            else:
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)

        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area='SIRFISSpendsAnalysisCKMTPrice', row_count=df.shape[0],
        error_msg=error_msg)
else:
    print(error_msg)
    log(query_succeeded, project_name=project_name, data_area='SIRFISSpendsAnalysisCKMTPrice', row_count=df.shape[0],
        error_msg=error_msg)


# SIRFISSpendsAnalysisTechTranslator
fromTable = 'slm.SIRFIS_Spends_Analysis_Tech_Translator'
toTable = 'Stage.brdSIRFISSpendsAnalysisTechTranslator'
query = """SELECT *
           FROM {}""".format(fromTable)

query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
if query_succeeded:
    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = LoadBy
    insert_succeeded, error_msg = uploadDFtoSQL(toTable, df, truncate=True)  # upload dataframe to SQL
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], toTable, source_db,
                                                                              dest_db))
        table_name = '[Base].[brdSIRFISSpendsAnalysisTechTranslator]'
        sp_name = 'ETL.spTruncateTable'
        truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
        if truncate_succeeded:
            print("Successfully truncated table [Base].[brdSIRFISSpendsAnalysisTechTranslator]")
            Insert_query = """insert into [GSCDW].[Base].[brdSIRFISSpendsAnalysisTechTranslator]
            SELECT *  FROM [GSCDW].[Stage].[brdSIRFISSpendsAnalysisTechTranslator]"""
            insert_succeeded, error_msg = executeSQL(Insert_query)
            if insert_succeeded:
                print("Successfully copied data from staging to base table")
                table_name = '[Stage].[brdSIRFISSpendsAnalysisTechTranslator]'
                truncate_succeeded, error_msg = executeStoredProcedure(sp_name, table_name)
                if truncate_succeeded:
                    print("Successfully truncated table [Stage].[brdSIRFISSpendsAnalysisTechTranslator]")
                else:
                    log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                        error_msg=error_msg)
            else:
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)

        else:
            log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                error_msg=error_msg)

    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area='SIRFISSpendsAnalysisTechTranslator', row_count=df.shape[0],
        error_msg=error_msg)
else:
    print(error_msg)
    log(query_succeeded, project_name=project_name, data_area='SIRFISSpendsAnalysisTechTranslator', row_count=df.shape[0],
        error_msg=error_msg)

