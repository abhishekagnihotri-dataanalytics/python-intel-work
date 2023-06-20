__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Learning data to GSCDW DB"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import querySQL, uploadDFtoSQL, executeStoredProcedure, executeSQL
import pandas as pd
from Logging import log
from Project_params import params

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'Supply Chain Data Fluency'
    data_area = 'SABA'
    load_by = 'LearningODS'
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')
    sourceserver = 'lodsdbaas.intel.com,3181'
    sourceDB = 'LearningODS'
    destserver = 'sql2943-fm1-in.amr.corp.intel.com,3181'
    destDB = 'GSCDW'
    # SABA Students
    stage_table = 'Stage.LearningStudents'
    base_table = 'Base.LearningStudents'

    query = """SELECT
                H.[PersonId] 
                ,H.[CourseId] 
                ,H.[SessionId] 
                ,L.[StatusDsc] as StudentStatusDsc
                ,S.StatusDsc as SessionStatusDsc
                ,H.[StartDt] 
                ,H.[CompletionDt] 
                ,P.WWID 
                ,C.[CourseNbr] 
                ,C.[CourseTitleNm] 
                ,C.[CourseDsc] 
                ,P.[OrgUnitID]
                ,P.[SiteCode]
                ,P.[RegionCode]
                FROM 
                (
                SELECT 
                    t1.[PersonId]
                    ,t1.[CourseId] 
                    ,t1.[SessionId]
                    ,t1.[StartDt]
                    ,t1.[CompletionDt]
                    ,t1.[StatusId]
                    ,ROW_NUMBER() OVER (Partition By t1.PersonId, t1.[SessionId], t1.[CourseID] ORDER BY t1.StatusId ASC) AS row_nbr
                FROM [dbo].[StudentHistory] t1
                WHERE t1.[CourseId] IN ('46517','46518','46618','48004','48002','48003','49398','48765','47725','48436','48116', '46921', '47186','47325', '47538','49932')
                )H
                LEFT JOIN (SELECT StatusId, StatusDsc FROM [dbo].[StatusLookup])L ON (H.StatusID = L.StatusID)
                LEFT JOIN (SELECT [PersonId],[WWID],[OrgUnitID],[SiteCode] ,[RegionCode] FROM [dbo].[Person])P ON (H.PersonId = P.PersonId)
                LEFT JOIN (SELECT [CourseId] ,[CourseNbr] ,[CourseTitleNm] ,[CourseDsc] FROM [dbo].[Course] )C ON (H.CourseId = C.CourseId)
                LEFT JOIN (SELECT [SessionId] ,[StatusId] FROM [dbo].[CourseSession] )CS ON H.SessionId = CS.SessionId
                LEFT JOIN (SELECT [StatusId] ,[StatusDsc] FROM [dbo].[StatusLookup] )S ON CS.StatusId = S.StatusId
                WHERE H.row_nbr = 1
                """
    success_bool, df, error_msg = querySQL(query, server=sourceserver, database=sourceDB)
    if success_bool:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = load_by

        # map_columns(stage_table,df)

        # upload dataframe to SQL
        insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True, server=destserver, database=destDB)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, load_by,
                                                                                  params['GSMDW_DB']))

            # Clear base table before attempting to copy data from staging there
            sp_name = 'ETL.spTruncateTable'
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server=destserver, database=destDB)
            if truncate_succeeded:
                print("Successfully truncated table {}".format(base_table))

                # Copy data from Stage table to Base table
                insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table,
                                                                                            copy_from=stage_table)
                insert_succeeded, error_msg = executeSQL(insert_query, server=destserver, database=destDB)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0],
                    error_msg=error_msg)  # log regardless of success or failure
                if insert_succeeded:
                    print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table,
                                                                                          copy_from=stage_table))

                    # Clear stage table after successful insert into Base table
                    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table, server=destserver, database=destDB)
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

#SABA Instructors
    stage_table = 'Stage.LearningInstructors'
    base_table = 'Base.LearningInstructors'

    query = """SELECT DISTINCT
                CS.SessionId
                ,P.EmailAddr
                ,P.FullName
                FROM [LearningODS].[dbo].[CourseSession] as CS
                LEFT JOIN [LearningODS].[dbo].[CourseDelivery] as CD
                ON (CS.CourseDeliveryId = CD.CourseDeliveryId)
                left join LearningODS.dbo.SessionInstructor as SI
                on SI.SessionId = CS.SessionId
                left join LearningODS.dbo.Person as P
                on SI.PersonId = P.PersonId
                WHERE CD.CourseId IN ('46517','46518','46618','48004','48002','48003','49398','48765','47725','48436','48116', '46921', '47186','47325', '47538','49932')
            """
    success_bool, df, error_msg = querySQL(query, server=sourceserver, database=sourceDB)
    if success_bool:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = load_by


        # upload dataframe to SQL
        insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True, server=destserver, database=destDB)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, load_by, params['GSMDW_DB']))

            # Clear base table before attempting to copy data from staging there
            sp_name = 'ETL.spTruncateTable'
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server=destserver, database=destDB)
            if truncate_succeeded:
                print("Successfully truncated table {}".format(base_table))

                # Copy data from Stage table to Base table
                insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table, copy_from=stage_table)
                insert_succeeded, error_msg = executeSQL(insert_query, server=destserver, database=destDB)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # log regardless of success or failure
                if insert_succeeded:
                    print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table, copy_from=stage_table))

                    # Clear stage table after successful insert into Base table
                    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table, server=destserver, database=destDB)
                    if truncate_succeeded:
                        print("Successfully truncated table {}".format(stage_table))
                    else:
                        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            else:
                log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)


#SABA Courses
    stage_table = 'Stage.LearningCourses'
    base_table = 'Base.LearningCourses'

    query = """SELECT DISTINCT
                 CS.SessionId
                ,CS.SessionNbr
                 ,C.CourseId
                ,C.CourseNbr
                ,C.CourseTitleNm
                ,CS.StartDt
                ,CS.EndDt
                ,CS.MaximumStudentCnt
                ,S.StatusDsc
                FROM [LearningODS].[dbo].[CourseSession] as CS
                LEFT JOIN [LearningODS].[dbo].[CourseDelivery] as CD
                ON (CS.CourseDeliveryId = CD.CourseDeliveryId)
                LEFT JOIN [LearningODS].[dbo].[Course] as C
                ON (CD.CourseId = C.CourseId)
                LEFT JOIN [LearningODS].[dbo].[StatusLookup] as S
                ON (CS.StatusId = S.StatusId)
                WHERE CD.CourseId IN ('46517','46518','46618','48004','48002','48003','49398','48765','47725','48436','48116', '46921', '47186','47325', '47538','49932')
                            """
    success_bool, df, error_msg = querySQL(query, server=sourceserver, database=sourceDB)
    if success_bool:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = load_by

         # upload dataframe to SQL
        insert_succeeded, error_msg = uploadDFtoSQL(stage_table, df, truncate=True, server=destserver, database=destDB)
        if insert_succeeded:
            print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], stage_table, load_by, params['GSMDW_DB']))

            # Clear base table before attempting to copy data from staging there
            sp_name = 'ETL.spTruncateTable'
            truncate_succeeded, error_msg = executeStoredProcedure(sp_name, base_table, server=destserver, database=destDB)
            if truncate_succeeded:
                print("Successfully truncated table {}".format(base_table))

                # Copy data from Stage table to Base table
                insert_query = """insert into {copy_to} SELECT * FROM {copy_from}""".format(copy_to=base_table, copy_from=stage_table)
                insert_succeeded, error_msg = executeSQL(insert_query, server=destserver, database=destDB)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # log regardless of success or failure
                if insert_succeeded:
                    print("Successfully copied data from {copy_from} to {copy_to}".format(copy_to=base_table, copy_from=stage_table))

                    # Clear stage table after successful insert into Base table
                    truncate_succeeded, error_msg = executeStoredProcedure(sp_name, stage_table, server=destserver, database=destDB)
                    if truncate_succeeded:
                        print("Successfully truncated table {}".format(stage_table))
                    else:
                        log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            else:
                log(truncate_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        else:
            print(error_msg)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
