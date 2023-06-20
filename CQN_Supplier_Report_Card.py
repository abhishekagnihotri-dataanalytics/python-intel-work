__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads from the Supplier Report Card (SRC) database to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Twice daily at 10:25 AM and 4:25 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import uploadDFtoSQL, querySQL, executeSQL
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    table = "src.SupplierScoreCard"
    project_name = "GSM_Quality"
    data_area = 'SRC Scorecard'

    # remove recent 2 years of data
    sqlQuery = """DELETE [src].[SupplierScoreCard] WHERE RIGHT(Quarter,4) >= YEAR(GETDATE()) - 2"""
    success_bool, error_msg = executeSQL(statement=sqlQuery)
    if not success_bool:
        print(error_msg)
        log(success_bool, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        SRCQuery = """SELECT [SupplierId]
                      ,[ESDSupplierId]
                      ,[SupplierName]
                      ,[UnitName]
                      ,[TMEName] AS [Name]
                      ,[TMEDept] AS [Dept]
                      ,[CategoryName]
                      ,[ItemName]
                      ,[ScoringGroupName]
                      ,[ScoredAt]
                      ,[ScoreInput]
                      ,[ScoreCalculated]
                      ,[MaxPoints]
                      ,[Weight]
                      ,[WeightedScore]
                      ,[WeightedMaxScore]
                      ,[PointsLost]
                      ,CAST([Comment] AS NVARCHAR(2000)) AS [Comment]  --,CONVERT(NVARCHAR(2000),[Comment]) AS [Comment]
                      ,LTRIM(RTRIM([Quarter])) AS [Quarter]
                      ,[SCQITarget]
                      ,CAST([Segmentation] AS NVARCHAR(50)) AS [Segmentation]
                      ,[LastUpdate]
                      ,[UpdatedBy]
                      ,[Locked]
                      ,[LockDate]
                      ,N'sql2447-fm1-in.amr.corp.intel.com,3181.OnlineSRC.dbo.vwReportSRCScore' AS [Source]
                      ,[LineRestricted]
                      ,[SupplierRestricted]
                      ,[DeptRestricted]
                      ,[LineItemDescription]
                      ,CAST('System' AS NVARCHAR(8)) AS [SysCreatedBy]
                      ,CAST(GETDATE() AS datetime2(2)) AS [SysCreated]
                      FROM [OnlineSRC].[dbo].[vwReportSRCScore]
                      WHERE RIGHT(Quarter,4) >= YEAR(GETDATE()) - 2"""
        success_bool, src_df, error_msg = querySQL(statement=SRCQuery, server="sql2447-fm1-in.amr.corp.intel.com,3181", database="OnlineSRC")
        if not success_bool:
            print(error_msg)
            log(success_bool, project_name=project_name, data_area=data_area, error_msg=error_msg)
        else:
            OwningQuery = """SELECT [SupplierName], [OwningDept] FROM [src].[OwningSupplier]"""
            success_bool, owning_df, error_msg = querySQL(statement=OwningQuery)
            if not success_bool:
                print(error_msg)
                log(success_bool, project_name=project_name, data_area=data_area, error_msg=error_msg)
            else:
                df = src_df.merge(owning_df, how='left', on='SupplierName')

                success_bool, error_msg = uploadDFtoSQL(table, df, columns=df.columns, truncate=False, chunk_size=5000, driver="{ODBC Driver 17 for SQL Server}")  # DO NOT TRUNCATE THE TABLE
                log(success_bool, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)  # row_count is automatically set to 0 if error
                if success_bool:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                else:
                    print(error_msg)
