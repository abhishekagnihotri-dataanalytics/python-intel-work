__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script loads data for the GSM_Quality tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"""
__schedule__ = "Daily at 12:05 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from time import time
import shutil
from Project_params import params
from Helper_Functions import uploadDFtoSQL, executeSQL, getSQLCursorResult
from Logging import log, log_warning

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()

    # Initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('chelsea.gorius@intel.com')  # add Megan to list of email error receivers
    shared_drive_folder_path = r"\\vmsoapgsmssbi06.amr.corp.intel.com\gsmbi\SSIS\Quality"
    excel_file = "2022 EPIC Roadmap.csv"  #"2021 SCQI Roadmap.csv"

    # Read from Roadmap Excel file
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    if excel_file not in file_list:
        log_warning(project_name='Quality', data_area='SCQI Road Map', file_path=shared_drive_folder_path, warning_type='Missing')
    else:  # file exists
        df = pd.read_csv(os.path.join(shared_drive_folder_path, excel_file), delimiter=',', quotechar='"')

        year = excel_file.split(' ')[0]  # parse year from Excel file name

        # Derived Columns
        df['Upload_Date'] = [datetime.today()] * len(df.index)  # create list of duplicate dates the same size as df
        df['BusinessUnit'] = ["TME"] * len(df.index)
        df['Year'] = [year] * len(df.index)
        df.rename(columns={"gfpb": "ESD_ID"}, inplace=True)  # Rename ESD_ID column, as gfpb is the internal name of the ESDID column
        # print(df.columns)

        # Placeholder Columns (prior to select statement override below)
        df['Q1Score'] = None
        df['Q2Score'] = None
        df['Q3Score'] = None
        df['Q4Score'] = None
        df['Q1PAG'] = None
        df['Q2PAG'] = None
        df['Q3PAG'] = None
        df['Q4PAG'] = None

        # SQL statement to get SRC and PAG scores by Supplier ID
        select_stmt = """SELECT src.[ESDSupplierId]
                              ,src.[Q1]/100 AS [Q1SRCScore%]
                              ,src.[Q2]/100 AS [Q2SRCScore%]
                              ,src.[Q3]/100 AS [Q3SRCScore%]
                              ,src.[Q4]/100 AS [Q4SRCScore%]
                              ,pag.[Q1]/100 AS [Q1SCQIPAG%]
                              ,pag.[Q2]/100 AS [Q2SCQIPAG%]
                              ,pag.[Q3]/100 AS [Q3SCQIPAG%]
                              ,pag.[Q4]/100 AS [Q4SCQIPAG%]
                        FROM (SELECT [ESDSupplierId], [Q1], [Q2], [Q3], [Q4]
                                            FROM (SELECT LEFT([Quarter], 2) AS [Quarter]
                                                        ,RIGHT([Quarter], 4) AS [Year]
                                                        ,dbo.TrimX([ESDSupplierId]) AS [ESDSupplierId]
                                                        ,CONVERT(decimal(5,2), SUM([WeightedScore])) AS SRCScore
                                                FROM src.SupplierScoreCard
                                                WHERE RIGHT([Quarter], 4) = {0}
                                                GROUP BY [ESDSupplierId], [Quarter]) AS SourceTable
                                                PIVOT (SUM(SRCScore) 
                                                FOR [Quarter] IN ([Q1], [Q2], [Q3], [Q4])
                                                ) AS PivotTable
                            ) src
                        LEFT OUTER JOIN (SELECT [ESDSupplierId], [Q1], [Q2], [Q3], [Q4]
                                            FROM (SELECT LEFT([Quarter], 2) AS [Quarter]
                                                        ,RIGHT([Quarter], 4) AS [Year]
                                                        ,dbo.TrimX([ESDSupplierId]) AS [ESDSupplierId]
                                                        ,CONVERT(decimal(5,2), [ScoreInput]) AS PAGScore
                                                FROM src.SupplierScoreCard
                                                WHERE [ItemName] = 'Supplier Improvement Plan Score' AND RIGHT([Quarter], 4) = {0}
                                                ) AS SourceTable
                                                PIVOT (SUM(PAGScore) 
                                                FOR [Quarter] IN ([Q1], [Q2], [Q3], [Q4])
                                                ) AS PivotTable
                            ) pag ON pag.ESDSupplierId = src.ESDSupplierId""".format(year)
        print(select_stmt)
        select_success, cursor, error_msg = getSQLCursorResult(select_stmt)
        # print(cursor)
        # print(type(cursor))
        if not select_success:
            log(select_success, project_name='Quality', data_area='SCQI Road Map', row_count=0, error_msg=error_msg)  # log failed select
        else:
            scores = dict()
            for row in cursor:
                # Load all the scores into a dictionary by supplier id
                scores[row[0]] = {'Q1_SRC': row[1], 'Q2_SRC': row[2], 'Q3_SRC': row[3], 'Q4_SRC': row[4],
                                  'Q1_PAG': row[5], 'Q2_PAG': row[6], 'Q3_PAG': row[7], 'Q4_PAG': row[8]}
                # print(row)
            # print(scores.keys())

            for index, row in df.iterrows():
                try:
                    supl_id = str(int(getattr(row, 'ESD ID')))
                except ValueError: # case when Supplier ID is not a number
                    continue
                try:
                    df.at[index, 'Q1Score'] = scores[supl_id]['Q1_SRC']
                    df.at[index, 'Q2Score'] = scores[supl_id]['Q2_SRC']
                    df.at[index, 'Q3Score'] = scores[supl_id]['Q3_SRC']
                    df.at[index, 'Q4Score'] = scores[supl_id]['Q4_SRC']
                    df.at[index, 'Q1PAG'] = scores[supl_id]['Q1_PAG']
                    df.at[index, 'Q2PAG'] = scores[supl_id]['Q2_PAG']
                    df.at[index, 'Q3PAG'] = scores[supl_id]['Q3_PAG']
                    df.at[index, 'Q4PAG'] = scores[supl_id]['Q4_PAG']
                except KeyError:  # case when Supplier ID is not in the dictionary (i.e. no score for that supplier)
                    continue

            # Map Columns from CSV to SQL
            keep_columns = ['Year', 'Title', 'ESD ID', 'Remain on Annual_x00', 'BusinessUnit', 'Business-Org',
                            'Segmentation', 'Annual-Award-Target', 'Commodity-Manager', 'Q1Score', 'Q2Score', 'Q3Score',
                            'Q4Score', 'SRC category <', 'Min-Quality-Assessme', 'Assessment-Minimum-S', 'Assessment-Date',
                            'Q1PAG', 'Q2PAG', 'Q3PAG', 'Q4PAG', 'ESG Expectations', 'Notes', 'Upload_Date']
            df = df[keep_columns]  # manually change column order to match database table

            # Delete values from table that are in the current Roadmap Excel file
            delete_statement = """DELETE FROM {0} WHERE [Year] = {1}""".format(params['Table_RoadMap'], year)
            delete_success, error_msg = executeSQL(delete_statement)
            if not delete_success:
                log(delete_success, project_name='Quality', data_area='SCQI Road Map', row_count=0, error_msg=error_msg)  # log failed delete
            else:  # successful delete
                # Insert current Roadmap values
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_RoadMap'], data=df, categorical=['ESD ID'], truncate=False, driver="{SQL Server}")
                log(insert_succeeded, project_name='Quality', data_area='SCQI Road Map', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_RoadMap']))
                    update_success, error_msg = executeSQL("""UPDATE {0} SET [AnnualAwardTarget] = 'N/A' WHERE [AnnualAwardTarget] IS NULL AND [Year] = {1}""".format(params['Table_RoadMap'], year))  # replace NULL values with N/A in AnnualAwardTarget field
                    if not update_success:
                        log(delete_success, project_name='Quality', data_area='SCQI Road Map', row_count=0, error_msg=error_msg)  # log failed update
                    shutil.move(os.path.join(shared_drive_folder_path, excel_file), os.path.join(shared_drive_folder_path, 'Archive', excel_file))  # Move Excel file to Archive folder after it has been loaded successfully

    print("--- %s seconds ---" % (time() - start_time))
