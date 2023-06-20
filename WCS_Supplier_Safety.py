__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads from Excel files sent to the gsmariba@intel.com account to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181 for use in the GSM_Supplier_Safety tabular model"
__schedule__ = "Daily at 5:55 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from xlrd.biffh import XLRDError
from datetime import datetime
from time import time
import shutil
from Project_params import params
from Helper_Functions import downloadEmailAttachment, loadExcelFile, uploadDFtoSQL, executeSQL
from Logging import log, log_warning


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def removeBlankColumns(df:pd.DataFrame):
    data_frame = df

    blank_columns = list()
    for column_name in df.columns:
        if data_frame[column_name].isnull().all():
            blank_columns.append(column_name)
    # print(blank_columns)
    data_frame.drop(blank_columns, axis=1, inplace=True)

    return data_frame


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'Supplier Safety'
    package_name = os.path.basename(__file__)  # automatically adds the name of the current script
    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\WCS\Safety"

    ### Construct Secure Factory ###
    file_list = downloadEmailAttachment(shared_drive_folder_path, email_subject="CS Factory", file='.xlsx', exact_match=False, delete_email=True)
    # file_list = ['Intel_Supplier_Status_12_31.xlsx']  # manually load file by name from Shared Drive
    if not file_list:  # file list is empty
        log_warning(project_name=project_name, package_name=package_name, data_area='Construct Secure: Factory', file_path='gsmariba@intel.com inbox', warning_type='Missing')
    for excel_file in file_list:
        if not excel_file.startswith('~') and 'image' not in excel_file:  # find Excel file by name in Shared Drive
            try:
                excel_sheet_name = 'Intel Supplier Status'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                df['Action_Plan'] = None
                df['Help_Needed'] = None
                df['Source'] = 'Factory'

                # format date columns for SQL insert
                try:
                    date_columns = ['Enrollment Date', 'Expiration Date']
                    for col in date_columns:
                        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
                except KeyError:
                    log(False, project_name=project_name, data_area='Construct Secure: Factory', error_msg='Column name not found in file. Perhaps the header row changed positions?')
                    continue  # move on to the next file

                df.drop(['Approval Date'], axis=1, inplace=True, errors='ignore')  # remove this column from the Excel file if present

                # set report date
                if datetime.today().day < 21:  # Check when file was sent in month, if in or after 3rd week then assume the report is for that month and not the next
                    report_month = 12 if int(datetime.today().month) == 1 else int(datetime.today().month) - 1  # set month as the previous month
                    report_year = int(datetime.today().year) - 1 if report_month == 12 else int(datetime.today().year)  # match year accordingly
                else:
                    report_month = datetime.today().month
                    report_year = datetime.today().year
                df['Report_Date'] = datetime(report_year, report_month, 1)

                if df.shape[1] != 23:  # if number of columns does not match SQL
                    log(False, project_name=project_name, data_area='Construct Secure: Factory', row_count=0, error_msg='Excel file is missing columns. Please check and correct.')
                    continue
                # print(df.columns)

                # reorder columns to match SQL Server data table
                sql_table_columns = ['Supplier_Name', 'Enrollment_Date', 'Expiration_Date', 'Supplier_ID', 'Region',
                                     'Division', 'Supplier_Contact_Name', 'Supplier_Contact_Email', 'Status', 'Approval',
                                     'Prequalification_Status', 'Failure_Reason', 'Completion_Percentage', 'Safety_Score',
                                     'Safety_Program_Elements', 'Special_Elements', 'Safety_Management_Systems',
                                     'Discrepancies', 'Whats_Left', 'Action_Plan', 'Help_Needed', 'Source', 'Report_Date']

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_Construct_Secure'], data=df, categorical=['supl_id'], columns=sql_table_columns, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Construct Secure: Factory', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_Construct_Secure']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append('ConstructSecure Factory {0}-{1}.xlsx'.format(report_year, '0' + str(report_month) if len(str(report_month)) < 2 else str(report_month)))

                    # Update Supplier IDs using previous data in Construct Secure table
                    query = """
                            WITH supplier_id_mapping
                            AS (
                                SELECT Supplier_Name
                                    ,Supplier_ID
                                    ,ROW_NUMBER() OVER(PARTITION BY [Supplier_Name] ORDER BY [Report_Date] DESC) AS latest_record
                                FROM wcs.Construct_Secure_Supplier_Status
                                WHERE Supplier_ID IS NOT NULL
                                    AND [Source] = 'Factory'
                                    AND [Report_Date] < '{report_date}'
                            )
                            MERGE wcs.Construct_Secure_Supplier_Status T
                            USING (SELECT * FROM supplier_id_mapping WHERE latest_record = 1) S
                            ON T.[Supplier_Name] = S.[Supplier_Name]

                            WHEN matched AND T.[Report_Date] = '{report_date}' AND T.[Source] = 'Factory' AND T.[Supplier_ID] IS NULL THEN
                            UPDATE
                                SET T.[Supplier_ID] = S.[Supplier_ID]
                            ;""".format(report_date='{0}-{1}-01'.format(report_year, report_month))

                    update_succeeded, error_msg = executeSQL(query)
                    if not update_succeeded:
                        log(update_succeeded, project_name=project_name, data_area='Construct Secure: Factory', error_msg=error_msg)

            except XLRDError as error:
                log(False, project_name=project_name, data_area='Construct Secure: Factory', error_msg=error)

    ### Construct Secure - Construction ###
    file_list = downloadEmailAttachment(shared_drive_folder_path, email_subject="CS Construction", file='.xlsx', exact_match=False, delete_email=True)
    # file_list = ['ConstructSecure Construction 2022-07.xlsx']  # manually load file by name from Shared Drive
    if not file_list:
        log_warning(project_name=project_name, package_name=package_name, data_area='Construct Secure: Construction', file_path='gsmariba@intel.com inbox', warning_type='Missing')
    for excel_file in file_list:
        if not excel_file.startswith('~') and 'image' not in excel_file:  # find Excel file by name in Shared Drive
            try:
                excel_sheet_name = 'Intel Construction Status'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=1)

                df = removeBlankColumns(df)
                df['Source'] = 'Construction'

                # format date columns for SQL insert
                date_columns = ['Enrollment Date', 'Expiration Date']
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')

                # set report date
                if datetime.today().day < 21:  # Check when file was sent in month, if in or after 3rd week then assume the report is for that month and not the next
                    report_month = 12 if int(datetime.today().month) == 1 else int(datetime.today().month) - 1  # set month as the previous month
                    report_year = int(datetime.today().year) - 1 if report_month == 12 else int(datetime.today().year)  # match year accordingly
                else:
                    report_month = datetime.today().month
                    report_year = datetime.today().year
                df['Report_Date'] = datetime(report_year, report_month, 1)

                # print(df.columns)

                sql_table_columns = ['Supplier_Name', 'Enrollment_Date', 'Expiration_Date', 'Status', 'Approval',
                                     'Prequalification_Status', 'Failure_Reason', 'Completion_Percentage',
                                     'Whats_Left', 'Source', 'Report_Date']

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_Construct_Secure'], data=df, columns=sql_table_columns, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Construct Secure: Construction', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_Construct_Secure']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append('ConstructSecure Construction {0}-{1}.xlsx'.format(report_year, '0' + str(report_month) if len(str(report_month)) < 2 else str(report_month)))

            except XLRDError as error:
                log(False, project_name=project_name, data_area='Construct Secure: Construction', row_count=0, error_msg=error)

    ### EHS HLVEs ###
    file_list = downloadEmailAttachment(shared_drive_folder_path, email_subject="HLVE", file='.xlsx', exact_match=False, delete_email=True)
    # file_list = ['Published EHS Nov19-July2022.xlsx']  # manually load file by name
    if not file_list:
        log_warning(project_name=project_name, package_name=package_name, data_area='EHS HLVEs', file_path='gsmariba@intel.com inbox', warning_type='Missing')
    for excel_file in file_list:
        if not excel_file.startswith('~') and 'EHS' in excel_file:  # find Excel file by name in Shared Drive
            # parse year from Excel file name
            try:
                excel_file_year = int(excel_file.split(' ')[-1][:4])
            except ValueError:
                excel_file_year = 2019
            # print(excel_file_year)

            # delete values from table that are in the Excel file
            delete_statement = """DELETE FROM {0} WHERE Local_Event_Date >= '{1}-01-01'""".format(params['Table_EHS'], excel_file_year)
            delete_success, error_msg = executeSQL(delete_statement)
            if not delete_success:
                log(delete_success, project_name=project_name, data_area='EHS HLVEs', row_count=0, error_msg=error_msg)
            else:
                excel_sheet_name = 'HLVE Details'
                try:
                    df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)

                    df = removeBlankColumns(df)
                    df['Local Event Date'] = pd.to_datetime(df['Local Event Date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')  # format date column for SQL insert
                    df['Upload_Date'] = pd.to_datetime('today')  # add upload date to dataframe

                    sql_table_columns = ['Supplier_Name', 'Supplier_ID', 'Classification', 'Event_Campus', 'Event_Title',
                                         'Type', 'Event_Level_Risk', 'HLVE', 'Contract_Emp_Involved', 'Contract_Emp_Type',
                                         'Event_ID', 'Specific_Area', 'Local_Event_Date', 'Organizational_Entity',
                                         'Report', 'Status', 'Year', 'Upload_Date']

                    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_EHS'], data=df, columns=sql_table_columns, categorical=['Supplier ID'], truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='EHS HLVEs', row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_EHS']))
                        successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                        renamed_files.append(excel_file)

                except XLRDError as error:
                    log(False, project_name=project_name, data_area='EHS HLVEs', row_count=0, error_msg=error)

    # ### OLD Escalations Tracker -- unused now ###
    # shared_drive_folder_path2 = r"\\VMSPFSFSEG05.amr.corp.intel.com\Risk_Assessment\Source Files for Power BI"
    # excel_file = "Monthly SCS Escalation Tracker.xlsx"
    # excel_sheet_name = "Sheet1"
    #
    # df = loadExcelFile(os.path.join(shared_drive_folder_path2, excel_file), excel_sheet_name, header_row=2)
    #
    # # Data Preprocessing
    # df.drop(columns=[1, 'Current Status for Power BI'], inplace=True)  # remove extra columns
    # df.rename(columns={'Supplier ': 'Supplier'}, inplace=True)  # fix column name
    # # print(df.columns)
    #
    # df = df.melt(id_vars=['ESDID', 'Supplier', 'Org'], var_name='Month', value_name='Level')  # melt is the unpivot method in python
    #
    # df = df[df['ESDID'].notna()]  # filter rows where Supplier ESDID is not blank
    # df = df[df['Level'].notna()]  # filter rows where Escalation Level is not blank
    #
    # insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_Escalations'], data=df, categorical=['ESDID'], truncate=True, driver="{SQL Server}")
    # log(insert_succeeded, project_name=project_name, data_area='Escalations Tracker', row_count=df.shape[0], error_msg=error_msg)
    # if insert_succeeded:
    #     print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_Escalations']))

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    print("--- %s seconds ---" % (time() - start_time))
