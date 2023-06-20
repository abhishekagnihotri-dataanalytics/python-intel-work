__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SES_Data tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import datetime, timezone
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.client_request_exception import ClientRequestException
from Helper_Functions import getLastRefresh, loadExcelFile, getSQLCursorResult, uploadDFtoSQL, map_columns
from Logging import log, log_warning
from Password import accounts


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":

    ### BEGIN MOR Tool Costs Section ###
    # initialize variables
    project_name = 'Substrate Equipment'
    data_area = 'MOR Tool Costs'
    file_path = 'https://intel.sharepoint.com/:x:/r/sites/MSOGSEMATSSESSCPandIE/Automation/MOR%20Tool%20Costs/MOR%20Tool%20Costs.xlsx?d=wb443f2623a9b471892cef1920970131b&csf=1&web=1&e=vRTV2t'
    sheet_name = 'Working'
    table = 'ats.Substrate_Equipment_Tool_Costs'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file
    df = loadExcelFile(file_path, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
    if len(df.index) == 0:
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
    else:
        try:
            df.drop(["Please note, File is not in alphabetical order. Please don't sort. Use filters.\n\nIE Notes:",
                     "Previous SEAT Costs (Q3'20)", "Delta (since last cycle)", "SEAT Tool Group Cost", "INTEL Tool Group Install Cost",
                     "x", "INSTALL COMMENTS", "NTM Cost (3% OR 5%?)", "SHIPPING COST", "AUTOMATION", "PTP", "MORp LRT-L \nQ4'22\n",
                     "Clean up filter", "In MORp or process flow", "Disposition ", "SES owned ", "Unnamed: 40", "IE Name"
                     ], axis=1, inplace=True, errors='ignore')  # remove unnecessary columns

            last_column = 'x Ref vs Lynx 2.2, LRT-L capital wk bk, Ti Sched ww20'
            df.rename(columns={'CEID INDICATOR\n\nWIF or POR?': 'CEID INDICATOR',
                               'x Ref vs Lynx 2.2, LRT-L capital wk bk, Ti Sched ww20': 'Program Reference'}, inplace=True)

            tf_columns = ['FOK (Yes/No)', 'Linked Tool (Yes/No)', 'Uni-Cassette (Yes/No)', 'w/Flipper (Yes/No)']
            for col in tf_columns:
                df[col] = df[col].apply(lambda x: True if isinstance(x, str) and x.lower() in ['yes', 'y'] else
                                                  False if isinstance(x, str) and x.lower() in ['no', 'n'] else
                                                  None).astype(bool)  # value if blank in Excel file

            # Format money column as decimal for SQL upload
            seat_column = [col for col in df.columns if col.startswith('New SEAT Costs')][0]  # Determine name of SEAT Cost Column
            df[seat_column] = pd.to_numeric(df[seat_column], errors='coerce').astype(float).round(decimals=2)  # Round to two decimal places
            df['INTEL Total Tool Group Cost'] = pd.to_numeric(df['INTEL Total Tool Group Cost'], errors='coerce').astype(float).round(decimals=2)

            # Format numeric columns as integer for SQL upload
            int_columns = ['No. of EFEMs', 'No. of Load Ports per EFEM']
            for col in int_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df['LoadDtm'] = datetime.today()
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
            df['LoadSource'] = 'MOR'

            # # Uncomment the below line of code to debug truncation error in SQL insert
            # map_columns(table, df, display_result=True)

            # Load data to Database
            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Missing column in the {0} Excel File.'.format(data_area))
        except IndexError:  # thrown by line 50 if no columns named "New SEAT Costs"
            log(False, project_name=project_name, data_area=data_area,error_msg='Column "New SEAT Costs" not found in Smartsheet Summary Report.')

    ### END MOR Tool Costs Section ###

    ### BEGIN Tool Install Schedule Section ###
    data_area = 'Tool Install Schedule'
    sp_site = "https://intel.sharepoint.com/sites/MSOGSEMATSSESSCPandIE"
    source_relative_url = '/sites/MSOGSEMATSSESSCPandIE/Automation/SPTD Tool Install Schedule'
    file_prefix = 'TI Schedule'
    # file_path = 'https://intel.sharepoint.com/:x:/r/sites/MSOGSEMATSSESSCPandIE/Automation/SPTD%20Tool%20Install%20Schedule/2022%20-%20SPTD%20WW30%20TI%20Schedule.xlsx?d=waa8a1edc54854ab4a9a58e6f2ad9df4e&csf=1&web=1&e=s6UPmX'
    sheet_name = 'Working'
    table = 'ats.Substrate_Equipment_Tool_Install_Schedule'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)
    # last_refreshed = datetime.strptime('11/19/2022', '%m/%d/%Y').replace(tzinfo=timezone.utc).astimezone(tz=None)  # hard-code last refresh date for debugging

    try:
        # Connect to SharePoint Online
        client_credentials = ClientCredential(accounts['SharePoint'].client_id, accounts['SharePoint'].client_secret)
        ctx = ClientContext(sp_site).with_credentials(client_credentials)

        # Load source folder information using relative path information
        folder = ctx.web.get_folder_by_server_relative_path(source_relative_url)
        ctx.load(folder)
        ctx.execute_query()

        # Load all files in the folder
        sp_files = folder.files
        ctx.load(sp_files)
        ctx.execute_query()

    except ClientRequestException as error:
        log(False, project_name=project_name, data_area=data_area, error_msg=error.args[1])
        raise

    # iterate over all files in the SharePoint Online folder
    for excel_file in sp_files:
        file_name = excel_file.properties['Name']
        if file_prefix in file_name:  # and 'WW34' in file_name:
            # Determine when file was created
            file_datestamp = datetime.strptime(excel_file.properties['TimeCreated'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).astimezone(tz=None)
            print('Found file: "{0}" which was created at {1}'.format(file_name, file_datestamp))  # format '2022-05-09 04:19:27'

            if file_datestamp > last_refreshed:  # determine if file is newer than the last one loaded into the SQL database table
                print('Loading file "{0}" since it has not yet been uploaded to the database.'.format(file_name))

                # Extract data from Excel file
                file_path = 'https://intel.sharepoint.com/:x:/r' + source_relative_url + '/' + file_name
                sheet_name = file_name.split(' ')[3] + ' TI Schedule - GSC'  # determine sheet name using WW in file name
                df = loadExcelFile(file_path, sheet_name=sheet_name)
                if len(df.index) == 0:
                    log(False, project_name=project_name, data_area=data_area, error_msg="Worksheet named '{sheet_name}' not found".format(sheet_name=sheet_name))
                    raise FileNotFoundError("Worksheet named '{sheet_name}' not found".format(sheet_name=sheet_name))

                # Remove blank columns from DataFrame
                blank_columns = [col for col in df.columns if df[col].isnull().all()]
                # print(blank_columns)
                df.drop(blank_columns, axis=1, inplace=True)

                try:
                    df.drop(['Entity Code - Life', 'Location', 'Contractor', 'Clean Room Class', 'Mobilization Start', 'Mobilization Finish',
                             'CFD', 'Demo Kickoff Meeting Finish', 'Decon Start', 'Decon Finish', 'Disconnect Tool Start', 'Disconnect Tool Finish',
                             'Demo Start', 'Move Out Start', 'Move Out Finish', 'Demo Finish', 'MRCL Float (Work Days)', 'MRCL Float (Calendar Days)',
                             'Prefac Float'], axis=1, inplace=True, errors='ignore')  # remove unnecessary columns

                    df = df.loc[df['Event Type'].str.lower() != "demo"]  # ignore Demo events from load

                    df['Pre-Design Finish'] = pd.to_datetime(df['Pre-Design Finish'], errors='coerce')  # remove "ON HOLD" entries

                    # RDD column may or may not exist in template
                    if 'RDD' in df.columns:
                        df['Required Dock Date'] = df['RDD']
                    else:
                        df['Required Dock Date'] = None
                    df.drop(['RDD'], axis=1, inplace=True, errors='ignore')  # remove original RDD column which may or may not exist

                    df['LoadDtm'] = datetime.today()
                    df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
                    workweeks = [x for x in file_path.split('/')[-1].split(' ') if x.startswith('WW')]  # parse workweek from file name
                    if not workweeks:  # workweek list is empty
                        query = """SELECT ww_int_nbr AS [Work Week]
                                    FROM dbo.Intel_Calendar
                                    WHERE clndr_dt = CONVERT(date, GETDATE())"""
                        query_succeeded, result, error_msg = getSQLCursorResult(query)
                        if query_succeeded:
                            current_workweek = "WW{}".format(result[0][0])
                        else:
                            current_workweek = "WW?"
                        workweeks = [current_workweek,]
                    df['LoadSource'] = "SPTD " + workweeks[0] + " TI Schedule"

                    # # Uncomment the below line of code to debug truncation error in SQL insert
                    # map_columns(table, df, display_result=True)

                    # Load data to Database
                    insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

                except KeyError:
                    log(False, project_name=project_name, data_area=data_area, error_msg='Missing column in the {0} Excel File.'.format(data_area))
    ### END Tool Install Schedule Section ###
