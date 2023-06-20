__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_ATS_SCCI_Proc_1SOT tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 7:10 AM PST"

import os, sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import numpy as np
from re import sub
from datetime import datetime
from time import time
import shutil
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getSQLCursorResult,loadSharePointList, getLastRefresh, map_columns
from Logging import log, log_warning
from Password import accounts


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def convert(tup, dictionary):
    for a, b in tup:
        dictionary[a] = b
    return dictionary


def get_sharepoint_files(sp_site: str, relative_url: str):
    # Connect to SharePoint Online
    client_credentials = ClientCredential(accounts['SharePoint'].client_id, accounts['SharePoint'].client_secret)
    ctx = ClientContext(sp_site).with_credentials(client_credentials)

    # Load folder information using relative path information
    folder = ctx.web.get_folder_by_server_relative_path(relative_url)
    ctx.load(folder)
    try:
        ctx.execute_query()
    except ClientRequestException:
        return []

    # Load all files in the folder
    sp_files = folder.files
    ctx.load(sp_files)
    ctx.execute_query()

    return sp_files


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    params['EMAIL_ERROR_RECEIVER'].append('abhishek.agnihotri@intel.com')
    project_name = 'ATS Operations Dashboard'

    ##### BEGIN PSI OTT #####
    # initialize variables
    data_area = 'PSI OTT'
    sp_site = 'https://intel.sharepoint.com/sites/gscatstestsupplierottmanagement'
    table = 'ats.PSI_OTT'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from SharePoint Online Lists
    df = loadSharePointList(sp_site=sp_site, list_name='PSI OTT Current', decode_column_names=True, remove_metadata=True, last_upload_time=last_refreshed)
    df_archive = loadSharePointList(sp_site=sp_site, list_name='PSI OTT Archive', decode_column_names=True, remove_metadata=True)
    if len(df.index) == 0:  # PSI OTT Current SharePoint List has not been updated since last script run
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{} Current" SharePoint List has not been updated since last run. Skipping.'.format(data_area))
    else:
        if len(df_archive) == 0:  # PSI OTT Archive has not been updated since last refresh
            log(False, project_name=project_name, data_area=data_area, error_msg='Unable retrieve SharePoint List "{} Archive".'.format(data_area))
        else:
            df = df.append(df_archive)  # combine DataFrames

            # Transform data
            keep_columns = ['Title', 'Category', 'ATD', 'ATD On Time to', 'Gap',
                            'Potential Late Penal', 'MM', 'PO#', 'PO Line', 'PO Line Qty', 'RTD',
                            'Description', 'Late Reason', 'Remedy Recommendation', 'CTD',
                            'CCMComments', 'CMComments', 'ReconciledATD', 'ReconciledCTD',
                            'CCMOTTRecommendation', 'CCM', 'CM', 'CCMProgress', 'CMProgress',
                            'GR Date', 'SupplierPartNumber', 'SupplierID', 'POCreateDate']
            try:
                df = df[keep_columns]  # manually change column order to match database table

                # Data type conversions
                for col in ['ATD', 'GR Date', 'CTD', 'RTD', 'ReconciledATD', 'ReconciledCTD', 'POCreateDate']:  # date columns
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date  # format='%Y-%m-%dT%H:%M:%S:%Z'
                for col in ['PO#', 'PO Line', 'Gap', 'SupplierID']:  # integer columns
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                df['Remedy Recommendation'].replace(r'^\s\$\s*\-\s*$', np.nan, regex=True, inplace=True)  # remove random space dash combinations

                # Filter rows to remove where Supplier = "Adder Line"
                df = df.loc[df['Title'].str.lower() != "adder line"]

                # Add columns for logging
                df['LoadDtm'] = pd.to_datetime('today')
                df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

                columns = ['Supplier', 'POType', 'ATD', 'ATDOnTimeToCTD', 'Gap',
                           'CalculatedPenalty', 'MM', 'PONumber', 'POLine', 'POLineQty', 'RTD',
                           'Description', 'LateReason', 'SuggestedPenalty', 'CTD',
                           'CCMComments', 'CMComments', 'ReconciledATD', 'ReconciledCTD',
                           'CMOTTRecommendation', 'CCM', 'CM', 'CCMProgress', 'CMProgress',
                           'GRDate', 'SupplierPartNumber', 'SupplierID','POCreateDate', 'LoadDtm', 'LoadBy']

                # map_columns(table, df, sql_columns=columns)

                # Load data into SQL Server database
                insert_succeeded, error_msg = uploadDFtoSQL(table, df, columns=columns, truncate=True, driver='{ODBC Driver 17 for SQL Server}')
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

            except KeyError as error:
                log(False, project_name=project_name, data_area=data_area, error_msg="Column missing/changed. Full error: {0}".format(error))
    ##### END PSI OTT #####

    ##### BEGIN Test Supplier Capacities #####
    # initialize variables
    data_area = 'Supplier Capacities'
    sp_site = 'https://intel.sharepoint.com/sites/gscatstestall'
    table = 'ats.Test_Supplier_Capacities'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    df = loadSharePointList(sp_site=sp_site, list_name='Test Supplier Capacities', decode_column_names=True, remove_metadata=True, last_upload_time=last_refreshed)
    if len(df.index) == 0:
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{}" SharePoint List has not been updated since last run. Skipping.'.format(data_area))
    else:
        keep_columns = ['ItemType', 'SupplierID','Supplier', 'ItemFamily','Prev.Capacity(unitsp', 'Capacity(unitsperweek',
                        'PlannedCapacity(units','CMComments','Title']
        try:
            df = df[keep_columns]  # remove other columns & reorder columns
            df['ModifiedDateTime'] = pd.to_datetime('today')

            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['SupplierID'], truncate=True)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError as error:
            log(False, project_name=project_name, data_area=data_area, error_msg="Column missing/changed in Test Supplier Capacities List. Full error: {0}".format(error))
    ##### END Test Supplier Capacities #####

    ##### BEGIN PSI Forecast #####
    # initialize variables
    sp_site = "https://intel.sharepoint.com/sites/gscatsscci-SCCITabularModelDataSources/"
    relative_url = "/sites/gscatsscci-SCCITabularModelDataSources/Shared Documents/SCCI Tabular Model Data Sources/ATMBO PSI Forecast Files"
    data_area = 'ATMBO PSI Forecast'
    table = 'ats.ATMBO_PSI_Forecast'

    # Create mapping table to convert Work Week stamp into dates
    query_succeeded, result, error_msg = getSQLCursorResult("""SELECT CONCAT(LEFT(Intel_WW, 4), '.', RIGHT(Intel_WW, 2), '.', CASE WHEN day_of_ww_nbr < 10 THEN CONCAT('0', day_of_ww_nbr) ELSE day_of_ww_nbr END), clndr_dt
                                                               FROM dbo.Intel_Calendar
                                                               WHERE fscl_yr_int_nbr >= 2020 AND (day_of_ww_nbr IS NOT NULL AND day_of_ww_nbr <> 'NULL')"""
                                                            )
    dates = dict()
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
        exit(1)  # stop the file reporting error
    else:
        convert(result, dates)

    # Load information about which PSI Forecast was last loaded to SQL database table
    query_succeeded, result, error_msg = getSQLCursorResult("""SELECT CONCAT(LEFT(Intel_WW, 4), '.', RIGHT(Intel_WW, 2), '.', CASE WHEN day_of_ww_nbr < 10 THEN CONCAT('0', day_of_ww_nbr) ELSE day_of_ww_nbr END) AS [stamp]
                                                               FROM dbo.Intel_Calendar
                                                               WHERE clndr_dt = (SELECT MAX([ModifiedDateTime]) FROM {0})""".format(table)
                                                            )
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
        exit(1)
    else:
        last_loaded_stamp = result[0][0]  # result[0][0].split('_')[-1][:10]
        # last_loaded_stamp = '2022.49.03'  # hard-code last loaded stamp for debug purposes
        print('The last file stamp loaded into the database is: {0}'.format(last_loaded_stamp))

        sp_files = get_sharepoint_files(sp_site=sp_site, relative_url=relative_url)
        if not sp_files:
            error_msg = 'Unable to connect to SharePoint site: {0}'.format(sp_site)
            print(error_msg)
            log(False, project_name=project_name, data_area='ATMBO PSI Forecast', error_msg=error_msg)

        latest_file_name = ''

        # iterate over all files in the SharePoint Online folder
        for excel_file in sp_files:
            # print('File name: {}'.format(excel_file.properties['ServerRelativeUrl']))
            stamp = excel_file.properties['Name'].split('_')[-1][:10]
            if stamp > last_loaded_stamp:  # determine if file is newer than the last one loaded into the SQL database table
                latest_file_name = excel_file.properties['Name']
                print('New file: {}'.format(latest_file_name))

                upload_dt = dates[stamp]  # convert the stamp [Year].[Work Week].[Day of Work Week] into a real date
                print('Uploaded on: {}'.format(upload_dt))

                # Load Excel File from SharePoint Online into Pandas DataFrame
                latest_file_path = 'https://intel.sharepoint.com/:x:/r' + relative_url + '/' + latest_file_name
                df = pd.DataFrame()
                df1 = loadExcelFile(file_path=latest_file_path, sheet_name='RDD Table', header_row=0)
                try:
                    df2 = loadExcelFile(latest_file_path, 'RDD Table_CRO_4th Quarter', header_row=0)
                    df = pd.concat([df1, df2])  # Append second Excel tab to first
                except ValueError as error:
                    if str(error) == "Worksheet named 'RDD Table_CRO_4th Quarter' not found":
                        df = df1
                    else:
                        log(False, project_name=project_name, data_area='ATMBO PSI Forecast', error_msg=error)
                        exit(1)
                # print(df.columns)

                # Get column format to match SQL database table
                keep_columns = ['Product', 'Tool', 'Cycle', 'Site', 'ProcureBy', 'Name', 'Type', 'Material',
                                'Supplier Name', 'Supplier Number', 'OaDescription', 'Quantity', 'UOM', 'Date', 'CND',
                                'Subcategory', 'Repairable', 'FundType', 'Forecast$', 'OAUnit$', 'OaLeadTime',
                                'CPA Month', 'Fund Loaded?', 'Status', 'Line Item', 'Remark', 'CRO', 'Checkpoint',
                                'Quarter', 'Common Product Name', 'Category']
                try:
                    df = df[keep_columns]  # manually change column order to match database table
                except KeyError as error:
                    log(False, project_name=project_name, data_area='ATMBO PSI Forecast', error_msg="Column missing/changed in PSI Forecast file. Full error: {0}".format(error))
                    exit(1)

                # Data massaging for SQL table
                df['Roadmap Segment'] = df['Product'].apply(lambda x: x.split(',')[2] if isinstance(x, str) and len(x.split(',')) > 3 else None)  # Only keep product info before the first comma
                df['Product'] = df['Product'].apply(lambda x: x.split(',')[0] if isinstance(x, str) else None)  # Only keep product info before the first comma

                try:
                    df['Date'] = df['Date'].apply(lambda x: x if isinstance(x, datetime) else datetime.strptime(x, '%d %b %Y'))  # Convert text "9 Apr 2021" to Datetime object

                    money_columns = ['OAUnit$', 'Forecast$']
                    for col in money_columns:
                        df[col] = df[col].apply(lambda x: sub(r'[^\d.]', '', x) if isinstance(x, str) else x)  # Remove text " (OA)" after decimal number
                        df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x == '' else float(x))  # Ignore rows without any values

                    df['Quantity'] = df['Quantity'].apply(lambda x: sub(r'[^\d.]', '', x.split('(')[0]) if isinstance(x, str) and '(' in x  # Parse first number from cases like "1 (1 ST of 1)"
                                                          else sub(r'[^\d.]', '', x) if isinstance(x, str)  # Remove text " EA" after number
                                                          else x)
                    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')  # convert the strings into numbers for insertion into database

                except ValueError as error:  # Catch errors with different format of date text, and values that cannot be cast as int or float
                    print(error)
                    log(False, project_name=project_name, data_area=data_area, row_count=0, error_msg=error)
                    exit(1)

                tf_columns = ['Repairable', 'Fund Loaded?', 'CRO']
                for col in tf_columns:
                    df[col] = df[col].apply(lambda x: True if isinstance(x, str) and x.lower() == 'yes' else False if isinstance(x, str) and x.lower() == 'no' else None)  # Change Yes/No column to True/False

                df['Module'] = df['Tool'].apply(lambda x: x.split('#')[0] if isinstance(x, str) else None)  # Create new column "Module" that takes everything from the "Tool" column before the first
                df['OaLeadTime'] = df['OaLeadTime'].apply(lambda x: float(sub(r'[^\d.]', '', x)) if isinstance(x, str) else float(x))  # Remove text " (OA)" after decimal number

                df['ModifiedDateTime'] = upload_dt  # append modified date

                columns = ['Product', 'Tool', 'Cycle', 'Site', 'Procure By', 'PSIG', 'Type', 'IPN', 'Supplier Name',
                           'Supplier Part Number', 'OA Description', 'Quantity', 'UOM', 'RDD', 'CND', 'Subcategory',
                           'Repairable', 'Fund Type', 'Forecasted Spends', 'OA Unit Price', 'EMS Lead Time',
                           'CPA Month', 'Fund Loaded', 'Status', 'Forecast Line Item', 'Remark', 'CRO Flag',
                           'CRO Checkpoint', 'CRO Quarter', 'Common Product Name', 'Category', 'Product Segment',
                           'Module', 'ModifiedDateTime']

                # # Debugging - uncomment the following two lines if you face an upload error and need to determine the number of characters in a column
                # print(df.dtypes)
                # map_columns(table, df, sql_columns=columns)

                # Insert data into SQL database table
                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, columns=columns, categorical=['Material'], truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ##### END PSI Forecast #####

    ##### BEGIN MPE PSI Forecast #####
    # initialize variables
    sp_site = "https://intel.sharepoint.com/sites/mpecollateralsforecast"
    relative_url = "/sites/mpecollateralsforecast/Shared Documents/General/Forecast and Readiness"
    table = 'ats.MPE_PSI_Forecast'
    data_area = 'MPE PSI Forecast'

    # Load information about which PSI Forecast was last loaded to SQL database table
    query_succeeded, result, error_msg = getSQLCursorResult("""SELECT CONCAT(LEFT(Intel_WW, 4), '.', RIGHT(Intel_WW, 2), '.', CASE WHEN day_of_ww_nbr < 10 THEN CONCAT('0', day_of_ww_nbr) ELSE day_of_ww_nbr END) AS [stamp]
                                                               FROM dbo.Intel_Calendar
                                                               WHERE clndr_dt = (SELECT MAX([ModifiedDateTime]) FROM {0})""".format(table)
                                                            )
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        try:
            last_loaded_stamp = result[0][0]
        except IndexError:
            today_datetime = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)  # strip time from today's date
            dates_reverse_lookup = {value: key for key, value in dates.items()}
            last_loaded_stamp = dates_reverse_lookup[today_datetime]  # get today's stamp from dates dict
            # last_loaded_stamp = '2022.45.02'  # hardcode the last_loaded_stamp
        print('The last file stamp loaded into the database is: {0}'.format(last_loaded_stamp))

        sp_files = get_sharepoint_files(sp_site=sp_site, relative_url=relative_url)
        if not sp_files:
            error_msg = 'Unable to connect to SharePoint site: {0}'.format(sp_site)
            print(error_msg)
            log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)

        # iterate over all files in the SharePoint Online folder
        for excel_file in sp_files:
            print('File name: {}'.format(excel_file.properties['ServerRelativeUrl']))

            stamp = excel_file.properties['Name'].split('_')[-1][:10]
            # print(stamp)
            if stamp > last_loaded_stamp:  # determine if file is newer than the last one loaded into the SQL database table
                latest_file_name = excel_file.properties['Name']
                print('New file: {}'.format(latest_file_name))

                # Load Excel File from SharePoint Online into Pandas DataFrame
                latest_file_path = 'https://intel.sharepoint.com/:x:/r' + relative_url + '/' + latest_file_name
                df = loadExcelFile(file_path=latest_file_path, sheet_name='GSE Forecast', header_row=0)

                df.drop(['Cycle', 'RDD'], axis=1, errors='ignore', inplace=True)  # Remove unused RDD year workweek column

                upload_dt = dates[stamp]  # convert the stamp [Year].[Work Week].[Day of Work Week] into a real date
                # print('Uploaded on: {}'.format(upload_dt))
                df['ModifiedDateTime'] = upload_dt  # append modified date

                columns = ['Product', 'Roadmap Segment', 'Module', 'Site', 'Procure By', 'PSIG', 'IPN', 'Supplier Part Number',
                           'Supplier Name', 'Part Description', 'Quantity', 'Supplier ID', 'UOM', 'RDD', 'Subcategory',
                           'ModifiedDateTime']

                # # Debugging - uncomment the following line of code if you face an upload error
                # map_columns(table, df, sql_columns=columns)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, columns=columns, categorical=['Material', 'Supplier Number'], truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ##### END MPE PSI Forecast #####

    print("--- %s seconds ---" % (time() - start_time))
