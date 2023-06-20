import imaplib
import email
import json
import csv
import requests
import requests.exceptions
from requests_negotiate_sspi import HttpNegotiateAuth
from typing import Union
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from ssl import SSLEOFError
import os
import io
import re  # regular expressions
from datetime import datetime, timezone
import pytz
from codecs import decode
import pandas as pd
import numpy as np
import pyodbc
import smartsheet
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceMalformedRequest, SalesforceResourceNotFound
from hdbcli import dbapi
# import pythoncom
# import win32com.client
import sys
import urllib3
import warnings
from Project_params import params
from Password import accounts, decrypt_password


# Set proxies
os.environ['http_proxy'] = 'http://proxy-dmz.intel.com:912'
os.environ['https_proxy'] = 'http://proxy-dmz.intel.com:912'
os.environ['no_proxy'] = 'icloud.intel.com'  # for Cloud Foundry


class ColumnMap:
    def __init__(self, column_name: Union[str, None], column_number: int):
        if column_name is None:
            self._source_name = '%Unknown%'
        else:
            self._source_name = column_name
        self._source_dtype = '%Unknown%'
        self._source_length = 0
        self._destination_name =  '%Unknown%'
        self._destination_dtype = '%Unknown%'
        self._destination_length = 0
        self._column_number = column_number

    @property
    def source_name(self):
        return self._source_name

    @property
    def source_length(self):
        return self._source_length

    @property
    def source_dtype(self):
        return self._source_dtype

    @property
    def destination_name(self):
        return self._destination_name

    @property
    def destination_dtype(self):
        return self._destination_dtype

    @property
    def destination_length(self):
        return self._destination_length

    @property
    def column_number(self):
        return self._column_number

    def set_source_length(self, length):
        self._source_length = length

    def set_source_dtype(self, data_type):
        self._source_dtype = data_type

    def set_destination_name(self, name):
        self._destination_name = name

    def set_destination_dtype(self, data_type):
        self._destination_dtype = data_type

    def set_destination_length(self, length):
        self._destination_length = length

    def __str__(self):
        header = ''
        if self._column_number == 1:  # Print header with first column
            header = "|Source Column Name                           |Source Data Type         |Source Max Length        |Destination Column Name                      |Destination Data Type    |Destination Max Length   |\n" \
                     "|---------------------------------------------|-------------------------|-------------------------|---------------------------------------------|-------------------------|-------------------------|\n"
        return header + "|{0: <45}|{1: <25}|{2: <25}|{3: <45}|{4: <25}|{5: <25}|".format(self._source_name, str(self._source_dtype), str(self._source_length), self._destination_name, str(self._destination_dtype), str(self._destination_length))


def map_columns(table: str, df: Union[pd.DataFrame, None], display_result: bool = True, export_result_destination: str = None,
                sql_columns: list = None, server: str = None, database: str = None,
                username: str = None, password: str = None) -> dict:
    """Determine column mapping for SQL insert statements.

        Args:
            table: [str] Name of SQL table.
            df: [Pandas DataFrame] Data to be inserted into the SQL table.
            display_result: [bool] Whether to print the column mapping or not. Default True, prints the mapping
            export_result_destination: [str] Name of file and full path of where to export the column mapping. Default None, does not export results
            sql_columns: [list of str] List of columns from SQL table to include.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns: [list of str] List of downloaded file names. If no Excel files were found and downloaded, list is empty.
    """
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    column_dtypes = dict()
    table_schema = table.split('.')[0].replace("[", "").replace("]", "")
    table_name = table.split('.')[1].replace("[", "").replace("]", "")

    # Initialize a mapping of columns using the DataFrame
    if df is None:
        df_columns = list()
    elif any(df.columns.duplicated()):  # Duplicate column name in DataFrame
        print('Duplicate column name in DataFrame. Removing duplicate column for map_column function. This may cause the columns to not align correctly.')
        df = df.loc[:,~df.columns.duplicated(keep='last')].copy()  # Remove duplicate column from DataFrame
        df_columns = list(df.columns)
    else:
        df_columns = list(df.columns)

    i = 1
    for col in df_columns:
        column_dtypes[col] = ColumnMap(column_name=col, column_number=i)

        if df[col].dtype == 'object':
            temp_df = df[~df[col].isnull()]
            if len(temp_df.index) > 0:
                value_1 = temp_df[col].iloc[0]  # first value
                value_2 = temp_df[col].iloc[round(len(temp_df)/2)]  # some value in the middle of the DataFrame
                value_3 =  temp_df[col].iloc[-1]  # last value
                # print('Original dtype: {0}'.format(temp_df[col].dtype))

                data_types = {type(value_1).__name__, type(value_2).__name__, type(value_3).__name__}  # add data types these into a set
                if len(data_types) == 1:  # all objects have the same value
                    new_data_type = data_types.pop()
                else:  # two or more different data types in column
                    new_data_type = 'object'
            else:  # the only values in the column are NULLs
                new_data_type = 'NoneType'
            # print('New dtype: {0}'.format(new_data_type))
            column_dtypes[col].set_source_dtype(new_data_type)
        else:
            column_dtypes[col].set_source_dtype(df[col].dtype)

        # Determine column lengths from DataFrame
        if df[col].dtype == 'int64':
            temp = df[col].map(lambda x: len(str(x)) if isinstance(x, int) else None).max()
        else:
            temp = df[col].map(lambda x: len(x) if isinstance(x, str) else None).max()
        # print(temp)
        column_dtypes[col].set_source_length(temp)
        i += 1

    # Retrieve destination table column ordering from database
    select_statement = "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH\n" \
                       "FROM INFORMATION_SCHEMA.COLUMNS\n" \
                       "WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}';".format(table_schema, table_name)
    if sql_columns is not None:
         select_statement = select_statement[:-1] + " AND COLUMN_NAME IN ('{0}');".format("', '".join(sql_columns).replace('[', '').replace(']', ''))
    # print(select_statement)

    query_succeeded, result, error_msg = getSQLCursorResult(select_statement, server=server, database=database, username=username, password=password)
    if query_succeeded:
        if len(result) == 0:  # Table was not found in the INFORMATION SCHEMA
            print('Unable to find table {} in INFORMATION_SCHEMA.COLUMNS. Please verify you entered the correct server and database.'.format(table))
        elif sql_columns is not None:  # case when column order was entered by the user
            for i in range(len(sql_columns)):
                try:
                    source_column = df_columns[i]
                except IndexError:  # More columns in destination table than source DataFrame
                    source_column = "%Unknown_{}%".format(i)
                    column_dtypes[source_column] = ColumnMap(column_name=None, column_number=i)  # Intentionally exclude source column name

                for j in range(len(result) + 1):  # iterate once more than the number of columns in result
                    if j > len(result) - 1:  # column was not found in result
                        print('Column "{0}" not found in table {1}. Perhaps you spelt the column name incorrectly in your Python script?'.format(sql_columns[i], table))
                    elif sql_columns[i].lower() == result[j][0].lower():  # SQL Server column names are not case sensitive
                        column_dtypes[source_column].set_destination_name(result[j][0])
                        column_dtypes[source_column].set_destination_dtype(result[j][2])
                        column_dtypes[source_column].set_destination_length(result[j][3])
                        break
        else:  # column order not specified by user (use default table ordering)
            i = 0
            for x in result:
                try:
                    source_column = df_columns[i]
                except IndexError:  # More columns in destination table than source DataFrame
                    source_column = "%Unknown_{}%".format(i)
                    column_dtypes[source_column] = ColumnMap(column_name=None, column_number=x[1])  # Intentionally exclude source column name
                finally:
                    column_dtypes[source_column].set_destination_name(x[0])
                    column_dtypes[source_column].set_destination_dtype(x[2])
                    column_dtypes[source_column].set_destination_length(x[3])
                    i += 1  # iterate i

    if display_result:
        for key in column_dtypes.keys():
            print(column_dtypes[key])

    if export_result_destination is not None:
        if os.path.isdir(export_result_destination):  # if user provided directory without file name
            export_result_destination = os.path.join(export_result_destination, '{}_Mapping.csv'.format(table.replace('.', '_')))  # Append default file name
        else:
            pre, file_extension = os.path.splitext(export_result_destination)  # determine file extension entered by the user
            if file_extension.lower() != '.csv':  # file extension is not .csv
                export_result_destination = pre + '.csv'
        try:
            with open(export_result_destination, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Source Column Name', 'Source Data Type', 'Source Max Length', 'Destination Column Name', 'Destination Data Type', 'Destination Max Length'])  # header
                for key in column_dtypes.keys():
                    writer.writerow([column_dtypes[key].source_name, column_dtypes[key].source_dtype, column_dtypes[key].source_length,
                                     column_dtypes[key].destination_name, column_dtypes[key].destination_dtype, column_dtypes[key].destination_length])
        except PermissionError as error_msg:
            print('Unable to write to map_columns destination. Perhaps you still have the file open? Skipping export.\nOriginal Error: {}'.format(error_msg))

    return column_dtypes


def chunks(data: list, n: int):
    """Yield successive n-sized chunks from given object (List, DataFrame, etc.)

        Args:
            data: [list-like] object to chunk
            n: [int] chunk size
    """
    for i in range(0, len(data), n):
        yield data[i:i + n]


def downloadEmailAttachment(destination: str, email_subject: str, email_receiver: str = accounts['GSM Ariba'].username, email_receiver_password: str = accounts['GSM Ariba'].password,
                            file: str = None, exact_match: bool = True, delete_email: bool = False) -> list:
    """Download an email attachment and save to SharedDrive folder.
        
        Args:
            destination: [str] Folder path to store the downloaded Email attachment
            email_subject: [str] Subject of email containing file
            email_receiver: [str] Email address of the account that you would like to download emails from
            email_receiver_password: [str] Password for the email account which you would like to download emails from
            file: [str] File to search for in email (if none provided, all files in email are moved)
            exact_match: [bool] If true, file name must match email exactly. Use false when file name is dynamic to match any file that contains the specified file name
            delete_email: [bool] If true, moves email message to trash after processing.
             
        Returns: [list of str] List of downloaded file names. If no Excel files were found and downloaded, list is empty.
    """
    files_loaded = list()

    # print("Started email login")
    mail = imaplib.IMAP4_SSL(params['EMAIL_SERVER'], params['EMAIL_SERVER_PORT'])  # server, port
    try:
        # print('Attempting email login')
        mail.login(email_receiver, email_receiver_password)  # login to email account

        # Search email Inbox for subjects containing the user provided email subject
        mail.select('Inbox')
        search_query = '(SUBJECT "' + email_subject + '")'
        # print('Searching mailbox')
        result, data = mail.search(None, search_query)
        ids = data[0]
        id_list = ids.split()

        # Iterate over each email (only ones that match the subject field from above)
        for email_id in id_list:
            result, email_data = mail.fetch(email_id, '(RFC822)')  # fetch the email body (RFC822) for the given ID
            raw_email_string = email_data[0][1].decode('utf-8')  # converts byte literal to string removing b''
            email_message = email.message_from_string(raw_email_string)
            print('Reading email with subject: {}'.format(email_message['subject']))
            # print('Email was sent from: {}'.format(email_message['from']))

            # # Load text content from email
            # body = email_message.get_body(preferencelist=('html', 'plain'))
            # if body:
            #     body = body.get_content()
            # # print(body)

            # Download attachments from email
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:  # skip email parts that are not attachments
                    continue

                # Parse file name from attachment
                file_name, encoding = email.header.decode_header(part.get_filename())[0]
                if encoding:  # if file_name is encoded, decode it first
                    file_name = file_name.decode(encoding)
                if '\r' in file_name or '\n' in file_name:  # if file_name has line breaks (new lines), remove them
                    file_name = file_name.replace('\r', '').replace('\n', '')
                # print(file_name)

                if bool(file_name):  # if file exists
                    # print('Found file: {}'.format(file_name))

                    # Check if file name matches user provided argument
                    if file:  # if file name is specified by user, otherwise load all documents
                        if exact_match:  # if the user wants exact name match
                            if file != file_name:  # name of file does not match exactly specified file name
                                continue
                        else:  # if user does not want exact name match
                            if file not in file_name:  # name of file does not contain the specified file name
                                continue

                    # Copy attachment file to destination folder
                    print('Moving file "{0}" to {1}.'.format(file_name, destination))
                    file_path = os.path.join(destination, file_name)
                    if os.path.isfile(file_path):  # check if file already exists in filepath
                        os.remove(file_path)  # if file already exists, remove it and reload
                    with open(file_path, 'wb') as fp:
                        fp.write(part.get_payload(decode=True))
                    files_loaded.append(file_name)

            # Move email to TRASH
            if delete_email:
                # print('Deleting email with subject: {}'.format(email_message['subject']))
                mail.store(email_id, '+FLAGS', '\\Deleted')

        mail.expunge()
        mail.close()
        mail.logout()

    except imaplib.IMAP4.error as error:
        if error.args[0] == b'LOGIN failed.':  # error raised by mail.login() function
            # TODO: add error logging for email login failed
            print("Failing logging into the {} email account!".format(email_receiver))
        else:
            print(error)
            raise error
    except OSError:  # error raised by os.remove() function
        # TODO: add error logging for failed file delete
        print('Unable to remove file prior to reload. Download failed.')
    # except ConnectionResetError as error:
    #   # TODO: add error logging for connection reset

    return files_loaded


def getLastRefresh(project_name: str, data_area: str) -> Union[datetime, None]:
    """Function to get Last Refresh Datetime from SQL audit table

        Args:
            project_name: [str] Name of the project to query in the Audit Processing Log.
            data_area: [str] Name of the data area to query in the Audit Processing Log.

        Returns:
            [datetime] Datetime representing the last refresh or None when unable to determine last refresh date time.

    """
    # get date and time of previous upload
    query = """WITH CTE_ordering AS (
                    SELECT audt.*
                        ,max_audt.[last_check]
                        ,ROW_NUMBER() OVER (PARTITION BY [log_source], [data_area] ORDER BY [entry_date] DESC) AS latest
                    FROM [audit].[processing_log] audt
                    LEFT OUTER JOIN (
                        SELECT [log_source] AS [log_source2]
                              ,MAX([entry_date]) AS [last_check] 
                        FROM [audit].[processing_log] 
                        GROUP BY [log_source]
                    ) max_audt ON audt.[log_source] = max_audt.[log_source2]
                    WHERE audt.[log_type] = 'I'
                        AND audt.[scope] = '{0}'
                        AND audt.[data_area] = '{1}'
                    )
                SELECT [entry_date] AS [Last Refresh Date]
                FROM CTE_ordering
                WHERE latest = 1
            """.format(project_name, data_area)
    query_succeeded, result, error_msg = getSQLCursorResult(query)
    if query_succeeded:
        try:
            local_timezone = datetime.now(timezone.utc).astimezone().tzinfo  # determine current local timezone
            last_upload_time = pytz.timezone('US/Pacific').localize(result[0][0])  # load last_upload_time from SQL query result and set timezone as Pacific time (server default is Pacific time)
            if local_timezone != 'US Pacific':  # if local timezone does not match Pacific timezone
                last_upload_time = last_upload_time.astimezone(local_timezone)  # convert server timezone to local timezone
        except IndexError:  # case when query returns a blank table (meaning the project name/data area combination is not in the audit.processing_log)
            last_upload_time = None  # datetime.today() - timedelta(days=1)
    else:  # case when query against database unable to determine last upload (if the database is down, unreachable, etc.)
        print(error_msg)
        last_upload_time = None
    # print("Last uploaded: {}".format(last_upload_time))

    return last_upload_time


def loadExcelFile(file_path: str, sheet_name: Union[str, int] = None, header_row: Union[int, list] = 0, na_values: list = None, keep_default_na: bool = True, credentials: dict = None, last_upload_time: datetime = None) -> pd.DataFrame:
    """Function to load Excel sheet into Pandas DataFrame.

    Args:
        file_path: Full path to Excel file.
        sheet_name: Sheet(s) to be loaded from Excel file. Default sheet_name=None loads all sheets in file.
        header_row: [int, list of two int, or None] Row(s) that header begins on, 0-indexed. List of two ints implies multi-row header. Default header_row=0 treats first row as header.
        na_values: [list] Additional strings to recognize as NA/NaN. By default, the following values are interpreted as NaN: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’.
        keep_default_na: [bool] Whether or not to include the default NaN values when parsing the data.
        credentials: [dict] SharePoint Online site credentials (from Azure Application) as a python dictionary. Must include keys client_id and client_secret. Default credentials=None uses Operational Analytics credentials.
        last_upload_time: [datetime] Date when file was last uploaded to Database. If present, loadExcelFile will check the last modified datetime from a file prior to loading then if last_refresh >= last_modified, the file load is skipped.

    Returns:
        [list of pandas DataFrames] List of Excel sheets loaded as dataframe.
    """
    result = pd.DataFrame()
    load_file = True

    if 'sharepoint.com' not in file_path:  # file assumed to be on local drive, shared drive, or VM share
        if last_upload_time:
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))  # os.path.getmtime returns a timestamp
            # print(mod_time)
            if mod_time.replace(tzinfo=None) <= last_upload_time.replace(tzinfo=None):  # check if file was modified since the last time it was loaded
                load_file = False
        if load_file:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter(action='ignore', category=UserWarning)  # ignores UserWarning about Data Validation rules in the Excel Workbook (which is not supported by openpyxl - the default engine in the pandas read_excel function)
                    result = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, na_values=na_values, keep_default_na=keep_default_na)  # by default sheet_name=None loads all sheets
            except ValueError:
                if sheet_name:
                    print('No sheet found matching name: {}.'.format(sheet_name))
                return result
        # print("Column headings: {0}".format(df.columns))  # print column headers
    else:  # sharepoint online site URL provided
        file_path = file_path.replace(':x:/r/', '')  # remove SharePoint Online extension from URL
        file_path = file_path.split('?')[0]  # remove anything after file name from path

        temp = file_path.split('/')
        if temp[3] == 'sites':  # fourth section of URL is "sites"
            sp_site = '/'.join(temp[:5])
            relative_url = '/' + '/'.join(temp[3:])
            if '/_layouts/15/' in relative_url:
                raise ValueError('Invalid SharePoint URL. You need to include the relative path to the file.')
        else:
            raise ValueError('Invalid SharePoint URL')

        if credentials is None:
            credentials = {'client_id': accounts['SharePoint'].client_id,
                           'client_secret': accounts['SharePoint'].client_secret}

        # Connect to SharePoint Online
        try:
            client_credentials = ClientCredential(credentials['client_id'], credentials['client_secret'])
            ctx = ClientContext(sp_site).with_credentials(client_credentials)
        except KeyError:
            print('Error: Unable to parse client_id or client_secret keys from credentials argument.')
            return result

        if last_upload_time:
            ctx2 = ClientContext(sp_site).with_credentials(client_credentials)
            target_file = ctx2.web.get_file_by_server_relative_url(relative_url)
            ctx2.load(target_file)
            ctx2.execute_query()

            mod_time = datetime.strptime(target_file.properties['TimeLastModified'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).astimezone(tz=None)  # load datetime from List properties as UTC time
            # print(mod_time)
            if mod_time.replace(tzinfo=None) <= last_upload_time.replace(tzinfo=None):  # check if file was modified since the last time it was loaded
                load_file = False

        if load_file:
            # Get Excel File by Relative URL
            web = ctx.web
            ctx.load(web)
            try:
                ctx.execute_query()
            except ValueError as error:
                error_msg = error.args[0]
                if 'unauthorized_client' in error_msg:
                    print('Error: Unauthorized client. Invalid client id provided in credentials argument.')
                elif 'invalid_client' in error_msg:
                    print('Error: Invalid client secret provided in credentials argument.')
                else:
                    print(error)
                return result
            except ClientRequestException as error:
                print(error)
                error_msg = error.args[1]
                print("Error: {}".format(error_msg))
                return result
            except (requests.exceptions.SSLError, SSLEOFError, requests.exceptions.HTTPError) as error:
                print(error)
                return result
            except requests.exceptions.ProxyError:
                print('ProxyError when you tried to access the SharePoint site.')
                return result

            response = File.open_binary(ctx, relative_url)

            # save data to BytesIO stream
            bytes_file_obj = io.BytesIO()
            bytes_file_obj.write(response.content)
            bytes_file_obj.seek(0)  # set file object to start

            # load Excel file from BytesIO stream
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter(action='ignore', category=UserWarning)  # ignores UserWarning about Data Validation rules in the Excel Workbook (which is not supported by openpyxl - the default engine in the pandas read_excel function)
                    result = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, header=header_row, na_values=na_values, keep_default_na=keep_default_na)
            except ValueError as error:
                print(error)
                return result
            
    return result


def loadSharePointList(sp_site: str, list_name: str, credentials: dict = None, decode_column_names: bool = True, remove_metadata: bool = True, last_upload_time: datetime = None) -> pd.DataFrame:
    """Function to load SharePoint List into Pandas DataFrame.

    Args:
        sp_site: [str] Full path to SharePoint Online site.
        list_name: [str] Name of the SharePoint List to load.
        credentials: [dict] SharePoint Online site credentials (pulled from appregnew.aspx) as a python dictionary. Must include keys client_id and client_secret. Default credentials=None uses Operational Analytics credentials.
        decode_column_names: [bool] Decode the URL Encodings in the column system name?
        remove_metadata: [bool] Remove the metadata columns that are present in the List?
        last_upload_time: [datetime] Date when SP List was last uploaded to Database. If present, loadSharePointList will check the last modified datetime from a List prior to loading then if last_refresh >= last_modified, the List load is skipped.

    Returns:
        [pandas DataFrame] SharePoint List loaded as dataframe.
    """
    result = pd.DataFrame()
    load_file = True

    if credentials is None:
        credentials = {'client_id': accounts['SharePoint'].client_id, 'client_secret': accounts['SharePoint'].client_secret}

    # Connect to SharePoint Online
    try:
        client_credentials = ClientCredential(credentials['client_id'], credentials['client_secret'])
        ctx = ClientContext(sp_site).with_credentials(client_credentials)
    except KeyError:
        print('Error: Unable to parse client_id or client_secret keys from credentials argument.')
        return result

    # Get List by Title
    list_object = ctx.web.lists.get_by_title(list_name)  # List object - reference: https://github.com/vgrem/Office365-REST-Python-Client/blob/master/office365/sharepoint/lists/list.py
    # list_object = ctx.web.lists.get_by_id(list_id)

    if last_upload_time:
        ctx.load(list_object)
        try:
            ctx.execute_query()
        except ValueError as error:
            error_msg = error.args[0]
            if 'unauthorized_client' in error_msg:
                print('Error: Unauthorized client. Invalid client id provided in credentials argument.')
            elif 'invalid_client' in error_msg:
                print('Error: Invalid client secret provided in credentials argument.')
            else:
                print(error)
            return result
        except ClientRequestException as error:
            print(error)
            error_msg = error.args[1]
            print("Error: {}".format(error_msg))
            return result

        mod_time = datetime.strptime(list_object.properties['LastItemUserModifiedDate'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).astimezone(tz=None)  # load datetime from List properties as UTC time then convert to local timezone for comparison
        # print(mod_time)
        if mod_time.replace(tzinfo=None) <= last_upload_time.replace(tzinfo=None):  # check if file was modified since the last time it was loaded
            load_file = False

    if load_file:
        list_items = list_object.get_items()
        ctx.load(list_items)
        try:
            ctx.execute_query()
        except ValueError as error:
            error_msg = error.args[0]
            if 'unauthorized_client' in error_msg:
                print('Error: Unauthorized client. Invalid client id provided in credentials argument.')
            elif 'invalid_client' in error_msg:
                print('Error: Invalid client secret provided in credentials argument.')
            else:
                print(error)
            return result
        except ClientRequestException as error:
            print(error)
            error_msg = error.args[1]
            print("Error: {}".format(error_msg))
            return result

        # Determine if any duplicate columns exist in the SharePoint List (case when the column is converted from a User Object in the API call)
        duplicate_columns = list()
        id_columns = set()
        set_length = 0  # current size of set
        for key in list_items[0].properties.keys():
            if key.endswith('Id'):
                if key.endswith('StringId'):  # suffix is StringId
                    temp = key[:-8]
                else:  # suffix is just Id
                    temp = key[:-2]
                # print(key)
                id_columns.add(temp)
                if len(id_columns) == set_length:
                    duplicate_columns.append(key)
                else:
                    set_length += 1
        user_columns = [col[:-8] + 'Id' if col.endswith('StringId') else col for col in duplicate_columns]  # Change "StringId" to "Id"

        if len(user_columns) > 0:
            user_dict = dict()
            user_object = ctx.web.site_users.get()  # UserCollection object - reference: https://github.com/vgrem/Office365-REST-Python-Client/blob/master/office365/sharepoint/principal/user_collection.py
            ctx.load(user_object)
            try:
                ctx.execute_query()
            except ValueError as error:
                error_msg = error.args[0]
                if 'unauthorized_client' in error_msg:
                    print('Error: Unauthorized client. Invalid client id provided in credentials argument.')
                elif 'invalid_client' in error_msg:
                    print('Error: Invalid client secret provided in credentials argument.')
                else:
                    print(error)
                return result
            except ClientRequestException as error:
                print(error)
                error_msg = error.args[1]
                print("Error: {}".format(error_msg))
                return result

            for user in user_object:
                # print(user.get_property('title'))
                user_dict[user.get_property('id')] = user.get_property('title')

            user_columns.extend(['AuthorId', 'EditorId'])  # Manually mark Author and Editor as additional User Object fields

            parsed_items = list()
            for item in list_items:
                row = dict()
                # print(item.properties)  # Before changes
                for key, value in item.properties.items():
                    if key in duplicate_columns:  # Ignore columns that are marked as duplicate
                        continue
                    elif key in user_columns:
                        if key.endswith('Id'):
                            new_key = key[:-2]
                        else:
                            new_key = key
                        # print(new_key)
                        # print(value)
                        try:
                            if isinstance(value, dict):
                                user_ids = list(value.values())
                                user_id = user_ids[0]
                                row[new_key] = user_dict[user_id]
                                for i in range(len(user_ids) - 1):
                                    row[new_key] += '; ' + user_dict[user_ids[i + 1]]
                            else:
                                row[new_key] = user_dict[value]
                        except KeyError:  # Case when the user object for this cell is blank (null)
                            row[new_key] = None
                    else:
                        row[key] = value
                # print(row)  # After changes
                parsed_items.append(row)
        else:
            parsed_items = [item.properties for item in list_items]
        df = pd.DataFrame.from_records(parsed_items)

        if remove_metadata:
            metadata_columns = ['FileSystemObjectType', 'Id', 'ServerRedirectedEmbedUri', 'ServerRedirectedEmbedUrl', 'ID',
                                'ContentTypeId', 'Modified', 'Created', 'Author', 'AuthorId', 'Editor', 'EditorId',
                                'OData__UIVersionString', 'Attachments', 'GUID', 'ComplianceAssetId']
            df = df.drop(metadata_columns, axis=1, errors='ignore')

        if decode_column_names:
            for col in df.columns:
                col_renamed = decode(re.sub(r'_x00(.?.)_', r'\\x\1', col), 'unicode_escape')  # use codecs to decode Unicode encoded values (i.e. _x0020 as a space) in column names
                temp = col_renamed.find('_x')  # if there is left over Unicode at the end of the column name, remove it
                if temp > -1 and temp >= len(col_renamed) - 6:
                    col_renamed = col_renamed[:temp]
                df.rename(columns={col: col_renamed}, inplace=True)

        result = df

    return result


# def loadSharePointFolderContents(sp_site: str, relative_folder_path: str, credentials: dict = None) -> list:
#     """Function to load all files from a SharePoint Online Folder.
#
#     Args:
#         sp_site: [str] Full path to SharePoint Online site.
#         relative_folder_path: [str] Relative path to folder in SharePoint Online site.
#         credentials: [dict] SharePoint Online site credentials (pulled from appregnew.aspx) as a python dictionary. Must include keys client_id and client_secret. Default credentials=None uses Operational Analytics credentials.
#
#     Returns:
#         [list of sharepoint.file.file.File objects] List of files in the SharePoint Online folder.
#     """
#     if credentials is None:
#         credentials = {'client_id': accounts['SharePoint'].client_id, 'client_secret': accounts['SharePoint'].client_secret}
#
#     # Connect to SharePoint Online
#     try:
#         client_credentials = ClientCredential(credentials['client_id'], credentials['client_secret'])
#         ctx = ClientContext(sp_site).with_credentials(client_credentials)
#     except KeyError:
#         print('Error: Unable to parse client_id or client_secret keys from credentials argument.')
#         return None
#
#     # Load folder information using relative path information
#     folder = ctx.web.get_folder_by_server_relative_path(relative_folder_path)
#     ctx.load(folder)
#     ctx.execute_query()
#
#     # Load all files in the folder
#     files = folder.files
#     ctx.load(files)
#     ctx.execute_query()
#
#     return files


def readSmartsheet(sheet_name: str, access_token: str = accounts['Smartsheet'].password, doc_type: str = 'Sheet', page_size: int = 100, last_upload_time: datetime = None) -> pd.DataFrame:
    """Function to read Smartsheet into Pandas DataFrame.

    Args:
        sheet_name: [str] Name of the Smartsheet to load.
        access_token: [str] Access Token from Smartsheet website for a given user id. Default uses sys_SCdata account access token.
        doc_type: [str] Which document type you want to access on Smartsheet. Either "Sheet" or "Summary Report".
        page_size: [int] Used for API pagination. The maximum number of items to return per page. The largest page_size supported as of May 2021 is 2500.
        last_upload_time: [datetime] Date when Smartsheet Sheet was last uploaded to database. If present, readSmartsheet will check the last modified datetime from a Sheet prior to loading then if last_refresh >= last_modified, the Sheet load is skipped.

    Returns:
        [pandas DataFrame] Smartsheet loaded as dataframe.
    """
    result = pd.DataFrame()

    # Initialize client
    smartsheet_client = smartsheet.Smartsheet(access_token)
    # print(smartsheet_client)

    # Check for any errors
    smartsheet_client.errors_as_exceptions(True)

    if doc_type == "Sheet":
        # Get Sheet Id using name
        sheet_id = None
        if last_upload_time:
            index_result = smartsheet_client.Sheets.list_sheets(include_all=True, modified_since=last_upload_time)
        else:
            index_result = smartsheet_client.Sheets.list_sheets(include_all=True)
        sheets = index_result.data
        for sheet_info in sheets:  # type: smartsheet.Smartsheet.models.sheet.Sheet
            # print(sheet_info)
            if sheet_info.name == sheet_name:
                sheet_id = sheet_info.id

        if not sheet_id:
            if last_upload_time:  # If no file was found when using the last_upload_time parameter, check if file exists at all
                index_result = smartsheet_client.Sheets.list_sheets(include_all=True)
                sheets = index_result.data
                for sheet_info in sheets:  # type: smartsheet.Smartsheet.models.sheet.Sheet
                    if sheet_info.name == sheet_name:
                        sheet_id = sheet_info.id
                if sheet_id:
                    print('Skipped {0} load as it has not been modified since the last upload.'.format(sheet_name))
                    return result
            raise FileNotFoundError("No Smartsheet Sheet named '{}' for ACCESS_TOKEN specified".format(sheet_name))
        else:
            sheets = list()
            total_rows = 100  # initialize this as anything greater than 0 to start the While loop
            i = 1
            while (i - 1) * page_size < total_rows:  # iterate once more than the number of total rows / page size
                sheet = smartsheet_client.Sheets.get_sheet(sheet_id, page_size=page_size, page=i, exclude='nonexistentCells')  # for get_sheet() documentation see: https://smartsheet-platform.github.io/api-docs/#get-sheet
                sheets.append(sheet)
                if i == 1:  # update the number of total rows based on the value in the first returned page
                    total_rows = sheet.total_row_count
                i += 1  # increment page variable
            # print("Loaded Sheet: " + sheet.name)
    
    elif "Summary" in doc_type:
        # Get Report Id using Report name
        report_id = None
        # if last_upload_time:
        #     index_result = smartsheet_client.Reports.list_reports(include_all=True, modified_since=last_upload_time)  # TODO: Fix this as it doesn't seem to accurately track modified date within the report
        # else:
        index_result = smartsheet_client.Reports.list_reports(include_all=True)  # type: smartsheet.models.index_result.IndexResult
        reports = index_result.data
        for report_info in reports:  # type: smartsheet.models.report.Report
            if report_info.name == sheet_name:
                report_id = report_info.id
                # print(report_info.total_row_count)  # can only view if you have ADMIN access Level

        if not report_id:
            # if last_upload_time:  # If no file was found when using the last_upload_time parameter, check if file exists at all
            #     index_result = smartsheet_client.Reports.list_reports(include_all=True)
            #     reports = index_result.data
            #     for report_info in reports:
            #         if report_info.name == sheet_name:
            #             report_id = report_info.id
            #     if report_id:
            #         print('Skipped {0} load as it has not been modified since the last upload.'.format(sheet_name))
            #         return result
            raise FileNotFoundError("No Smartsheet Summary Report named '{}' for ACCESS_TOKEN specified".format(sheet_name))
        else:
            sheets = list()
            total_rows = 100  # initialize this as anything greater than 0 to start the While loop
            i = 1
            while (i - 1) * page_size < total_rows:  # iterate once more than the number of total rows / page size
                sheet = smartsheet_client.Reports.get_report(report_id, page_size=page_size, page=i, level=2)  # for get_report() documentation see: https://github.com/smartsheet-platform/smartsheet-python-sdk/blob/master/smartsheet/reports.py#L56
                sheets.append(sheet)
                if i == 1:  # update the number of total rows based on the value in the first returned page
                    total_rows = sheet.total_row_count
                i += 1  # increment page variable
            # smartsheet_client.Reports.get_report_as_excel(report_id, download_path=r"C:\Users\davismat\OneDrive - Intel Corporation\Downloads")
    else:
        raise ValueError('Invalid argument "{}" passed as the doc_type parameter. Valid options are "Sheet" or "Summary".'.format(doc_type))

    # Format result as a DataFrame
    col_names = [col.title for col in sheets[0].columns]
    rows = list()
    for sheet in sheets:
        for row in sheet.rows:  # type: smartsheet.Smartsheet.models.row.Row
            cells = list()
            for cell in row.cells:  # type: smartsheet.Smartsheet.models.cell.Cell or smartsheet.Smartsheet.models.report_cell.ReportCell
                # print(cell.value)
                cells.append(cell.value)
            rows.append(cells)
    df = pd.DataFrame(rows, columns=col_names)

    return df


def uploadDFtoSQL(table: str, data: pd.DataFrame, columns: list = None, categorical: list = None, truncate: bool = True,
                  chunk_size: int = 10000, driver: str = None, server: str = None,
                  database: str = None, username: str = None, password: str = None) -> list:
    """Function to insert Pandas DataFrame into SQL Server database.

    Args:
        table: Name of the table within the database.
        data: [pandas DataFrame] Data to be uploaded.
        columns: [list of str] Ordered list of columns within table.
        categorical: [list of str] Columns to convert from numeric to text (indicating they are categorical i.e. supplier id)
        truncate: [bool] Truncate table prior to loading?
        chunk_size: [int] How large of chunks you would like to use during inserts
        driver: [str] Which SQL driver to use. Default from Project_params file.
        server: [str] Which server to connect to. Default from Project_params file.
        database: [str] Which database to connect to. Default from Project_params file.
        username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
        password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

    Returns:
        [two element list] [boolean] If the insert statement executed properly. True for success, False otherwise. [str] Error message.
        
    """
    if chunk_size == 10000 and data.shape[0] <= 10000:  # do not chunk when DataFrame is less than or equal to 10000 rows
        chunk_size = 0

    success_bool = False
    error_msg = None
    column_info = dict()
    column_list = list()

    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    if any(data.columns.duplicated()):  # Duplicate column name in DataFrame
        error_msg = "Duplicate column name in the Pandas DataFrame. This function will not attempt to load data now because of column mapping issue."
        return [success_bool, error_msg]
    elif columns is not None:
        if len(columns) != data.shape[1]:  # columns entered the user are not equal to the number of columns in the DataFrame
            error_msg = "[SQL Server]Unable to insert values into to table {0} in database {1}. Number of columns provided by the user does not match the number of columns in the Pandas DataFrame. Perhaps a column was deleted from your data source?".format(table, database)
            return [success_bool, error_msg]
        else:
            column_info = map_columns(table, df=data, display_result=False, sql_columns=columns, server=server, database=database, username=username, password=password)
            if all(x.destination_name == '%Unknown%' for x in column_info.values()):
                error_msg = "Table {0} not found in database {1}".format(table, database)
                return [success_bool, error_msg]
            elif any(x.destination_name == '%Unknown%' for x in column_info.values()):
                error_msg = "[SQL Server]Unable to insert values into to table {0} in database {1}. Unable to find one or more columns provided by the user in the SQL table. Perhaps a column name changed in the SQL database?".format(table, database)
                return [success_bool, error_msg]
            else:
                column_list = ['[' + column_info[x].destination_name + ']' for x in column_info.keys()]
    else:
      
        column_info = map_columns(table, df=data, display_result=False, server=server, database=database, username=username, password=password)  # Use function to determine column mapping information
        if all(x.destination_name == '%Unknown%' for x in column_info.values()):  # INFORMATION_SCHEMA did not have column information for table
            error_msg = "Table {0} not found in database {1}".format(table, database)
            return [success_bool, error_msg]
        elif any(x.source_name == '%Unknown%' for x in column_info.values()):
            error_msg = "[SQL Server]Unable to insert values into to table {0} in database {1}. There are more columns in the SQL table than are available in the Pandas DataFrame. Perhaps a column was deleted from your data source?".format(table, database)
            return [success_bool, error_msg]
        elif sum(x.destination_name == '%Unknown%' for x in column_info.values()) > 0:  # At least one of the columns does not map correctly
            error_msg = "[SQL Server]Unable to insert values into to table {0} in database {1}. There are more columns in the Pandas DataFrame than are available in the SQL table. Perhaps a column was added to your data source?".format(table, database)
            return [success_bool, error_msg]
        else:
            column_list = ['[' + column_info[x].destination_name + ']' for x in column_info.keys()]  # Add all column_names in table a list
     
    # CONNECT -- to GSMDW Server and Database using SQL Server driver
    if username is not None and password is not None:
        conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password, autocommit=False)  # Connect via pyodbc using SQL Server authentication
    else:
        conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes', autocommit=False)  # Connect via pyodbc using keywords for Windows authentication
    cursor = conn.cursor()

    # If user specified to truncate Table prior to loading new data
    if truncate:
        if server in ('sql3266-fm1-in.amr.corp.intel.com,3181', 'sql2652-fm1s-in.amr.corp.intel.com,3181') and database == 'gscdw':  # only for new Supply Chain BI Production environment
            execute_statement = "SET NOCOUNT ON;\nDECLARE @ret int\nEXEC @ret = ETL.spTruncateTable '{}'\nSELECT @ret;".format(table)  # use Stored Procedure to truncate table
        else:
            execute_statement = "TRUNCATE TABLE " + table + ";"

        print("TRUNCATE TABLE " + table + ";")
        try:
            if execute_statement.startswith('TRUNCATE'):  # Normal truncation
                cursor.execute(execute_statement)
            else:  # Stored Procedure truncation
                sp_success_bool = not bool(cursor.execute(execute_statement).fetchone()[0])  # Zero return values in SQL Server indicate success
                if not sp_success_bool:  # Stored Procedure failed to return success message
                    conn.rollback()
                    error_msg = "Table " + table + " could not be truncated. Check the spelling of your table name and ensure it is present on the database you are connection to. This function will not attempt to load data now because of possible duplication."
                    return [sp_success_bool, error_msg]
        except pyodbc.ProgrammingError:
            conn.rollback()  # rollback to previous database state
            success_bool = False
            error_msg = "Table " + table + " could not be truncated. Check the spelling of your table name and ensure it is present on the database you are connection to. This function will not attempt to load data now because of possible duplication."
            return [success_bool, error_msg]

    if column_list:  # at least one columns specified for insert
        # Format SQL Server insert statements
        schema_string = '(' + ', '.join(column_list) + ')'
        values_string = '(?'
        for _ in range(len(column_list) - 1):
            values_string += ', ?'
        values_string += ')'

        insert_statement = "INSERT INTO " + table + " " + schema_string + " VALUES " + values_string + ";"
        print(insert_statement)

        # format DataFrame values for SQL
        data.replace(r'^\s*$', np.nan, regex=True, inplace=True)  # replace field that's entirely spaces (or empty) with NaN
        if categorical:
            for col in categorical:
                if data[col].dtype == np.float64 or data[col].dtype == np.int64:
                    data[col] = data[col].map(lambda x: '{:.0f}'.format(x))  # avoid pandas automatically using scientific notation for large numbers
                    data[col].replace('nan', np.nan, inplace=True)  # when line above converts float to string it incorrectly converts nulls
        data.replace({pd.NaT: None, np.nan: None, 'NaT': None, 'nan': None}, inplace=True)  # convert pandas NaT (not a time) and numpy NaN (not a number) values to python None type which is equivalent to SQL NULL

        insert_params = list(data.itertuples(index=False, name='Pandas'))
        # print(insert_params)

        # LOAD -- executemany using pyodbc
        cursor.fast_executemany = True
        try:
            if chunk_size <= 0:
                cursor.executemany(insert_statement, insert_params)
            else:
                for chunk in chunks(insert_params, n=chunk_size):
                    cursor.executemany(insert_statement, chunk)
            conn.commit()   # commit in the database after all insert statements
            success_bool = True
        except pyodbc.Error as error:
            # print(error)
            conn.rollback()  # rollback to previous database state
            success_bool = False
            if 'String data, right truncation' in error.args[0] and column_info:
                # Determine which column could cause the offending error
                error_column = next(iter(column_info))  # placeholder as the first column from the dictionary
                for col in column_info.keys():
                    temp = column_info[col].destination_length
                    if not isinstance(temp, int):  # nvarchar(max) case
                        temp = 10000
                    # print('First value: {}'.format(temp))
                    # print('Second value: {}'.format(column_info[col].source_length))
                    if temp < column_info[col].source_length:
                        error_column = col

                # Determine column length of the offending column
                if 'length' in error.args[0]:
                    length = int(error.args[0].split(' ')[5])  # parse length from error message
                    if column_info[error_column].destination_dtype == 'nvarchar':
                        length = int(length / 2)  # divide by 2 if nvarchar
                else:
                    length = '???'

                # Determine suggested length from buffer message
                length_hint = int(error.args[0].replace("'", '').replace(',', '').split(' ')[7]) / 2

                if column_info[error_column].destination_length > length_hint:
                    error_msg = "Error: Attempting to fit text value into numeric data type.\nOriginal error: {0}".format(error)
                else:
                    error_msg = "Error: Unable to insert data into table \"{0}\" due to possible data truncation. " \
                                "Attempted to insert {2} value of length {1} into column \"{4}\" " \
                                "which is the type {5}({6}).\nOriginal error: {3}".format(table, length, 'text', error,
                                                                                          column_info[error_column].destination_name,
                                                                                          column_info[error_column].destination_dtype,
                                                                                          str(column_info[error_column].destination_length)
                                                                                          )
            else:
                error_msg = error
        except MemoryError:
            conn.rollback()  # rollback to previous database state
            success_bool = False
            if chunk_size <= 0:
                error_msg = "MemoryError. Please add chunking to reduce the data load size."
            else:
                error_msg = "MemoryError. Data was too large to load into SQL Sever Database using current chunk_size = {0}.".format(chunk_size)

        ###  BEGIN TODO: Attempt to restructure upload to reduce length of time that Python is connected to database. ###
        # # Use sqlalchemy to connect to database instead of pyodbc
        # connection_string = "DRIVER={driver};\
        #                       SERVER={server};\
        #                       DATABASE={database};\
        #                       Trusted_Connection=yes;\
        #                       integrated security=true".format(driver=driver, server=server, database=database)
        # engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus(connection_string)), echo=False)

        # # Enable executemany functionality for faster insert with pandas to_sql
        # @sqlalchemy.event.listens_for(engine, "before_cursor_execute")
        # def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        #     if executemany:
        #         cursor.fast_executemany = True

        # # Write data to sql using the specified chunk_size
        # data.to_sql(table, con=engine, if_exists='append', index=False, method='multi', chunksize=chunk_size)
        ### END TODO ###

    conn.close()

    return [success_bool, error_msg]


def querySQL(statement: str, driver: str = None, server: str = None, database: str = None, username: str = None, password: str = None) -> list:
    """Function to executeSQL on SQL Server database.

        Args:
            statement: [str] SQL statement to execute to return a table.
            driver: [str] Which SQL driver to use. Default from Project_params file.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns:
            [three element list] [boolean] If the query statement executed properly. True for success, False otherwise. [pandas DataFrame] Query result. [str] Error message.

        """
    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    if username is not None and password is not None:
        conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password)  # Connect via pyodbc using SQL Server authentication
    else:
        conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes')  # Connect via pyodbc using keywords for Windows authentication

    df = pd.DataFrame()

    try:
        df = pd.read_sql(statement, conn)
        success_bool = True
        error_msg = None
    except pyodbc.Error as error:
        # print(error)
        success_bool = False
        error_msg = error

    conn.close()

    return [success_bool, df, error_msg]


def executeStoredProcedure(procedure_name: str, *parameters, driver: str = None, server: str = None, database: str = None, username: str = None, password: str = None):
    """Function to execute stored procedure on SQL Server database.

        Args:
            procedure_name: [str] Name of the stored procedure within the database.
            parameters: [list of string] Any parameters for the stored procedure.
            driver: [str] Which SQL driver to use. Default from Project_params file.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns:
            [two element list] [boolean] If the insert statement executed properly. True for success, False otherwise. [str] Error message.

    """
    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    # CONNECT -- to GSMDW Server and Database using SQL Server driver
    if username is not None and password is not None:
        conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password, autocommit=True)  # Connect via pyodbc using SQL Server authentication
    else:
        conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes', autocommit=True)  # Connect via pyodbc using keywords for Windows authentication
    cursor = conn.cursor()

    execute_statement = """SET NOCOUNT ON;\nDECLARE @ret int\nEXEC @ret = {0} {1}\nSELECT @ret""".format(procedure_name, ','.join(['?'] * len(parameters)))
    print("""EXEC {0} {1}""".format(procedure_name, parameters))

    try:
        success_bool = not bool(cursor.execute(execute_statement, parameters).fetchone()[0])  # Zero return values in SQL Server indicate success
        error_msg = None
    except pyodbc.Error as error:
        # print(error)
        conn.rollback()  # rollback to previous database state
        success_bool = False
        error_msg = error

    conn.close()

    # print(success_bool)
    return [success_bool, error_msg]


def executeSQL(statement: str, driver: str = None, server: str = None, database: str = None, username: str = None, password: str = None):
    """Function to executeSQL on SQL Server database.

        Args:
            statement: [str] SQL statement to execute.
            driver: [str] Which SQL driver to use. Default from Project_params file.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns:
            [two element list] [boolean] If the statement executed properly. True for success, False otherwise. [str] Error message.

        """
    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    if username is not None and password is not None:
        conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password, autocommit=True)  # Connect via pyodbc using SQL Server authentication
    else:
        conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes', autocommit=True)  # Connect via pyodbc using keywords for Windows authentication
    cursor = conn.cursor()

    print(statement)

    try:
        cursor.execute(statement)  # autocommit = True was specified so this will automatically show on the database
        success_bool = True
        error_msg = None
    except pyodbc.Error as error:
        # print(error)
        conn.rollback()  # rollback to previous database state
        success_bool = False
        error_msg = error

    conn.close()

    return [success_bool, error_msg]


def getSQLCursorResult(statement: str, driver: str = None, server: str = None, database: str = None, username: str = None, password: str = None):
    """Function to executeSQL on SQL Server database.

        Args:
            statement: [str] SQL statement to execute.
            driver: [str] Which SQL driver to use. Default from Project_params file.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns:
            [three element list] [boolean] If the insert statement executed properly. True for success, False otherwise. [str] Query result. [str] Error message.

        """
    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    if username is not None and password is not None:
        conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password, autocommit=True)  # Connect via pyodbc using SQL Server authentication
    else:
        conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes', autocommit=True)  # Connect via pyodbc using keywords for Windows authentication
    cursor = conn.cursor()

    # print(statement)

    try:
        result = cursor.execute(statement).fetchall()
        success_bool = True
        error_msg = None
    except pyodbc.Error as error:
        # print(error)
        result = None
        success_bool = False
        error_msg = error

    conn.close()

    return [success_bool, result, error_msg]


def truncate_table(table: str, driver: str = None, server: str = None, database: str = None, username: str = None, password: str = None) -> list:
    """Function to executeSQL on SQL Server database.

        Args:
            table: [str] Which table to truncate in SQL.
            driver: [str] Which SQL driver to use. Default from Project_params file.
            server: [str] Which server to connect to. Default from Project_params file.
            database: [str] Which database to connect to. Default from Project_params file.
            username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
            password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

        Returns:
            [two element list] [boolean] If the truncate executed properly. True for success, False otherwise. [str] Error message.

        """
    if driver is None:
        driver = params['SQL_DRIVER']
    if server is None:
        server = params['GSMDW_SERVER']
    if database is None:
        database = params['GSMDW_DB']

    success_bool = False
    error_msg = None

    if ';' in table:
        error_msg = "Possible SQL Injection attack detected. Please provide only a table name as the argument to the function."
    else:
        # CONNECT -- to Server and Database using SQL Server driver specified
        if username is not None and password is not None:
            conn = pyodbc.connect(driver=driver, server=server, database=database, user=username, password=password) # Connect via pyodbc using SQL Server authentication
        else:
            conn = pyodbc.connect(driver=driver, server=server, database=database, Trusted_Connection='yes')  # Connect via pyodbc using keywords for Windows authentication
        cursor = conn.cursor()

        try:
            cursor.execute("TRUNCATE TABLE " + table + ";")
            print("TRUNCATE TABLE " + table + ";")
            conn.commit()
            success_bool = True
        except pyodbc.ProgrammingError:
            conn.rollback()  # rollback to previous database state
            success_bool = False
            error_msg = "Table " + table + " could not be truncated. Check the spelling of your table name and ensure it is present on the database you are connection to. This function will not attempt to load data now because of possible duplication."
        finally:
            conn.close()

    return [success_bool, error_msg]


def queryHANA(statement: str, environment: str = 'Production', credentials: dict = None, single_sign_on: bool = False) -> pd.DataFrame:
    """A function to write a sql query against an SAP HANA server and return a DataFrame.

        Args:
            statement: [str] Working SELECT query from HANA Studio tool
            environment: [str] The key for the server/port you want query against.
            credentials: [dict] HANA DB Account credentials as a python dictionary. Must include keys username and password. Default credentials=None uses SYSH_SCES_OPSRPT credentials.
            single_sign_on: [boolean] If the user wants to use Windows credentials as Single Sign-on instead of HANA DB Account. True for Windows, False for HANA DB account.

        Returns:
            [pandas DataFrame] A DataFrame of the query results.

        """
    servers = {
        'Production' : {'server': 'sapehpdb.intel.com', 'port': '31015'},
        'Pre-DEV' : {'server': 'sapeh1db.intel.com', 'port': '33715'},
        'QA/CONS' : {'server': 'sapehcdb.intel.com', 'port': '31215'},
        'Development' : {'server': 'sapnbidb.intel.com', 'port': '31115'},
        'Benchmark' : {'server': 'sapehbdb.intel.com', 'port': '31015'},
        'Production Support' : {'server': 'sapehsdb.intel.com', 'port': '31315'},
    }

    if single_sign_on:
        conn = dbapi.connect(
            address=servers[environment]['server'],
            port=servers[environment]['port']
        )  # if user/password argument is not present, use Windows Single Sign-on as credentials
    else:
        if credentials is None:
            credentials = {'username': accounts['HANA'].username, 'password': accounts['HANA'].password}

        conn = dbapi.connect(
            address=servers[environment]['server'],
            port=servers[environment]['port'],
            user=credentials['username'],
            password=credentials['password']
        )

    cursor = conn.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()
    columns = [x[0] for x in cursor.description]
    cursor.close()
    conn.close()

    df = pd.DataFrame(result, columns=columns)
    return df


def queryTeradata(statement: str, server: str = 'TDPRD1.intel.com', credentials: dict = None) -> pd.DataFrame:
    """Function to query against Teradata databases

        Args:
            statement: [str] T-SQL statement to execute to return a table.
            server: [str] Which server to connect to. Default EDW Production server.
            credentials: [dict] Teradata DB Account credentials as a python dictionary. Must include keys username and password. Default credentials=None uses APPL_GSM_BI_01 credentials.

        Returns:
            [pandas DataFrame] A DataFrame of the query results.

        """
    if credentials is None:
        credentials = {'username': accounts['Teradata'].username, 'password': accounts['Teradata'].password}

    # Determine which Teradata Driver to use from list of Drivers installed on the system
    driver = ''
    for x in pyodbc.drivers():
        if 'Teradata' in x:
            if not driver:  # if no Teradata driver has been identified yet, set the driver as the match
                driver = x
            elif driver.split(' ')[-1] < x.split(' ')[-1]:  # if there is already a Teradata driver identified, check which is newer and use that
                driver = x

    # Establish connection to Teradata server
    conn = pyodbc.connect('DRIVER={0};DBCNAME={1};UID={2};PWD={3}'.format(driver, server, credentials['username'], credentials['password']))

    df = pd.read_sql(statement, conn)
    return df


def querySalesforce(statement: str, credentials: dict = None) -> pd.DataFrame:
    """Function to query against Salesforce database

        Args:
            statement: [str] Salesforce Object Query Language (SOQL) statement to execute to return a table.
            credentials: [dict] Salesforce Account credentials as a python dictionary. Must include keys username, password, and security_token. Default credentials=None uses icappublicwebsite@intel.com.icapprod credentials.

        Returns:
            [pandas DataFrame] A DataFrame of the query results.
    """
    result = pd.DataFrame()

    if credentials is None:
        credentials = {'username': accounts['Salesforce'].username, 'password': accounts['Salesforce'].password,
                       'security_token': decrypt_password(b'gAAAAABiIQIGlWofj37gej0v6qd0z14FjGwbsxlNLVDSkjTtYETsHg57f87oQ9pLWge26-q98dQ4kT-VBxgFrmK8Vm2qU0kCls7Kw_Gsliy0XKYqV6b2NGA=')
                       }

    try:
        sf = Salesforce(username=credentials['username'], password=credentials['password'], security_token=credentials['security_token'])  # connect to salesforce feed
    except KeyError:
        print('Error: Unable to parse username, password, or security_token from credentials argument.')
        return result
    except SalesforceAuthenticationFailed as error:
        print(error.message)
        return result

    try:
        sf_data = sf.query_all(statement)
    except (SalesforceMalformedRequest, SalesforceResourceNotFound):
        print('Error: Salesforce query failed.')
        return result

    result = pd.DataFrame.from_dict(sf_data['records'])
    return result


def querySSAS(statement: str, server: str, model: str) -> pd.DataFrame:
    """Function to query against SQL Server Analysis Service Tabular models

        Args:
            statement: [str] DAX statement to execute to return a table. Must begin with EVALUATE keyword.
            server: [str] Which server to connect to.
            model: [str] The name of the Tabular model to connect to.

        Returns:
            [pandas DataFrame] A DataFrame of the query results.
    """
    result = pd.DataFrame()

    # Determine location of AdomdClient.dll resource
    adomd_path = ''
    for directory in [r'C:\Program Files\Microsoft.Net\ADOMD.NET',  # primary path
                      r'C:\Program Files (x86)\Microsoft.NET\ADOMD.NET',
                      r'C:\Program Files (x86)\Microsoft Office\root\vfs\ProgramFilesX86\Microsoft.NET\ADOMD.NET']:
        if os.path.isdir(directory):  # if directory exists
            _, folders, _ = next(os.walk(directory))  # list all folders in directory
            folders = sorted(folders, reverse=True)  # order newest version first
            for folder in folders:
                if os.path.isfile(os.path.join(directory, folder, 'Microsoft.AnalysisServices.AdomdClient.dll')):  # if AdomdClient file exists in directory/folder
                    # print('AdomdClient.dll {} file found!'.format(os.path.basename(path)))
                    adomd_path = os.path.join(directory, folder)  # if match is found set as adomd_path and break out of loop
                    break
        else:  # directory does not exist
            continue  # the continue statement moves on to the next directory without triggering the break below
        break  # if the inner loop was broken, break the outer loop as well
    if adomd_path:
        sys.path.append(adomd_path)  # add ADOMD resource to Path
    else:
        print('No SSAS Driver (AdomdClient) found. Unable to load the Pyadomd library.')
        return result

    # Attempt to import pyadomd libary
    from pyadomd import Pyadomd  # if ADOMD resource is not in Path, this import throws an error

    # Import Exception from Microsoft OLAP exceptions
    import clr
    clr.AddReference('Microsoft.AnalysisServices.AdomdClient')
    from Microsoft.AnalysisServices.AdomdClient import AdomdErrorResponseException

    # Add evaluate keyword to query statement
    if statement[:8].upper() != 'EVALUATE':  # if query does not begin EVALUATE keyword, add it.
        statement = 'EVALUATE ' + statement

    # Use Pyadomd library to read SSAS cube
    conn_str = 'Provider=MSOLAP;Data Source={0};Catalog={1}'.format(server, model)
    with Pyadomd(conn_str) as conn:
        try:
            print('Querying SSAS Cube server: {0} for model: {1}'.format(server, model))
            with conn.cursor().execute(statement) as cur:
                result = pd.DataFrame(cur.fetchone(), columns=[i.name for i in cur.description])
        except AdomdErrorResponseException as error:
            print(error)

    return result


def queryAPIPortal(url: str, client_id: str = accounts['Apigee'].client_id, client_secret: str = accounts['Apigee'].client_secret) -> pd.DataFrame:
    """A function to use a REST URL to retrieve data from an api on https://api-portal-internal.intel.com/ and return a DataFrame.
            *** Your App needs to have been granted approval to the API before you will be able to send HTTP requests

        Args:
            url: [str] A working GET URL to send an HTTP request an API, should end with '$format=JSON' in order for the function to successfully return a dataframe
            client_id: [str] The client id from the app you are using to send the HTTP request to the API, defaulted to the id from the GSM Business Intelligence App owned by SCES - OAA
            client_secret: [dict] The client secret from the app you are using to send the HTTP request to the API, defaulted to the secret from the GSM Business Intelligence App owned by SCES - OAA

        Returns:
            [pandas DataFrame] A DataFrame of the query results.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    proxyDict = {
                "http": os.environ['http_proxy'],
                "https": os.environ['https_proxy'],
            }
    token_url = "https://apis-internal.intel.com/v1/auth/token"
    test_api_url = url
    if '$format=' not in test_api_url:  # if format argument was not provided by the user
        if '?' in test_api_url:  # case when they have already entered at least 1 parameter
            test_api_url = test_api_url + '&$format=JSON'
        else:  # case when there are no parameters provided
            test_api_url = test_api_url + '?$format=JSON'

    # Step A - single call with client credentials as the basic auth header - will return access_token
    data = {'grant_type': 'client_credentials','client_id': client_id,'client_secret': client_secret}
    access_token_response = requests.post(token_url, data=data, proxies=proxyDict, verify=False, allow_redirects=False)
    # print(access_token_response.headers)
    # print(access_token_response.text)
    if access_token_response.status_code == 401:
        raise ValueError("Unable to retrieve access token from API using credentials provided.")

    tokens = json.loads(access_token_response.text)
    # print("access token: " + tokens['access_token'])

    # Step B - with the returned access_token we can make as many calls as we want
    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.get(test_api_url, headers=api_call_headers, proxies=proxyDict, verify=False)
    # print(api_call_response.text)
    # print(api_call_response)
    if api_call_response.status_code == 401:
        raise ValueError("The App used to call the API does not have access to GET data.")
    elif api_call_response.status_code == 403:
        raise ValueError("The user does not have METADATA privileges on the view.")
    elif api_call_response.status_code == 404:
        raise ValueError("Invalid Request, Path not found.")
    elif api_call_response.status_code == 502:
        raise ValueError("The body size of the HTTP request was too big. errorcode: protocol.http.TooBigBody.")
    x = api_call_response.json()

    # Loading data into DataFrame
    df = pd.DataFrame(x['elements'])
    return df


def queryOdata(url: str, username: str = None, password: str = None) -> list:
    """A function to use a REST URL to retrieve data from an Odata api and return a dictionary.

        Args:
            url: [str] A working GET URL to send an HTTP request to an API, should return a json object for the function to successfully return a dictionary
            username: [str] Username to use for HTTP Basic Authentication
            password: [str] Password to use for HTTP Basic Authentication

        Returns:
            [three element list] [boolean] If the odata query returned a value properly. True for success, False otherwise. [dict] Query result. [str] Error message.
    """
    result = None
    success_bool = False
    error_msg = None

    # Query OData feed using requests library
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    headers = {
        'accept': 'application/json'
    }
    proxies = {
        'http': 'http://proxy-chain.intel.com:912',
        'https': 'http://proxy-chain.intel.com:912'
    }
    if username is None or password is None:  # use Windows credentials to authenticate
        resp = requests.get(url, headers=headers, auth=HttpNegotiateAuth(), proxies=proxies, verify=False)
    else:  # use HTTP Basic Authentication
        resp = requests.get(url, headers=headers, auth=requests.auth.HTTPBasicAuth(username, password), proxies=proxies, verify=False)
    if resp.status_code == 200:  # success
        try:
            result = json.loads(resp.text)
            success_bool = True
        except json.decoder.JSONDecodeError as error:
            error_msg = error
    elif resp.status_code == 401:
        error_msg = 'Unauthorized. The account executing this script does not have access to the Odata feed.'
    else:
        error_msg = resp.text
        print(resp.status_code)

    # # Query OData feed using win32 authentication
    # COM_OBJ = win32com.client.Dispatch('WinHTTP.WinHTTPRequest.5.1')
    # COM_OBJ.SetAutoLogonPolicy(0)
    # COM_OBJ.SetTimeouts("300000", "300000", "300000", "300000")  # increase timeout to 5 minutes (300 seconds)
    # try:
    #     COM_OBJ.Open('GET', url, False)
    #     if username is not None and password is not None:
    #         COM_OBJ.SetCredentials(username, password, 0)
    #     COM_OBJ.Send()
    # except pythoncom.com_error as error:
    #     print(error)
    #     return [success_bool, result, error]
    # status = COM_OBJ.Status
    # if status == 200:
    #     try:
    #         result = json.loads(COM_OBJ.ResponseText)
    #         success_bool = True
    #     except json.decoder.JSONDecodeError as error:
    #         error_msg = error
    # else:
    #     error_msg = COM_OBJ.StatusText
    #     print(error_msg)

    return [success_bool, result, error_msg]
