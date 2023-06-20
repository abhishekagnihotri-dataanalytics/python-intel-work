__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SES_Data tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Hourly around the 1st minute mark"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.client_request_exception import ClientRequestException
from numpy import nan
from datetime import datetime
from time import time
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getSQLCursorResult, querySQL
from Logging import log
from Project_params import params
from Password import accounts


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def send_success_email(recipients: list, table: str, file_name: str, version_list_flag: bool):
    """Function to email support users that the job has failed.

    Args:
        recipients: [list of str] List of people to receive the email
        table: [str] Name of the SQL Server table
        file_name: [str] Name of the file that was loaded
        version_list_flag: [bool] Was the Version List file successfully updated or not?

    Returns:
        None.

    """
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')

    # Create the body of the message (a plain-text and an HTML version).
    text = """A new file "{0}" was loaded to the {1} table on the gsmdw database on sql1717-fm1-in.amr.corp.intel.com,3181. 
              To see the full list of files that have been loaded, please visit the SES SCP and IE SharePoint.
              """.format(file_name, table)
    if not version_list_flag:  # Case when version list was not successfully updated
        text = text + '\nUnable to update the VersionList.xlsx because it is open by another user and "locked for writing". Please update the VersionList manually.'

    html = """
            <html>
              <head>
                <img src="{0}" alt="Email header" width="700" height="150">
              </head>
              <body>
                <p>A new file "{1}" was loaded to the {2} table on the gsmdw database on sql1717-fm1-in.amr.corp.intel.com,3181.<br>
                  To see the full list of files that have been loaded, please visit the 
                  <a href="https://intel.sharepoint.com/sites/MSOGSEMATSSESSCPandIE/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FMSOGSEMATSSESSCPandIE%2FShared%20Documents%2FE2Open%20Data%20Download&viewid=13fac870%2D1a7d%2D4319%2D9d34%2Daf941ebde942">
                  SES SCP and IE SharePoint</a>.<br>
            """.format(r'\\VMSOAPGSMSSBI06.amr.corp.intel.com\Assets\img\generic-email-banner.png', file_name, table)
    if not version_list_flag:  # Case when version list was not successfully updated
        html = html + """<br>Unable to update the VersionList.xlsx because it is open by another user and "locked for writing". Please update the VersionList manually."""
    html = html + """</p>
                  </body>
                </html>"""

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container
    msg.attach(part1)
    msg.attach(part2)

    if version_list_flag:
        msg['Subject'] = 'E2OpenDataDownload file loaded successfully to SQL database'
    else:
        msg['Subject'] = 'E2OpenDataDownload file loaded successfully to SQL database (VersionList not updated)'
    msg['From'] = accounts['GSM Support'].username
    msg['To'] = ', '.join(recipients)

    # Send the message via the Intel SMTP server.
    s = smtplib.SMTP('smtpauth.intel.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(accounts['GSM Support'].username, accounts['GSM Support'].password)
    s.sendmail(accounts['GSM Support'].username, recipients, msg.as_string())
    s.quit()


def append_to_version_list(old_file: str, snapshot_date: datetime, new_file: str) -> bytes:
    """Function to get the current E2Open Version List and append a new entry to the end.

    Args:
        old_file: [str] Original name of the Excel file in the source folder
        snapshot_date: [datetime] Date from the original file
        new_file: [str] Name of the file that is stored in the destination folder

    Returns:
        [bytes] Excel file data encoded as bytes.

    """
    # Load previous version list from SharePoint
    df = loadExcelFile('https://intel.sharepoint.com/:x:/r/sites/MSOGSEMATSSESSCPandIE/Shared%20Documents/E2Open%20Data%20Download/E2Open%20VersionList.xlsx?d=wfedac8e792cb4a8c8f18c77804c9de17&csf=1&web=1&e=52A7YX', sheet_name='E2OVL')
    if len(df) == 0:
        print('Unable to load data from SharePoint.')
        return bytes()  # Return empty bytes to indicate failure

    # Load Intel Calendar data to calculate Intel WW from date
    query_succeeded, df2, error_msg = querySQL("""SELECT clndr_dt AS [E2OSnapshotDt2]
                                                      ,CONVERT(int, fscl_yr_int_nbr) AS [E2OSnapshotYr]
                                                      ,RIGHT([Intel_Month], 3) AS [E2OSnapshotMth]
                                                      ,CONCAT('ww', CASE WHEN ww_int_nbr < 10 THEN CONCAT('0', ww_int_nbr) ELSE ww_int_nbr END) AS [E2OSnapshotWk]
                                                      ,CONCAT('ww', CASE WHEN ww_int_nbr < 10 THEN CONCAT('0', ww_int_nbr) ELSE ww_int_nbr END, '.', day_of_ww_nbr) AS [E2OSnapshotDate]
                                                FROM dbo.Intel_Calendar
                                                WHERE fscl_yr_int_nbr >= 2022 AND (day_of_ww_nbr IS NOT NULL AND day_of_ww_nbr <> 'NULL')
                                                ORDER BY clndr_dt ASC
                                                """)
    if not query_succeeded:
        print(error_msg)
        return bytes()  # Return empty bytes to indicate failure
    else:
        df2 = df2.loc[df2['E2OSnapshotDt2'] == snapshot_date.replace(hour=0, minute=0, second=0, microsecond=0)]  # Lookup entry matching the Snapshot Date in the Intel Calendar

        # Add information for new file to DataFrame
        df2['FileNameOld'] = old_file
        df2['E2OSnapshotDt'] = snapshot_date.strftime('%Y-%m-%d %H-%M-%S')
        df2['Version List'] = "EPDB-" + snapshot_date.strftime('%Y-%m-%d %H-%M-%S')
        df2['FileNameNew'] = new_file

        df2 = df2[['FileNameOld', 'E2OSnapshotDt', 'Version List', 'FileNameNew', 'E2OSnapshotDt2', 'E2OSnapshotYr',
                   'E2OSnapshotMth', 'E2OSnapshotWk', 'E2OSnapshotDate']]  # Reorder columns
        # print(df2.columns)
        # print(df2)

        df2 = df2.append(df, ignore_index=True)
        df2['E2OSnapshotDt2'] = df2['E2OSnapshotDt2'].apply(lambda x: x.date() if isinstance(x, datetime) else x)

        # Create Excel file temporarily in current directory, then remove it
        file_path = 'Temp_Python_Created_File.xlsx'
        df2.to_excel(file_path, sheet_name='E2OVL', index=False)
        with open(file_path, 'rb') as content_file:
            file_content = content_file.read()
        os.remove(file_path)

        return file_content


if __name__ == "__main__":
    start_time = time()

    # initialize variables
    sp_site = "https://intel.sharepoint.com/sites/MSOGSEMATSSESSCPandIE"
    source_relative_url = "/sites/MSOGSEMATSSESSCPandIE/Automation/E2open EquipmentExcelDownload Auto Upload Staging"
    destination_relative_url = "/sites/MSOGSEMATSSESSCPandIE/Shared Documents/E2Open Data Download"
    file_prefix = 'EquipmentExcelDownload'
    table = 'ats.Substrate_Equipment_T1T2_Supplier_Collaboration'
    table2 = 'ats.Substrate_Equipment_T1T2_Supplier_Collaboration_Linked_Tools'
    project_name = 'Substrate Equipment'
    data_area = 'E2Open Data Download'
    recipients = ['mso.gsem.ses.scp.analytics.owners@intel.com']  # PDL created by Jillian Ballard for communications

    if len(recipients) > 1:  # more than 1 recipient
        params['EMAIL_ERROR_RECEIVER'].extend(recipients)
    else:
        params['EMAIL_ERROR_RECEIVER'].append(recipients[0])

    # Load information about which EquipmentExcelDownload file was last loaded to SQL database table
    last_upload_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)  # set today midnight as the default
    query_succeeded, result, error_msg = getSQLCursorResult("SELECT MAX([LoadDtm]) FROM {0};".format(table))
    if not query_succeeded:
        print(error_msg)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        last_upload_date = result[0][0]  # get latest load date from table
        if last_upload_date is None:  # case when there is no data in the table
            last_upload_date = datetime(year=2022, month=1, day=1)  # set the date as Jan 1st, 2022

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
        if file_prefix in file_name:  # and '032' in file_name:
            print('Found file: {}'.format(excel_file.properties['ServerRelativeUrl']))
            # print(excel_file.properties['TimeCreated'])
            # print(excel_file.properties['TimeLastModified'])

            try:
                temp = file_name.split(' ')[1] + ' ' + file_name.split(' ')[2].split('.')[0]
                file_datestamp = datetime.strptime(temp, '%Y-%m-%d %H-%M-%S')
            except (ValueError, IndexError):
                file_datestamp = datetime.strptime(excel_file.properties['TimeCreated'], '%Y-%m-%dT%H:%M:%SZ')
            print(file_datestamp)  # format '2022-05-09 04:19:27'

            if file_datestamp > last_upload_date:  # determine if file is newer than the last one loaded into the SQL database table
                print('Loading file {0} that has not yet been uploaded to the database.'.format(file_name))

                # Load Excel File from SharePoint Online into Pandas DataFrame
                latest_file_path = 'https://intel.sharepoint.com/:x:/r' + source_relative_url + '/' + file_name
                df = loadExcelFile(file_path=latest_file_path, sheet_name='Equipment Data', header_row=1)

                last_column = 'Unknown?'  # for debugging purposes
                try:
                    # Convert dates to proper format for database upload
                    date_columns = ['First Released to T2', 'PO Need Date', 'T1 PO Placed', 'T2 Invoice Date', 'Repayment  Start Date',
                                    'RExFD', 'RTD', 'RDD', 'Actual Dock Date', 'T1ExFD', 'T1TD', 'T1CND', 'T1 UTP Date']
                    for col in date_columns:
                        last_column = col
                        df[col] = df[col].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date() if isinstance(x, str) and int(x[-4:]) > 2000 else None)

                    last_column = 'Intel Allocation %'
                    df['Intel Allocation %'] = df['Intel Allocation %'].apply(lambda x: x/100.0 if isinstance(x, float) else x)  # Normalize percentages between 0 and 1

                    # Remove "Invalid" values from Delta columns
                    delta_columns = ['T1T2 TD Delta', 'i1 ExFD Delta', 'i1 TD Delta', 'i2 ExFD Delta', 'i2 TD Delta']
                    for col in delta_columns:
                        last_column = col
                        df[col] = df[col].apply(lambda x: x if isinstance(x, int) else None)

                    # Format decimal columns
                    decimal_columns = ['T2 Invoice $ (USD) Amount', 'T2 Platform Cost $ (USD)', 'T2 other Costs $ (USD)',
                                       'T1 Other Costs $ (USD)', 'T1 PO Amount $ (USD)', 'PTP Cost $ (USD)']
                    for col in decimal_columns:
                        last_column = col
                        df[col] = df[col].apply(lambda x: float('{:0.2f}'.format(x)) if isinstance(x, float) else x)  # format money to 2 decimal places

                    ### BEGIN Linked Tools Section ###
                    # Unpivot T2 Exit Forecast Date and Tender Date columns
                    df_tools = df.melt(id_vars=['T1 Supplier Name', 'Entity', 'SSEID', 'T1 Site', 'Tool Quantity'],
                                       value_vars=[col for col in df.columns if 'T2ExFD_Tool' in col],
                                       var_name='LinkedTool', value_name='T2ExFD')
                    df_tools['LinkedTool'] = df_tools['LinkedTool'].apply(lambda x: x.split('_')[1])  # Parse tool name from column header

                    df_tools2 = df.melt(id_vars=['T1 Supplier Name', 'Entity', 'SSEID', 'T1 Site', 'Tool Quantity'],
                                        value_vars=[col for col in df.columns if 'T2TD_Tool' in col],
                                        var_name='LinkedTool', value_name='T2TD')
                    df_tools2['LinkedTool'] = df_tools2['LinkedTool'].apply(lambda x: x.split('_')[1])  # Parse tool name from column header

                    df_final = df_tools.merge(df_tools2, how='outer', on=['T1 Supplier Name', 'Entity', 'SSEID', 'T1 Site', 'Tool Quantity', 'LinkedTool'])  # Join T2ExFD unpivot and T2TD unpivot tables
                    df_final = df_final.loc[(df_final['Tool Quantity'].replace(nan, 1).astype(int) >= df_final['LinkedTool'].str[-1].astype(int)) | df_final['T2ExFD'].notna() | df_final['T2TD'].notna()]  # Remove empty values for LinkedTools above the Tool Quantity
                    df_final.drop(['Tool Quantity'], axis=1, inplace=True)  # Remove "Tool Quantity" column from final table

                    # Convert dates to proper format for database upload
                    for col in ['T2ExFD', 'T2TD']:
                        df_final[col] = df_final[col].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date() if isinstance(x, str) and int(x[-4:]) > 2000 else x if isinstance(x, datetime) else None)
                    ### END Linked Tools Section ###

                    keep_columns = ['#IE Alignment', 'GSC Comment', 'T1 Supplier Name', 'Entity', 'SSEID', 'T1 Site',  'Tech Block', 'T1 Fn Area',
                                    'T1 State', 'T1 Site Inventory', 'Intel Allocation %', 'T1 Equipment Description', 'Tool Quantity',
                                    'T1 Tool Audited', 'Tool Identifiers Comments', 'First Released to T2', 'PO Need Date',
                                    'T1 PO Placed', 'T2 Invoice Date', 'T2 Invoice $ (USD) Amount', 'T2 Platform Cost $ (USD)',
                                    'T2 other Costs $ (USD)', 'T1 Other Costs $ (USD)', 'PO vs. PTP Recon', 'PO vs. Invoice Recon',
                                    'T1 PO Amount $ (USD)', 'PTP Cost $ (USD)', 'Repayment Schedule', 'Repayment Trigger Date', 'Repayment Program',
                                    'Repayment  Start Date', 'Tool PO, Invoice & Cost  Data Comments', 'RExFD', 'RTD', 'RDD', 'Actual Dock Date', 'T1ExFD', 'T1TD',
                                    'T1T2 TD Delta', 'i1 ExFD Delta', 'i1 TD Delta', 'i2 ExFD Delta', 'i2 TD Delta',
                                    'T1 Transit Mode', 'T2 Transit Origin', 'T1 WS Tie', 'Tool Shipping Info Comments', 'T1 IQ', 'Intel IQ',
                                    'T1 Transit Time', 'T1CND', 'T1 UTP Date', 'Tool IQ & UTP Comments', 'T2 Supplier Name', 'Model', 'T2 Response',
                                    'T2 Build Type', 'Fcstd Bld Time', 'Unfcstd Bld Time', 'Fcst Notice Time', 'T2 to TD', 'T2 IQ'
                                    ]
                    for col in keep_columns:
                        if col not in df.columns:
                            last_column = col
                    df = df[keep_columns]
                except KeyError:
                    log(False, project_name=project_name, data_area=data_area, error_msg='Column "{0}" not found in EquipmentExcelDownload file.'.format(last_column))

                    excel_file.recycle()  # Remove file from SharePoint if the load failed.
                    ctx.execute_query()
                    raise
                except ValueError:
                    log(False, project_name=project_name, data_area=data_area, error_msg='Invalid value in column "{0}" of the EquipmentExcelDownload file.'.format(last_column))

                    excel_file.recycle()  # Remove file from SharePoint if the load failed.
                    ctx.execute_query()
                    raise

                # add database standards columns to end of DataFrame
                df['LoadDtm'] = file_datestamp
                df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
                df_final['LoadDtm'] = file_datestamp
                df_final['LoadBy'] = 'AMR\\' + os.getlogin().upper()

                sql_columns = ['IEAlignment', 'GSCComment', 'T1SupplierName', 'Entity', 'SSEID', 'T1Site', 'T1FnArea', 'TechBlock',
                               'T1State', 'T1SiteInventory', 'IntelAllocationPercent', 'T1EquipmentDescription', 'ToolQuantity',
                               'T1ToolAudited', 'ToolIdentifiersComments', 'T2FirstReleasedDate', 'PONeedDate', 'T1POPlacedDate',
                               'T2InvoiceDate', 'T2InvoiceAmount', 'T2PlatformCost', 'T2OtherCosts', 'T1OtherCosts', 'POPTPRecon',
                               'POInvoiceRecon', 'T1POAmount', 'PTPCost', 'RepaymentSchedule', 'RepaymentTriggerDays',
                               'RepaymentProgram', 'RepaymentStartDate', 'InvoiceComments', 'RExFD', 'RTD', 'RDD', 'ActualDockDate',
                               'T1ExFD', 'T1TD', 'T1T2TDDelta', 'I1ExFDDelta', 'I1TDDelta', 'I2ExFDDelta', 'I2TDDelta',
                               'T1TransitMode', 'T2TransitOrigin', 'T1WSTie', 'ToolShippingComments', 'T1IQ', 'IntelIQ',
                               'T1TransitTime', 'T1CND', 'T1UTPDate', 'ToolIQUTPComments', 'T2SupplierName', 'Model', 'T2Response',
                               'T2BuildType', 'FcstdBuildTime', 'UnfcstdBuildTime', 'FcstNoticeTime', 'T2toTD', 'T2IQ',
                               'LoadDtm', 'LoadBy'
                               ]

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, columns=sql_columns, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

                    insert_succeeded, error_msg = uploadDFtoSQL(table=table2, data=df_final, truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Linked Tools', row_count=df_final.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df_final.shape[0], table2))
                    else:
                        print(error_msg)

                    # Move Excel file to Archive folder on SharePoint site
                    new_file_name = file_name.split(' ')[0] + ' ' + file_datestamp.strftime('%Y-%m-%d %H-%M-%S') + '.xlsx'
                    excel_file.moveto(new_relative_url=os.path.join(destination_relative_url, new_file_name), flag=1)  # flag=1 means Overwrite, see docs https://docs.microsoft.com/en-us/previous-versions/office/developer/sharepoint-rest-reference/dn450841(v=office.15)?redirectedfrom=MSDN#moveto-method
                    ctx.execute_query()

                    # Log file move in the E2Open VersionList Excel file
                    version_list_updated = True
                    file_content = append_to_version_list(file_name, file_datestamp, new_file_name)
                    if file_content:  # Only update Excel file if version list was loaded correctly
                        try:
                            # Load destination folder information using relative path information
                            target_folder = ctx.web.get_folder_by_server_relative_path(destination_relative_url)
                            ctx.load(target_folder)
                            ctx.execute_query()

                            # Upload Excel to SharePoint
                            target_file = target_folder.upload_file('E2Open VersionList.xlsx', file_content)  # Upload Excel to SharePoint Online
                            ctx.load(target_file)
                            ctx.execute_query()
                        except ClientRequestException as error:
                            version_list_updated = False
                            # log(False, project_name=project_name, data_area=data_area, error_msg=error.args[1])

                    send_success_email(recipients=recipients, file_name=new_file_name, table=table, version_list_flag=version_list_updated)

                else:
                    print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
