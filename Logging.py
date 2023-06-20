import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Union
from datetime import datetime
import pyodbc
import sys
import os
from Project_params import params
from Password import accounts


def flatten(list_of_lists):
    if len(list_of_lists) == 0:
        return list_of_lists
    if isinstance(list_of_lists[0], list):
        return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
    return list_of_lists[:1] + flatten(list_of_lists[1:])


def upload_log_to_sql(log_message: tuple):
    """Function to insert Pandas DataFrame into SQL Server database.

    Args:
        log_message: Data to be entered into Logging Table.

    Returns:
        None.

    """
    # noinspection PyArgumentList
    conn = pyodbc.connect(driver=params['SQL_DRIVER'], server=params['GSMDW_SERVER'], database=params['GSMDW_DB'], Trusted_Connection='yes', autocommit=False)  # Connect via pyodbc
    cursor = conn.cursor()

    schema = ['log_type', 'scope', 'log_source', 'data_area', 'rec_count', 'log_comment']
    schema_string = '(' + ', '.join(schema) + ')'
    values_string = '(?'
    for _ in range(len(schema) - 1):
        values_string += ', ?'
    values_string += ')'

    insert_statement = "INSERT INTO " + params['Table_Log'] + " " + schema_string + " VALUES " + values_string + ";"
    cursor.execute(insert_statement, log_message)
    conn.commit()

    conn.close()


def send_error_email(package_name: str, data_area: str, error_msg: pyodbc.Error):
    """Function to email support users that the job has failed.

    Args:
        package_name: Name of Python module that is executing log function.
        data_area: Affected area.
        error_msg: Error message from Python.

    Returns:
        None.

    """
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')

    # Create the body of the message (a plain-text and an HTML version).
    text = """The Python Script "{0}" failed on {1}.\n\nThe generated error message is as follows:\n\t{2}
              Visit the Job Failure Dashboard and navigate to the "Recent Script Jobs" page for the latest details.
              If you do not have access, you can request through this AGS entitlement: GSM- KPI PBIRS Developer.
              You’ll need to click the link twice (let the page load, then come back and click this link again) if it does not open correctly in AGS.
              """.format(package_name, datetime.today().strftime("%m-%d-%Y %H:%M:%S"), error_msg)

    html = """
            <html>
              <head>
                <img src="{0}" alt="Email header" width="700" height="150">
              </head>
              <body>
                <p>The Python Script "{1}" failed on {2}.<br>
                  <br> 
                  The generated error message is as follows:<br>
                  &emsp;{3}<br>
                </p>
              </body>
              <foot>
                Visit the <a href="https://sqlbiprd.intel.com/reports/powerbi/GSM/SSBI%20Operations/JobFailureDashboard?rs:embed=True">Job Failure Dashboard</a> 
                and navigate to the "Recent Script Jobs" page for the latest details. 
                If you do not have access, you can request through this AGS entitlement: <a href="https://ags.intel.com/identityiq/ui/rest/redirect?rp1=/accessRequest/accessRequest.jsf&rp2=accessRequest/manageAccess/add?filterKeyword=GSM-%20KPI%20PBIRS%20Developer">GSM- KPI PBIRS Developer</a>. 
                You’ll need to click the link twice (let the page load, then come back and click this link again) if it does not open correctly in AGS.
              </foot>
            </html>
            """.format(r'\\VMSOAPGSMSSBI06.amr.corp.intel.com\Assets\img\email-error-banner.png', package_name, datetime.today().strftime("%m-%d-%Y %H:%M:%S"), error_msg)

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container
    msg.attach(part1)
    msg.attach(part2)

    msg['Subject'] = 'Python ' + data_area + ' Data Loader Failed'
    msg['From'] = accounts['GSM Support'].username
    try:
        msg['To'] = ', '.join(params['EMAIL_ERROR_RECEIVER'])
    except TypeError:  # list of Lists passed in EMAIL ERROR RECEIVER parameter
        msg['To'] = ', '.join(flatten(params['EMAIL_ERROR_RECEIVER']))

    # Send the message via the Intel SMTP server.
    s = smtplib.SMTP('smtpauth.intel.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    # TODO: Add error handling for failed login here
    # try:
    s.login(accounts['GSM Support'].username, accounts['GSM Support'].password)
    # except smtplib.SMTPAuthenticationError:
    s.sendmail(accounts['GSM Support'].username, params['EMAIL_ERROR_RECEIVER'], msg.as_string())
    s.quit()


def log_warning(project_name: str, data_area: str, package_name: str = os.path.basename(sys.argv[0]), file_path: str = None, warning_type: str = 'Missing'):
    """Function to upload log information to SQL Server for missing files or files that have not been updated.

    Args:
        project_name: Name of project.
        data_area: Affected area.
        package_name: Name of Python module that is executing log function.
        file_path: Path to the missing file.
        warning_type: What type of warning to log. Accepted values are "Missing" or "Not Modified". Default "Missing" assumes a file was not found on a Shared Drive.

    Returns:
        None.

    """
    if not package_name.startswith('Python: ') and not package_name.startswith('SQL: '):  # add script type if not already present
        package_name = 'Python: ' + package_name

    if warning_type.lower() == 'missing':  # File was not found
        log_comment = 'No file(s) found in the {0} folder.'.format(file_path)
    elif warning_type.lower() == 'not modified':  # File has not been updated since the last upload
        log_comment = 'Skipped {0} load as it has not been modified since the last upload.'.format(data_area)
    else:
        log_comment = None
    upload_log_to_sql(('W', project_name, package_name, data_area, 0, log_comment))


def log(sql_stmt_succeeded: bool, project_name: str, data_area: str, package_name: str = os.path.basename(sys.argv[0]), row_count: int = 0, error_msg: Union[str, pyodbc.Error] = None):
    """Function to upload log information to SQL Server.

    Args:
        sql_stmt_succeeded: True or False if the SQL statement was successful.
        project_name: Name of project.
        package_name: Name of Python module that is executing log function.
        data_area: Affected area.
        row_count: Number of rows successfully inserted into the database table
        error_msg: Error message, if any

    Returns:
        None.

    """
    if not package_name.startswith('Python: ') and not package_name.startswith('SQL: '):  # add script type if not already present
        package_name = 'Python: ' + package_name

    if sql_stmt_succeeded:
        upload_log_to_sql(('I', project_name, package_name, data_area, row_count, None))  # log success
    else:
        upload_log_to_sql(('E', project_name, package_name, data_area, 0, 'Error message: {}'.format(error_msg)))  # log error
        if params['EMAIL_ERROR_NOTIFICATIONS']:  # Only send error email if uploading to PROD
            send_error_email(package_name=package_name, data_area=data_area, error_msg=error_msg)
        else:
            print("ERROR: Failed to insert.\nError message: \n\n{}".format(error_msg))
