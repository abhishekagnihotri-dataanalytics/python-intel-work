__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SCRAM tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Every 5 minutes at the 2nd minute mark"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import imaplib
import email
from email import policy, encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from time import time, sleep
import shutil
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, executeSQL
from Logging import log
from Password import accounts


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def download_email_attachment(destination: str, email_subject: str, file: str = None, exact_match: bool = True, delete_email: bool = False) -> list:
    """Download an email attachment and save to SharedDrive folder.

        Args:
            destination: Folder path to store the downloaded Email attachment
            email_subject: Subject of email containing file
            file: [str] File to search for in email (if none provided, all files in email are moved)
            exact_match: [bool] If true, file name must match email exactly. Use false when file name is dynamic to match any file that contains the specified file name
            delete_email: [bool] If true, moves email message to trash after processing.

        return: [two element list] [list of str] List of downloaded file names. If no Excel files were found and downloaded, list is empty. [list of str] List of
    """
    files_loaded = list()
    incidents = list()

    print("Started email login")
    mail = imaplib.IMAP4_SSL(params['EMAIL_SERVER'], params['EMAIL_SERVER_PORT'])  # server, port
    try:
        print('Attempting email login')
        mail.login(accounts['GSM Ariba'].username, accounts['GSM Ariba'].password)  # login to email account

        mail.select('Inbox')
        search_query = '(SUBJECT "' + email_subject + '")'
        print('Searching mailbox')
        result, data = mail.search(None, search_query)
        ids = data[0]
        id_list = ids.split()

        for latest_email_id in id_list:
            result, email_data = mail.fetch(latest_email_id,'(RFC822)')  # fetch the email body (RFC822) for the given ID
            raw_email_string = email_data[0][1].decode('utf-8')  # converts byte literal to string removing b''
            email_message = email.message_from_string(raw_email_string, policy=policy.default)
            print('Reading email with subject: {}'.format(email_message['subject']))
            print('Email was sent from: {}'.format(email_message['from']))

            body = email_message.get_body(preferencelist=('html', 'plain'))
            if body:
                body = body.get_content()
            # print(body)
            incident_keys = list()
            incident_info = list()
            soup = BeautifulSoup(body, 'html.parser')

            # Extract incident information for html table
            tags = ['Time:', 'Impact radius:']  # tags to search for in the table
            table = soup.find(lambda tag: tag.name == 'h4' and 'Incident Details' in tag.text).find_next('table')  # search for table containing incident information
            rows = table.find_all('tr')
            row_count = 1
            for row in rows:
                cols = row.find_all('td')
                cols = [cell.text.strip() for cell in cols]
                if row_count == 1:  # First row always contains the Incident Title
                    incident_keys.append('Incident')
                    incident_info.append(cols[0])
                elif any(x in cols[0] for x in tags):  # Check if other rows match any of the seach tags
                    temp_split = cols[0].split(':', 1)
                    incident_keys.append(temp_split[0])
                    incident_info.append(temp_split[1].lstrip())
                row_count += 1  # increment row count

            # Extract outage probability from email body
            if 'Initial Alert' in email_message['subject']:
                div_tag = soup.find(lambda tag: tag.name == 'h4' and 'Supplier Impact Prediction' in tag.text).find_next('div')  # search for div containing outage information
                outage_info = div_tag.contents[0]
                incident_keys.append('Outage probability')
                incident_info.append(outage_info.split(',')[0].split(' ')[-1])

            # TODO: Extract incident description information from email body
            incident_keys.append('Description')
            incident_info.append(None)
            # print(incident_keys)
            # print(incident_info)

            # Extract incident hyperlink from html
            link = soup.find(lambda tag: tag.name == 'h4' and 'News Links' in tag.text).find_next('a', href=True)
            # print(link.contents[0])
            # print(link['href'])

            if link.contents[0] != 'bcmsupport@supplyrisk.com':  # ignore bad website link
                site = link.contents[0]
                incident_keys.append('Link')
                incident_info.append(site)

                news_sites = ['www.gdacs.org', 'inciweb.nwcg.gov']
                if any(x in site for x in news_sites):  # only query known news websites

                    # Extract incident latitude and longitude from news source
                    response = requests.get(site)
                    if response.status_code == 200:
                        try:
                            if 'www.gdacs.org' in site:
                                soup2 = BeautifulSoup(response.text, 'html.parser')
                                div = soup2.find("div", {"id": "alert_summary_left"})
                                for table in div.find_all('table'):
                                    rows = table.find_all('tr')
                                    for row in rows:
                                        cols = row.find_all('td')
                                        cols = [ele.text.strip() for ele in cols]
                                        # print(cols)
                                        tags = ['GDACS ID', 'Lat/Lon:']
                                        if cols[0] in tags:
                                            if cols[0] == 'Lat/Lon:':
                                                coordinates = cols[1].split(' , ')
                                                # print('Latitude: {}'.format(coordinates[0]))
                                                # print('Longitude: {}'.format(coordinates[1]))
                                                incident_keys.append('Latitude')
                                                incident_info.append('{:0.4f}'.format(float(coordinates[0]))) # trim latitude and longitude to 4 decimal places (accurate within 11.1 meters)
                                                incident_keys.append('Longitude')
                                                incident_info.append('{:0.4f}'.format(float(coordinates[1])))
                                            elif cols[0] == 'GDACS ID':
                                                # incident_keys.append('Incident Id')
                                                # incident_info.append(cols[1].split(' ')[1])
                                                incident_types = {'EQ': 'Earthquake',
                                                                  'FL': 'Flood',
                                                                  'TC': 'Tropical Cyclone',
                                                                  'VO': 'Volcano',
                                                                  'WF': 'Wildfire'
                                                                  }
                                                incident_keys.append('Incident Type')
                                                try:
                                                    incident_info.append(incident_types[cols[1].split(' ')[0]])
                                                except KeyError:  # key not in incident_types variable
                                                    incident_info.append(None)
                            elif 'inciweb.nwcg.gov' in site:
                                soup2 = BeautifulSoup(response.text, 'html.parser')
                                div = soup2.find("div", {"id": "IncidentInformation"})
                                for table in div.find_all('table'):
                                    rows = table.find_all('tr')
                                    for row in rows:
                                        cols = row.find_all('td')
                                        cols = [ele.text.strip() for ele in cols]
                                        # print(cols)
                                        tags = ['Incident Type', 'Coordinates']
                                        if cols[0] in tags:
                                            if cols[0] == 'Coordinates':
                                                coordinates = cols[1].split(', ')
                                                for coordinate in coordinates:
                                                    temp = coordinate.split()
                                                    # print('{0}: {1}'.format(temp[1].capitalize(), temp[0]))
                                                    incident_keys.append(temp[1].capitalize())
                                                    incident_info.append(temp[0])
                                            else:
                                                # print('{0}: {1}'.format(cols[0], cols[1]))
                                                incident_keys.append(cols[0])
                                                incident_info.append(cols[1])

                        except AttributeError:  # BeautifulSoup does not find div (aka Lat/Long info cannot be parsed from website)
                            print('Exception found in BeautifulSoup parsing.')
                            break  # exit if condition

            incident = dict(zip(incident_keys, incident_info))
            try:
                incident['Time'] = datetime.strptime(incident['Time'], '%I:%M %p %Z on %d %b %Y')  # convert time column from str to datetime object
            except ValueError:  # month name was not abbreviated
                incident['Time'] = datetime.strptime(incident['Time'], '%I:%M %p %Z on %d %B %Y')
            if 'Outage probability' in incident.keys():
                incident['Outage probability'] = float(incident['Outage probability'].split('%')[0]) / 100  # convert outage probability from str to decimal
            else:
                incident['Outage probability'] = None
            if 'Link' not in incident.keys():  # if no news link found for the incident, add a placeholder key
                incident['Link'] = None
            if 'Incident Type' not in incident.keys():
                if 'Earthquake' in incident['Incident']:
                    incident['Incident Type'] = 'Earthquake'
                elif 'Eruption' in incident['Incident']:
                    incident['Incident Type'] = 'Volcano'
                elif 'Flood' in incident['Incident']:
                    incident['Incident Type'] = 'Flood'
                else:
                    incident['Incident Type'] = None
            if 'Longitude' not in incident.keys():  # if news link was unable to be parsed, add placeholder keys
                incident['Longitude'] = None
                incident['Latitude'] = None
            incidents.append(incident)

            # downloading attachments
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                    continue

                # parse file name
                file_name, encoding = email.header.decode_header(part.get_filename())[0]
                if encoding:  # if file_name is encoded, decode it first
                    print('Decoding file name.')
                    file_name = file_name.decode(encoding)
                if '\r' in file_name or '\n' in file_name:  # if file_name has line breaks (new lines), remove them
                    print('New line found in file name.')
                    file_name = file_name.replace('\r', '').replace('\n', '')
                # print(file_name)

                if bool(file_name):  # if file exists
                    print('Found file: {}'.format(file_name))
                    if file:  # if file name is specified by user, otherwise load all documents
                        if exact_match:  # else if file name is specified and user wants exact name match
                            if file_name != file:  # name of file does not match exactly specified file name
                                continue
                        else:  # else if file name is specified and user does not want exact name match
                            if file not in file_name:  # name of file contains the specified file name
                                continue

                    print('Moving file "{0}" to {1}.'.format(file_name, destination))
                    file_path = os.path.join(destination, file_name)
                    if os.path.isfile(file_path):  # check if file already exists in filepath
                        os.remove(file_path)  # if file already exists, remove it and reload
                    fp = open(file_path, 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()
                    files_loaded.append(file_name)

            if delete_email:
                print('Deleting email with subject: {}'.format(email_message['subject']))
                mail.store(latest_email_id, '+FLAGS', '\\Deleted')

    except imaplib.IMAP4.error as error:
        if error.args[0] == b'LOGIN failed.':  # error raised by mail.login() function
            # TODO: add error logging for email login failed
            print("Failing logging into the {} email account!".format(accounts['GSM Ariba'].username))
        else:
            print(error)
            raise error
    except OSError:  # error raised by os.remove() function
        # TODO: add error logging for failed file delete
        print('Unable to remove file prior to reload')
    # except ConnectionResetError as error:
    #   # TODO: add error logging for connection reset
    finally:
        mail.expunge()
        mail.close()
        mail.logout()

    return [files_loaded, incidents]


def parse_email(email_subject: str, delete_email: bool = False) -> list:
    """Read from an email body

        Args:
            email_subject: Subject of email containing file
            delete_email: [bool] If true, moves email message to trash after processing.

        return: [two element list] [list of str] List of downloaded file names. If no Excel files were found and downloaded, list is empty. [list of str] List of
    """
    emails_parsed = list()

    print("Started email login")
    mail = imaplib.IMAP4_SSL(params['EMAIL_SERVER'], params['EMAIL_SERVER_PORT'])  # server, port
    try:
        print('Attempting email login')
        mail.login(accounts['GSM Ariba'].username, accounts['GSM Ariba'].password)  # login to email account

        mail.select('Inbox')
        search_query = '(SUBJECT "' + email_subject + '")'
        print('Searching mailbox')
        result, data = mail.search(None, search_query)
        ids = data[0]
        id_list = ids.split()

        for latest_email_id in id_list:
            result, email_data = mail.fetch(latest_email_id,'(RFC822)')  # fetch the email body (RFC822) for the given ID
            raw_email_string = email_data[0][1].decode('utf-8')  # converts byte literal to string removing b''
            email_message = email.message_from_string(raw_email_string, policy=policy.default)
            print('Reading email with subject: {}'.format(email_message['subject']))
            print('Email was sent from: {}'.format(email_message['from']))

            body = email_message.get_body(preferencelist=('html', 'plain'))
            if body:
                body = body.get_content()

                soup = BeautifulSoup(body, 'html.parser')

                # Extract incident hyperlink from html
                link = soup.find(lambda tag: tag.name == 'h4' and 'Supply Chain and Logistics Incidents' in tag.text).find_next('a', href=True)
                news_name = link.contents[0]
                news_link = link['href']
                # print(link.contents[0])
                # print(link['href'])

                emails_parsed.append([news_name, news_link, body])  # append email content to return value list

            # print(body)

            if delete_email:
                print('Deleting email with subject: {}'.format(email_message['subject']))
                mail.store(latest_email_id, '+FLAGS', '\\Deleted')

    except imaplib.IMAP4.error as error:
        if error.args[0] == b'LOGIN failed.':  # error raised by mail.login() function
            # TODO: add error logging for email login failed
            print("Failing logging into the {} email account!".format(accounts['GSM Ariba'].username))
        else:
            print(error)
            raise error
    except OSError:  # error raised by os.remove() function
        # TODO: add error logging for failed file delete
        print('Unable to remove file prior to reload')
    except ConnectionResetError as error:
        # TODO: add error logging for connection reset
        mail.expunge()
        mail.close()
        mail.logout()
        raise error
    finally:
        mail.expunge()
        mail.close()
        mail.logout()

    return emails_parsed


def send_email(recipients: list, event_title: str, event_type: str, event_location: str, excel_file: str = None):
    """Function to send an email

            Args:
                recipients: [list of str] The TO field in an email. Who is actually receiving the email.
                event_title: [str] Event title to include in the email.
                event_type: [str] Event type to include in the email.
                event_location: [str] Event location to include in the email.
                excel_file: [str] File path to Excel file to be attached to email.

            return: None
        """
    cc_recipients = ['gsc.bc@intel.com', 'mary.e.abel@intel.com']
    bcc_recipients = ['matthew1.davis@intel.com']

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')

    # Create the body of the message (a plain-text and an HTML version).
    text = """Intel Confidential, please do not reply all when responding to this message\n
            Level 0 - GSC Business Continuity Communications:\n
            This email is to advise you that we are aware of the {0} that just occurred{1}. Our suppliers and commodity teams have been notified directly from Supply Risk Solutions (SRS). We are monitoring the situation. 
            Should this event escalate into a higher risk Level (as outlined below), this message will be followed by further communications and details. 
            To view this event, please log into our SRS Dashboard and click the "Live Event Tracking" button for the latest details.\n\n
            If you do not have access, you can request through this AGS entitlement: PBIRS_GSC_BCP_SRS_BROWSER.
            You’ll need to click the link twice (let the page load, then come back and click this link again) if it does not open correctly in AGS.\n\n
            You can learn more about this event: "{2}" by logging into the SRS Reporting Portal and clicking on the "Monitor" tab at the top of the page.
            Additionally, to see an initial list of suppliers that may be impacted by this event, please see the attached Excel file.\n\n
            BC Crisis Risk Level Escalations and Response Requirements\n<Table here>\n
            Commodity teams, please continue to work with your suppliers to respond to the SRS alert requests.\n
            Should you have any questions related to this event or on the dashboard, please don’t hesitate to contact the BC PMO or directly to your department’s BC Champion – thank you.\n
            GOTO/GSCBC to access the GSC BC Crisis Response Playbook
           """.format(event_type.title() if event_type is not None else 'disaster', '' if event_location is None else ' in the ' + event_location.title() if event_location.title()[-1] == 's' else ' in ' + event_location.title(), event_title)

    html = """
        <html>
          <head>
            <img src="{0}" alt="Email header" width="700" height="150">
            <p style="color:red"><font size="+1">
                *** Intel Confidential, please do not reply all when responding to this message ***
            </font></p>
          </head>
          <body>
            <p><font size="+1"> <b>Level 0</b> - GSC Business Continuity Communications:<br>
              <br> 
              This email is to advise you that we are aware of the {1} that just occurred{2}. Our suppliers and commodity teams have been notified directly from Supply Risk Solutions (SRS). We are monitoring the situation.
              Should this event escalate into a higher risk Level (as outlined below), this message will be followed by further communications and details.
              To view this event, please log into our <a href="https://sqlbiprd.intel.com/reports/powerbi/GSM/Risk%20Management/Supply%20Risk%20Solutions/SRS%20Dashboard?rs:embed=True">SRS Dashboard</a> 
              and click the "Live Event Tracking" button for the latest details.<br><br>
              If you do not have access, you can request through this AGS entitlement: <a href="https://ags.intel.com/identityiq/ui/rest/redirect?rp1=/accessRequest/accessRequest.jsf&rp2=accessRequest/manageAccess/add?filterKeyword=PBIRS%5FGSC%5FBCP%5FSRS%5FBROWSER">PBIRS_GSC_BCP_SRS_BROWSER</a>. 
              You’ll need to click the link twice (let the page load, then come back and click this link again) if it does not open correctly in AGS.<br><br>
              You can learn more about this event: "<b>{3}</b>" by logging into the <a href="https://customer.supplyrisk.com/">SRS Reporting Portal</a>
              and clicking on the "Monitor" tab at the top of the page.
              Additionally, to see an initial list of suppliers that may be impacted by this event, please see the attached Excel file.<br>
            </font></p>
            </p><font size="+2">
              <b>BC Crisis Risk Level Escalations and Response Requirements</b>
            </font></p>
            <table cellspacing="3" border="1" bgcolor=black><font size="+1">
              <thead>
                <tr bgcolor="#ffffff">
                  <th><b>Level</b></th>
                  <th><b>Description</b></th>
                </tr>
              </thead>
              <tbody>
                <tr bgcolor="#ffffff">
                  <td style="background-color:#efefef">Level 0</td>
                  <td>FYI: not currently impacting the supply chain; <font color="blue">no action</font> is required. (May be prevalent in the media) </td>
                </tr>
                <tr bgcolor="#ffffff">
                  <td style="background-color:#34ff34">Level 1</td>
                  <td>Lowest level/urgency: Responses expected no later than ~ <font color="blue">24 hours</font> from initial BC Alert notification</td>
                </tr>
                <tr bgcolor="#ffffff">
                  <td style="background-color:#f8ff00">Level 2</td>
                  <td>Mid-level/urgency: Responses expected no later than ~ <font color="blue">12 hours</font> from initial BC Alert notification</td>
                </tr>
                <tr bgcolor="#ffffff">
                  <td style="background-color:#fe0000">Level 3</td>
                  <td>Highest level/urgency: Responses expected no later than ~ <font color="blue">6 hours</font> from initial BC Alert notification</td>
                </tr>
              </tbody>
            </font></table>
          </body>
          <foot>
            <p><font size="+1">
              <i>Commodity teams, please continue to work with your suppliers to respond to the SRS alert requests.</i>
            </font></p>
            <p style="color:blue"><font size="+1">
              <i>Should you have any questions related to this event or on the dashboard, please don’t hesitate to contact the <a href="mailto:gsc.bc@intel.com">BC PMO</a> or directly to your department’s BC Champion – thank you.</i>
            </font></p>
            <p><font size="+1">
                <a href="https://intel.sharepoint.com/sites/GSCRiskManagement/GSCBusinessContinuity">GOTO/GSCBC</a> to access the <a href="https://intel.sharepoint.com/:w:/r/sites/gscbusinesscontinuity/_layouts/15/Doc.aspx?sourcedoc=%7B62E58EDF-937B-4AF5-97FF-DB6D9388A2B1%7D&file=SC%20Business%20Continuity%20Crisis%20Response%20Playbook.docx&action=default&mobileredirect=true">SC Business Continuity Crisis Response Playbook</a>
            </font></p>
          </foot>
        </html>
        """.format(r'\\VMSOAPGSMSSBI06.amr.corp.intel.com\Assets\img\srs-email-banner.png', event_type.title() if event_type is not None else 'disaster', '' if event_location is None else ' in the ' + event_location.title() if event_location.title()[-1] == 's' else ' in ' + event_location.title(), event_title)

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container
    msg.attach(part1)
    msg.attach(part2)

    # Attach Excel file to email
    if excel_file:
        # Open file and add create an email object for it
        part3 = MIMEBase('application', "octet-stream")
        with open(excel_file, 'rb') as file:
            part3.set_payload(file.read())
        # After the file is closed
        encoders.encode_base64(part3)
        part3.add_header('Content-Disposition', 'attachment; filename="{}"'.format(os.path.basename(excel_file)))
        msg.attach(part3)

    msg['Subject'] = "GSC BC Communications <Level 0> Initial Alert: '{}'".format(event_title)
    msg['From'] = accounts['GSM Support'].username
    msg['To'] = ', '.join(recipients)
    msg['CC'] = ', '.join(cc_recipients)
    msg['BCC'] = ', '.join(bcc_recipients)

    # Send the message via the Intel SMTP server.
    s = smtplib.SMTP('smtpauth.intel.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(accounts['GSM Support'].username, accounts['GSM Support'].password)
    if params['EMAIL_ERROR_NOTIFICATIONS']:  # for PROD configuration send email to recipients and CC_recipients
        s.sendmail(accounts['GSM Support'].username, recipients + cc_recipients + bcc_recipients, msg.as_string())
    else:  # for DEV configuration only send email to recipients
        s.sendmail(accounts['GSM Support'].username, recipients, msg.as_string())
    s.quit()


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    # Initialize variables
    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'BCP SRS Dashboard'
    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\BCP\SRS"

    # Email variables
    if params['EMAIL_ERROR_NOTIFICATIONS']:  # When True then config is using a Production environment
        delete_bool = True
        # recip = ['susan.l.duncan@intel.com', 'mary.e.abel@intel.com', 'janette.l.ohalloran@intel.com', 'edwin.a.navas@intel.com','matthew1.davis@intel.com']
        recip = ['gsc.crisis.communication@intel.com', 'mso.gsem.ats.biz.ops@intel.com']
    else:
        delete_bool = False  # Don't delete email after reading in DEV configuration
        recip = ['matthew1.davis@intel.com']
        # sendEmail(recip, event_title='Volcano near Manila, Philippines', event_type='Volcano', event_location='Philippines', excel_file=os.path.join(shared_drive_folder_path, 'Archive', 'Supplier Impact Responses for Volcano near Manila, Philippines (2021-07-02 17-10 UTC).xlsx'))
        # exit()

    ### BEGIN Download emailed Excel files from mailbox ###
    srs_files = ['Initial Alert', 'Updated Alert']
    for subject in srs_files:
        file_name = subject + '.xlsx'
        try:
            print('Attempting to download {} from gsmariba@intel.com'.format(file_name))
            excel_files, incidents = download_email_attachment(shared_drive_folder_path, email_subject=subject, file='.xlsx', exact_match=False, delete_email=delete_bool)
        except ConnectionResetError as error:
            # log(False, project_name=project_name, data_area=subject, error_msg=error)
            print(error)
            continue

        if subject.startswith('Initial Alert'):
            for i in range(len(excel_files)):
                excel_file = excel_files[i]
                # print(excel_file)
                # print(incidents[i])
                excel_sheet_name = 'Sites'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=1)
                df['IncidentLink'] = incidents[i]['Link']  # add incident link (unique identifier) to each item
                df['Site Reference'] = df['Site Reference'].apply(lambda x: x[:10] if isinstance(x, str) else x)
                df['Upload_Date'] = datetime.today()
                country = df['Country'].iloc[0]  # set country variable as the first country in the dataframe

                # insert information downloaded from Excel file to database
                columns = ['CompanyName','Subcontractor','SubtierCompany','SiteName','DistanceInMiles','Activity',
                           'Address','City','State','Country','ContactName','ContactEmail','ContactPhoneNumber',
                           'SiteESDID','Commodity','Division','SupplierSegment','IncidentNewsLink','ModifiedDateTime']
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_SRS_Site_Impact'], data=df, columns=columns, categorical=['Site Reference'], truncate=False, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Site Impact (Live)', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_SRS_Site_Impact']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

                    # insert information scraped from email body to database
                    df = pd.DataFrame(incidents[i], index=[0])  # convert the incident dict into a DataFrame for SQL insert
                    df['Upload_Date'] = datetime.today()

                    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_SRS_Live_Events'], data=df, truncate=False, driver="{SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Live Events', row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_SRS_Live_Events']))
                        send_email(recip, event_title=incidents[i]['Incident'], event_type=incidents[i]['Incident Type'], event_location=country, excel_file=os.path.join(shared_drive_folder_path, excel_file))
                    else:
                        print(error_msg)
                else:
                    print(error_msg)

        elif subject.startswith('Updated Alert'):
            for i in range(len(excel_files)):
                excel_file = excel_files[i]
                excel_sheet_name = 'Sites'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=1)
                df['IncidentLink'] = incidents[i]['Link']  # add incident link (unique identifier) to each item
                df['Site Reference'] = df['Site Reference'].apply(lambda x: x[:10] if isinstance(x, str) else x)  # truncate site reference field to 10 characters
                df['Time Of Response'] = df['Time Of Response'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S UTC') if isinstance(x, str) else x)  # convert string to datetime
                df['Upload_Date'] = datetime.today()
                # print(df.columns)

                # delete previous responses prior to uploading new ones
                delete_statement = """DELETE FROM bcp.SRS_Live_Site_Impact WHERE [IncidentNewsLink] = '{}'""".format(incidents[i]['Link'])
                delete_success, error_msg = executeSQL(delete_statement)
                if not delete_success:
                    log(delete_success, project_name=project_name, data_area='Site Impact (Live)', error_msg=error_msg)
                    exit(1)
                else:  # delete was successful
                    # insert information from latest response Excel file
                    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_SRS_Site_Impact'], data=df, categorical=['Site Reference'], truncate=False, driver="{ODBC Driver 17 for SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Site Impact (Live)', row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_SRS_Live_Events']))
                        successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                        renamed_files.append(excel_file)
                    else:
                        print(error_msg)

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))
    ### END Download emailed Excel files from mailbox ###

    ### BEGIN Load News from mailbox ###
    subject = 'Supply Chain News'
    table = 'bcp.SRS_News'

    conn_error_msg = ''
    retries = 0
    while retries < 3:
        try:
            news = parse_email(email_subject=subject, delete_email=delete_bool)
            if news:  # at least one news article was returned
                df = pd.DataFrame(news, columns=['News Title', 'News Link', 'Email Body'])

                df = df[['Email Body', 'News Link']]  # Remove and reorder columns
                df['LoadDtm'] = datetime.now()
                df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

                insert_succeeded, error_msg = uploadDFtoSQL(table, df, truncate=False)
                log(insert_succeeded, project_name=project_name, data_area=subject, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                else:
                    print(error_msg)
            # else:
            #     log(False, project_name=project_name, data_area=subject, error_msg='No News loaded. Possible error?')
            break  # exit out of the while loop since news was successfully retrieved from email

        except ConnectionResetError as error:
            print(error)
            conn_error_msg = error
            sleep(30)  # sleep 30 seconds
            retries += 1  # add an additional count to retries

    if retries == 3:   # previous loop was unable to connect to the email after three retries
        log(False, project_name=project_name, data_area=subject, error_msg=conn_error_msg)
    ### END Load News from mailbox ###

    print("--- %s seconds ---" % (time() - start_time))
