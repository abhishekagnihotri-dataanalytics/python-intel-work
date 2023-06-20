__author__ = "Abhishek Agnihotri"
__email__ = "abhishek.agnihotri@intel.com"
__description__ = "This script downloads Ariba report excels and pushes them into the sql tables"
__schedule__ = "multi-schedule"

import os
import openpyxl
import xlrd
import shutil
import numpy as np
import sys;sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import downloadEmailAttachment, uploadDFtoSQL,loadExcelFile,map_columns,executeStoredProcedure
from Logging import log, log_warning
from datetime import datetime
from time import time , sleep
from zipfile import ZipFile
import imaplib
import email
import win32com.client as win32
import os
import sys
from Project_params import params
from Password import accounts, decrypt_password
import pandas as pd
import argparse







# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass



def downloadEmailAttachment_(destination: str, email_subject: str, email_receiver: str = accounts['GSM Ariba'].username,
                            email_receiver_password: str = accounts['GSM Ariba'].password,
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
        print(ids)
        #sort the list below
        # id_list = ids.split().sort(reverse=True)
        id_list = ids.split()
        print(id_list)

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
                if part.get_content_maintype() == 'multipart' or part.get(
                        'Content-Disposition') is None:  # skip email parts that are not attachments
                    continue

                # Parse file name from attachment
                file_name, encoding = email.header.decode_header(part.get_filename())[0]
                if encoding:  # if file_name is encoded, decode it first
                    file_name = file_name.decode(encoding)
                if '\r' in file_name or '\n' in file_name:  # if file_name has line breaks (new lines), remove them
                    file_name = file_name.replace('\r', '').replace('\n', '')
                print(file_name)

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

            break

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

    return files_loaded[0]


if __name__ == "__main__":
    # shared_drive_folder_path = sys.argv[1]
    # ariba_files = [sys.argv[2]]
    # change_file_name = sys.argv[3]
    # delete_bool = sys.argv[4]
    # print(ariba_files)
    # print(eval(delete_bool))

    # parser=argparse.ArgumentParser()
    # parser.add_argument("NegPlan",action='store')
    # args=parser.parse_known_args()
    # print(args)
    # exit()
    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\Sustainability\Ariba"
    ariba_files = ['Background Report All Contracts- Operational Analytics has completed']
    change_file_name = "ExecDash_Ariba_Contract"
    delete_bool = "False"
    subject='Background Report All Contracts- Operational Analytics has completed'
    #

    start_time = time()
    # print(start_time)
    # initialize variables
    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'Ariba_Report_Extractor'
    params['EMAIL_ERROR_RECEIVER'].append('abhishek.agnihotri@intel.com')

    ##################### Execute transform data after Ariba Loads##############################################
    if ariba_files[0] == 'Mock Subject to run Ariba Stored procedures':
        sp_name_1 = 'negplan.Calculate_Contract_TPT'
        data_area_1 = 'NegPlan Contract TPT Calculation'
        sp_name_2 = 'negplan.Update_NegPlan_ScoreCard'
        data_area_2 = 'NegPlan Contract Scorecard Calculation'

        update_succeeded_1, error_msg_1 = executeStoredProcedure(sp_name_1)
        if update_succeeded_1:
            print("Successfully executed [negplan].[Calculate_Contract_TPT]")
        else:
            log(update_succeeded_1, project_name=project_name, data_area=data_area_1, error_msg=error_msg_1)

        update_succeeded_2, error_msg_2 = executeStoredProcedure(sp_name_2)
        if update_succeeded_2:
            print("Successfully executed [negplan].[Calculate_Contract_TPT]")
        else:
            log(update_succeeded_2, project_name=project_name, data_area=data_area_2, error_msg=error_msg_2)

        # column_info = map_columns(table=table, df=df, display_result=True, sql_columns=sql_column_order)
    else:

        # Download emailed Excel files from mailbox
        # ariba_files = ['Background Report All Intel Neg Plans Report has completed Test']
        for subject in ariba_files:
            file = subject


            conn_error_msg = ''
            retries = 0
            while retries < 3:
                try:

                    print('Checkign if the file {} already exists'.format(str(change_file_name)+".xls"))
                    if os.path.isfile(os.path.join(shared_drive_folder_path, str(change_file_name))+".xls"):
                        print('moving file {} to archive'.format(str(change_file_name) + ".xls"))
                        try:
                            shutil.move(os.path.join(shared_drive_folder_path, str(change_file_name))+".xls", os.path.join(shared_drive_folder_path,'Archive',change_file_name)+'_'+str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))+'.xlsx')
                        except IOError as errr:
                            # print(errr)
                            log(False, project_name=project_name, data_area=subject, error_msg="File is open either by user or another process. Full error: {0}".format(errr))
                            exit()
                        # os.remove(os.path.join(shared_drive_folder_path, str(change_file_name))+".xlsx")
                        # except shutil.OSError as error:
                        #     data_area_global="Moving previous Attachment file to archive"
                        #     log(False, project_name=project_name, data_area=data_area_global,
                        #         error_msg="Archive destination does not exists. Full error: {0}".format(
                        #             error))
                    else:
                        print("Warning: previous file not found in location")

                    print('Attempting to download {} from gsmariba@intel.com'.format(file))
                    try:
                        attachment_name=downloadEmailAttachment_(shared_drive_folder_path, email_subject=subject, exact_match=False, delete_email=eval(delete_bool))
                    except IndexError as IndexErr:
                        print(IndexErr)
                        log(False, project_name=project_name,data_area=subject, error_msg="No email received or subject incorrect. Full error: {0}".format(IndexErr))
                    with ZipFile(os.path.join(shared_drive_folder_path, str(attachment_name)), 'r') as zipObj:
                        zipObj.extractall(shared_drive_folder_path)
                        # file_inside_attchmnt=os.path.join(shared_drive_folder_path, str(attachment_name)).rstrip(".zip")
                        file_inside_attchmnt = str(zipObj.filelist[0].filename)

                        os.rename(os.path.join(shared_drive_folder_path, str(file_inside_attchmnt)), os.path.join(shared_drive_folder_path,change_file_name+ ".xls"))
                        # excel = win32.gencache.EnsureDispatch('Excel.Application')
                        # # wb = excel.Workbooks.Open(os.path.join(shared_drive_folder_path, str(file_inside_attchmnt)), os.path.join(shared_drive_folder_path,change_file_name+ ".xls"))
                        rename_file_path = os.path.join(shared_drive_folder_path, str(file_inside_attchmnt))
                        # wb = excel.Workbooks.Open(rename_file_path)
                        # # wb.SaveAs(os.path.join(shared_drive_folder_path, str(file_inside_attchmnt)), os.path.join(shared_drive_folder_path,change_file_name+ ".xlsx"), FileFormat=51)  # FileFormat = 51 is for .xlsx extension
                        # wb.SaveAs(os.path.join(shared_drive_folder_path, str(change_file_name)),
                        #           FileFormat=51)  # FileFormat = 51 is for .xlsx extension# FileFormat = 56 is for .xls extension
                        # sheet_names = [sheet.Name for sheet in wb.Sheets]
                        # wb.Close()
                        # excel.Application.Quit()
                    break



                except ConnectionResetError as error:

                    print(error)
                    conn_error_msg = error
                    sleep(30)  # sleep 30 seconds
                    retries += 1  # add an additional count to retries

                    # os.remove(os.path.join(shared_drive_folder_path, str(attachment_name)))

            if retries == 3:  # previous loop was unable to connect to the email after three retries
                log(False, project_name=project_name, data_area=subject, error_msg=conn_error_msg)

            os.remove(os.path.join(shared_drive_folder_path, str(attachment_name)))
            os.remove(rename_file_path)
            exit()
################################# SQL INSERT ############################################################################

    ##########################Ariba Contract#######################################
            if subject=='Background Report All Contracts- Operational Analytics has completed':

                data_area = 'Ariba Contract'
                table = 'stage.stg_ExecDash_Contract'
                latest_file_path=os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xls'
                # latest_file_path = os.path.join(shared_drive_folder_path, str(file_inside_attchmnt))

                df = pd.DataFrame()
                try:
                    # df1 = loadExcelFile(file_path=latest_file_path, sheet_name=None,  engine='xlrd',header_row=5)
                    df1=pd.read_excel(latest_file_path, engine='xlrd',
                                  header=5)
                    # df1.to_excel('file_2003.xlsx', index=False, header=False)
                except FileNotFoundError as fileError:
                    log(False, project_name=project_name, data_area=data_area,
                        error_msg="Sheet name is incorrect or file does not exists. Full error: {0}".format(fileError))
                    # print(fileError)
                keep_cols=['Is Test Project', 'Organization - Department (L1)'
                ,'Organization - Department (L2)','Organization - Department','Contract Id',
                 'Project - Project Name', 'Related ID', 'Begin Date',
                 'Effective Date - Date', 'Expiration Date - Date', 'Contract Status',
                 'Contract Type', 'Owner Name',
                  'Supplier - Common Supplier ID',
                 'Supplier - Common Supplier', 'Description', 'Hierarchy Type',
                 'Payment Terms', 'sum(Contract Amount)']
                try:
                    df1=df1[keep_cols]
                    # print(df1.columns)
                except KeyError as err:
                    log(False, project_name=project_name, data_area=data_area,
                        error_msg="Column missing/changed/not matching in Excel attachment. Full error: {0}".format(err))


                New_cols_nm=['Is_Test_Project', 'Organization_Department_L1'
                ,'Organization_Department_L2','Organization_Department','Contract_Id',
                 'Project_Name', 'Related_ID', 'Begin_Date',
                 'Effective_Date', 'Expiration_Date', 'Contract_Status',
                 'Contract_Type', 'Owner_Name',
                  'Supplier_Common_Supplier_ID',
                 'Supplier_Common_Supplier', 'Description', 'Hierarchy_Type',
                 'Payment_Terms', 'Contract_Amount']
                try:
                    df1.columns=New_cols_nm
                except ValueError as ValError:
                    log(False, project_name=project_name, data_area=data_area,
                        error_msg="Column missing/changed Excel attachment. Full error: {0}".format(ValError))

                # change the unclassified dates to null
                df1=df1.replace('Unclassified',np.nan)
                for date_col in ['Begin_Date','Effective_Date', 'Expiration_Date']:
                    #df1[date_col] = df1[date_col].replace('Unclassified', np.nan)
                    df1[date_col] = pd.to_datetime(df1[date_col], errors='coerce')
                df1['Related_ID'] = df1['Related_ID'].astype(str)
                    # df1[date] = np.where(df1[date] == "Unclassified", np.nan,df1[date])

                # print(df1['Begin_Date'])
                output = map_columns('stage.stg_ExecDash_Contract', df1, New_cols_nm)
                print(df1)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
                                                            driver='{ODBC Driver 17 for SQL Server}')
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
                else:
                    log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)

    ############Ariba NegPlan################
            elif subject =='Background Report All Intel Neg Plans Report has completed':
                data_area = 'Ariba NegPlan'
                table = 'stage.stg_ExecDash_NegPlan'
                # latest_file_path = os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xls'
                # df = pd.DataFrame()
                latest_file_path=os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xlsx'
                # print(sheet_names)

                df1 = loadExcelFile(file_path=latest_file_path, sheet_name=sheet_names[0], header_row=5,)
                print(df1)
                # df1 =pd.read_excel(latest_file_path, sheet_name='ExecDash_Ariba_NegPlan', header=5)
                # d1=pd.read_csv(latest_file_path, sep = '\t',header=5)
                # print(df1.columns)

                keep_cols=['Is Test Project', 'Organization - Department (L1)','Organization - Department (L2)',
                  'Project - Project Id','Project - Project Name', 'Begin Date',
                 'Contract Effective Date - Date', 'Expiration Date - Date',
                 'End Date - Date', 'State', 'Neg Plan Type', 'Owner Name',
                  'sum(Baseline Spend)']

                df1=df1[keep_cols]

                New_cols_nm = ['Is_Test_Project', 'Org_Dept_L1','Org_Dept_L2',
                  'Project_Id','Project_Name', 'Begin_Date',
                 'Contract_Effective_Date', 'Expiration_Date',
                 'End_Date', 'State', 'Neg_Plan_Type', 'Owner',
                  'Baseline_Spend']
                df1.columns = New_cols_nm
                #change the unclassified dates to null and remove "{" from owner names
                df1=df1.replace('Unclassified',np.nan)
                for date_col in ['Begin_Date','Contract_Effective_Date', 'Expiration_Date','End_Date']:
                    df1[date_col] = pd.to_datetime(df1[date_col], errors='coerce')


                # print(df1.head(5))
                output = map_columns('stage.stg_ExecDash_NegPlan', df1, New_cols_nm)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
                                                            driver='{ODBC Driver 17 for SQL Server}')
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
                else:
                    log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)


    ####################Ariba Contract Task#########################
            elif subject == 'Background Report Contract Task Report has completed':
                data_area = 'Ariba Contract task'
                table = 'stage.stg_NegPlan_Ariba_Contract_Tasks'
                latest_file_path = os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xlsx'
                df = pd.DataFrame()
                df1 = loadExcelFile(file_path=latest_file_path, sheet_name=sheet_names[0], header_row=5)
                # print(df1.columns)

                keep_cols = ['[PCW] Contract Id', '[PCW]Project (Project Name)',
                       '[PCW] Using eSig', '[PCW]Owner (User)','[PTK]Task Name (Task Id)',
                       '[PTK]Task Name (Task Name)', '[PTK] Type', '[PTK]Start Date (Date)',
                       '[PTK]End Date (Date)', '[PTK] Status', 'sum(Contract Amount)']
                #
                df1 = df1[keep_cols]

                New_cols_nm = ['Contract_Id', 'Project_Name',
                               'Using_eSig', 'Owner','Task_ID', 'Task_Name',
                               'Task_Type', 'Begin_Date',
                               'End_Date', 'Status', 'Contract_Amount']
                df1.columns = New_cols_nm
                # change the unclassified dates to null
                df1.replace('Unclassified', np.nan)
                for date_col in ['Begin_Date',
                               'End_Date']:
                    df1[date_col] = pd.to_datetime(df1[date_col], errors='coerce')

                # print(df1.head(5))
                output = map_columns('stage.stg_NegPlan_Ariba_Contract_Tasks', df1, New_cols_nm)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
                                                            driver='{ODBC Driver 17 for SQL Server}')
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
                else:
                    log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)
        #
    #######################Ariba NegPlan Approval########################################################
            elif subject == 'Background Report Neg Plan Approval Task Details Report has completed':
                data_area = 'Ariba Negplan Approval'
                table = 'stage.stg_NegPlan_Ariba_Approval'
                latest_file_path = os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xlsx'
                df = pd.DataFrame()
                df1 = loadExcelFile(file_path=latest_file_path, sheet_name=sheet_names[0], header_row=5)
                # print(df1.columns)

                keep_cols = ['[SPRJ]Project (Project Id)', '[SPRJ]Project (Project Name)',
                       '[SPRJ]Start Date (Date)', '[SPRJ] State', '[SPRJ]Owner (User)',
                       '[APV]Task Name (Task Id)', '[APV]Task Name (Task Name)',
                       '[APV] Task Status', '[APV] Round', '[APV]Action Date (Date)',
                       '[APV]Approved By (User)', '[APV]Approved By (User ID)',
                       '[APV] Comment', '[APV] Reason', 'count(SourcingProject)']
                #
                df1 = df1[keep_cols]

                New_cols_nm = ['Project_Id', 'Project_Name',
                       'Begin_Date', 'State', 'Owner',
                       'Task_ID', 'TaskName',
                       'TaskStatus', 'Round', 'Action_Date',
                       'Approved_By_User', 'Approved_By_User_ID',
                       'Comment', 'Reason', 'Count']
                df1.columns = New_cols_nm
                #check roudn and user id is numeric # remove non numeric from round and user id
                for num in ['Round','Approved_By_User_ID']:
                    if df1[num].dtype!=np.int64:
                        df1[num] = pd.to_numeric(df1[num], errors='coerce')
                    else:
                        continue

                # change the unclassified dates to null
                df1.replace('Unclassified', np.nan)
                for date_col in ['Begin_Date','Action_Date']:
                    df1[date_col] = pd.to_datetime(df1[date_col], errors='coerce')
               # print(df1.head(5))
                output = map_columns('stage.stg_NegPlan_Ariba_Approval', df1, New_cols_nm)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
                                                            driver='{ODBC Driver 17 for SQL Server}')
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
                else:
                    log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)

    ############************Code between these comments is not used anymore as NegPlan Positions is depricated***********#################
            # elif subject == 'Background Report Neg Plan Positions Report has completed':
            #     data_area = 'Ariba Negplan Position'
            #     table = 'stage.stg_NegPlan_Ariba_Position'
            #     latest_file_path = os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xlsx'
            #     df = pd.DataFrame()
            #     df1 = loadExcelFile(file_path=latest_file_path, sheet_name=sheet_names[0], header_row=5)
            #     print(df1.columns)
            ##keep columns is missing the columns for 'Supplier_Common_Supplier_ID', 'Supplier_Common_Supplier'. edit Keep_cols before proceeding and match with new names in New_cols_nm
            #     keep_cols = ['[SPRJ]Project (Project Id)', '[SPRJ]Project (Project Name)',
            #            '[SPRJ]Start Date (Date)', '[SPRJ] State', '[SPRJ]Owner (User)',
            #            '[SFM]Savings Form (Savings Form Id)',
            #            '[SFM]Savings Form (Savings Form Title)', '[SFM] Savings Type',
            #            '[SFM] Version Type', '[SAD] Term / Requirement',
            #            '[SAD] Benchmark / Analysis', '[SAD] Supplier Known Positions',
            #            '[SAD] Opens', '[SAD] Accepts', '[SAD] Revised Accepts',
            #            '[SAD] Achieved', 'count(SourcingProject)']
            #     #
            #     df1 = df1[keep_cols]
            #
            #     New_cols_nm = ['Project_Id', 'Project_Name',
            #            'Begin_Date', 'State', 'Owner',
            #            'Savings_Form_ID',
            #            'Savings_Form_Title', 'Savings_Type',
            #            'Version_Type', 'Term_Requirement',
            #            'Benchmark_Analysis', 'Supplier_Known_Positions',
            #            'Supplier_Common_Supplier_ID', 'Supplier_Common_Supplier', 'Opens',
            #            'Accepts', 'Revised_Accepts','Achieved','Count']
            #     df1.columns = New_cols_nm
            #     # change the unclassified supp id and common supp to null
            #     print(df1.head(5))
            #     output = map_columns('stage.stg_NegPlan_Ariba_Position', df1, New_cols_nm)
            #
            #     insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
            #                                                     driver='{ODBC Driver 17 for SQL Server}')
            #     if insert_succeeded:
            #         print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
            #     else:
            #         log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)
    ############************Code between these comments is not used anymore as NegPlan Positions is depricated***********####################
            elif subject == 'Background Report Neg Plan Task Report has completed':
                data_area = 'NegPlan Ariba Task'
                table = 'stage.stg_NegPlan_Ariba_Tasks'
                latest_file_path = os.path.join(shared_drive_folder_path, str(change_file_name)) + '.xlsx'
                df = pd.DataFrame()
                df1 = loadExcelFile(file_path=latest_file_path, sheet_name=sheet_names[0], header_row=5)
                # print(df1.columns)
                #
                keep_cols = ['[SPRJ]Project (Project Id)', '[SPRJ]Project (Project Name)',
                       '[SPRJ]Start Date (Date)', '[SPRJ] State', '[SPRJ]Owner (User)',
                       '[PTK]Task Name (Standard Task Name)', '[PTK]Task Name (Task Name)',
                       '[PTK] Type', '[PTK] Status', '[SPRJ]Organization (Department (L1))',
                       '[SPRJ]Organization (Department (L2))',
                       '[SPRJ]Organization (Department)', 'count(SourcingProject)']

                df1 = df1[keep_cols]

                New_cols_nm = ['Project_Id', 'Project_Name',
                       'Begin_Date', 'State', 'Owner',
                       'StandardTaskName', 'TaskName',
                       'Type', 'Status', 'Org_Dept_L1',
                       'Org_Dept_L2',
                       'Org_Dept', 'Count']

                df1.columns = New_cols_nm
                # change the unclassified begin date to null
                df1.replace('Unclassified', np.nan)
                for date_col in ['Begin_Date']:
                    df1[date_col] = pd.to_datetime(df1[date_col], errors='coerce')
                # print(df1.head(5))
                output = map_columns('stage.stg_NegPlan_Ariba_Tasks', df1, New_cols_nm)

                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df1, columns=New_cols_nm, truncate=True,
                                                                driver='{ODBC Driver 17 for SQL Server}')
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df1.shape[0], table))
                else:
                    log(insert_succeeded, project_name=project_name, data_area=data_area, error_msg=error_msg)

