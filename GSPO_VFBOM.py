__author__ = "Angela Baltes"
__email__ = "angela.baltes@intel.com"
__description__ = "This script loads VFBOM data into the SQL table and executes stored procedure"
__schedule__ = "6:30 AM, 10:30 AM, 1:30 PM and 4 PM every day"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import imaplib
import smtplib
from openpyxl import load_workbook
import email
import pandas as pd
from pandas import ExcelFile, DataFrame
import numpy as np
import pyodbc
import datetime
import time
from email.message import EmailMessage
import shutil
import filecmp
from Project_params import params
from Helper_Functions import executeStoredProcedure
from Logging import log, log_warning
from Password import accounts


##server = "driver={SQL Server}; server=sql1717-fm1-in.amr.corp.intel.com,3181; database=gsmdw; Trusted_Connection=yes"

def get_file_list(DrivePath):
    # DrivePath = r"\\vmsoapgsmssbi06\VFBOM_Intake\Save_excel_in_this_folder"
    first_list = [os.path.join(root, name) for root, dirs, files in os.walk(DrivePath) for name in files if
                  name.endswith(".xlsx")]
    ArchivePath = DrivePath = r"\\vmsoapgsmssbi06\VFBOM_Intake\Archive"
    archive_list = [os.path.join(root, name) for root, dirs, files in os.walk(ArchivePath) for name in files if
                    name.endswith(".xlsx")]
    file_list = []
    print(archive_list)
    if len(archive_list) > 0:
        for f in first_list:
            x = 0
            # f_stat = os.stat(f)
            compare = f.replace('\\vmsoapgsmssbi06\VFBOM_Intake\Save_excel_in_this_folder\\', '')
            compare = compare.replace('.xlsx', '')
            for item in archive_list:
                pos = item.find(compare)
                # a_stat = os.stat(item)
                if pos != -1:  # if it finds match in archive, check if file is new
                    # if time.asctime(time.localtime(f_stat[8])) > time.asctime(time.localtime(a_stat[8])): #if new file has more recent modified date than the archived file
                    x = 1
                    if filecmp.cmp(f, item):
                        file_list.append(f)
                        pass
                    else:
                        file_list.append(f)
            if x == 0:  # no match in archive, upload
                file_list.append(f)
    else:
        file_list = first_list
    return file_list


def get_file_info(file):
    """ Function to extract who last modified the file, the time it was saved to the share drive and the supplier name on the file

    Args:
        String of the full excel file path
    Returns:
        A list that includes who last modified the file, the time is was saved to the share drive and the supplier name on the file
    """
    # getting file save time
    supplier_name = []
    stat = os.stat(file)
    upload_time = time.asctime(time.localtime(stat[9]))
    # getting file saved by
    wb = load_workbook(file)
    mod_by = wb.properties.lastModifiedBy
    y = 0
    xl = pd.ExcelFile(file)
    # get supplier name
    for x in xl.sheet_names:
        if y == 1:
            pass
        elif x.endswith("_Requests"):  # see all sheet name
            temp = pd.read_excel(file, sheet_name=x, usecols="A:B", nrows=2)
            supplier_name = list(temp.columns.values.tolist())
            supplier_name = supplier_name[0]
            y = 1
    listt = [mod_by, upload_time, supplier_name]
    return listt


def sheet_names(file):
    """ Function to create a dataframe for each sheet in the excel file that ends in "_Requests"
    Args:
    string that is the full excel file path
    Returns:
    A list of dataframes from the single excel sheet passed, often will only contain 1
    """
    listt = []
    xl = pd.ExcelFile(file)
    for x in xl.sheet_names:
        if x.endswith("_Requests"):  # see all sheet names
            listt.append(pd.read_excel(file, sheet_name=x, usecols="A:EA", nrows=9999))
    return listt


def prepareData(file_info, df):
    """Function to perform the necessary data transformations to prepare the data to be loaded to the sql tables
    If it fails it calls the log function with succeed = False
    Args:
    file_info - list, length of 3 returned from the get_file_info() funtion
    df - Dataframe corresponding to one of the sheets that ends in "_Requests" on the specified excel file
    Returns:
    A list of arrays, the dataframe has been transformed to numpy arrays in order to perform more transformations on the data
    """
    # df = pd.read_excel(f_name)
    # df = pd.read_excel(file, sheet_name=sheet)
    rows = df.loc[2:, :]  # getting rid of top 2 rows
    rows = rows.dropna(axis=0, how="all")  # remove completely blank rows
    rows = rows.dropna(axis=1, how="all")  # remove completely blank columns
    columns = rows.loc[2:2, :]  # column names
    columns = pd.DataFrame.to_numpy(columns)  # column names to numpy array
    rows = rows.loc[3:, :]  # get just the data rows
    rows.columns = columns[0]  # reset column names to column list
    try:
        rows = rows.loc[rows['MDA Approved'].notnull()]
    except KeyError:
        print("no MDA")
        return None
    if 'Complete date' in rows.columns:
        rows = rows.drop(columns=['StartAttribute', 'MDA Approved', 'Complete date', 'SLA Adjustment', 'SLA Notes'])
    else:
        rows = rows.drop(columns=['StartAttribute', 'MDA Approved'])
    # make array of the base rows
    change_lines = rows.loc[:, :"AttributeEnd/StartMachine"]
    change_lines = change_lines.drop(columns=["AttributeEnd/StartMachine"])
    # attribute_columns = change_lines.columns #get attribute column names
    change_lines = pd.DataFrame.to_numpy(change_lines)  # switch rows to numpy array
    # crows, ccols = change_lines.shape #get number of rows and columns
    # #maker array of machine types
    machines = rows.loc[:, "AttributeEnd/StartMachine":]  ###should just end at last column, no end indicator
    machines = machines.drop(columns=["AttributeEnd/StartMachine"])
    machines = machines.loc[:, machines.columns.notnull()]
    machine_list = machines.columns  # get machine list
    machines = pd.DataFrame.to_numpy(machines)  # isolate rows from mahcine list
    mrows, mcols = machines.shape
    # replace blanks in array with nan
    for i in range(0, mrows):  # machine row loop
        for j in range(0, mcols):  # machine column loop
            if pd.isnull(machines[i][j]) is False:  # is not null
                # print(machines[i][j]) #print(i, j)
                if len(machines[i][j].strip()) == 0:
                    machines[i][j] = np.nan
                else:
                    machines[i][j] = machines[i][j].strip()
    rows_ready, almost_ready, first_rows, commit_vars = [], [], [], []
    null_value = np.nan
    #####loop through mahcine array, if you find an x, prepare row to be committed to sql
    for i in range(0, mrows):  # machine row loop
        if change_lines[i][1] == 'Change CAT' or change_lines[i][1] == 'Change CAT/MSQ':
            # function that takes in the single CAT change line and returns a set of rows to be appended to first rows
            row = change_lines[i]
            cat_lines = CATChanges(machine_list, row)
            # for each row in the returned set, loop to append to firstrows
            for line in cat_lines:
                first_rows.append(line)
            # print(commit_vars)
        else:
            for j in range(0, mcols):  # machine column loop
                if pd.isnull(machines[i][j]) is False:
                    commit_vars = change_lines[i].tolist()
                    commit_vars.append(machine_list[j])
                    first_rows.append(commit_vars)
    import datetime
    # changing date column type for upload
    # chaning nulls to None for upload
    for i in range(0, len(first_rows)):
        for r in range(0, len(first_rows[i])):
            if type(first_rows[i][r]) is datetime.datetime:
                first_rows[i][r] = first_rows[i][r].strftime('%m/%d/%Y')
            elif pd.isnull(first_rows[i][r]) is True or first_rows[i][r] == '?':
                first_rows[i][r] = None
                # print(commit_vars)
        temp = file_info + first_rows[i]
        almost_ready.append(temp)
    # creating column key_row
    for i in range(0, len(almost_ready)):
        key = ""
        for x in range(2, 33):
            if almost_ready[i][x] is not None and x != 3 and x != 5 and x != 6:  # not validity, status, or notes
                key = key + str(almost_ready[i][x])
        almost_ready[i].append(key)
        # creating column key_A
    for i in range(0, len(almost_ready)):
        key = ""
        for x in range(9, 35):
            if almost_ready[i][x] is not None and (x == 14 or x == 15 or x == 34):  # OEM SPN and IPN, and machine type
                key = key + str(almost_ready[i][x])
        almost_ready[i].append(key)
    for i in range(0, len(almost_ready)):
        if type(almost_ready[i][6]) is not type(None):
            pos = almost_ready[i][6].find("CEID: ")
            if pos != -1:  # if CEID: was found
                notes = almost_ready[i][6]
                pos = pos + 6  ##CEID: is length 6
                first_part = notes[0:pos]  ##need to get entire string up to and including the space after "CEID:"
                second_part = notes[pos + 1:]
                ceids = second_part.split(", ")
                # loop to commit each row with the different ceid
                for item in ceids:
                    almost_ready[i][6] = first_part + item
                    rows_ready.append(almost_ready[i])
            else:
                rows_ready.append(almost_ready[i])
        else:
            rows_ready.append(almost_ready[i])
    return rows_ready


def upload2SQL(rows_ready, file):
    """Function to upload the transformed rows to the correct sql table
    If it fails it calls the log function with succeed = False
    Args:
    server - string of the entire input for the pyodbc.connect() function
    rows_ready - list of numpy arrays from the PrepareData funtion
    Returns:None
    """
    # connect to database
    # conn = pyodbc.connect(server)
    # Connect via pyodbc
    conn = pyodbc.connect(driver=params['SQL_DRIVER'], server=params['GSMDW_SERVER'], database=params['GSMDW_DB'],
                          Trusted_Connection='yes')
    cursor = conn.cursor()
    cursor.fast_executemany = True
    now = datetime.datetime.now()
    now = now.strftime("%m/%d/%Y, %H:%M:%S")
    # group into 3 df for 3 tables
    rows_Complete, rows_Open, rows_notValid = [], [], []
    for row in rows_ready:
        if row[3] is not None and row[5] is not None:
            row.append(now)
            if row[5] == "Complete" or row[5] == "Cancelled" or row[5] == "complete":
                rows_Complete.append(row)
            elif row[3][0:5] != "Valid" or row[5] == "See Notes":
                rows_notValid.append(row)
            else:
                rows_Open.append(row)
    df_Complete = DataFrame(rows_Complete,
                            columns=['file_saved_by', 'file_save_date', 'supplier', 'validity', 'req_type', 'status',
                                     'Notes', 'req_date', 'requestor', 'approver', 'ECC_WP', 'deplete_ind',
                                     'parts_family_ind', 'oem_supplier', 'oem_SPN', 'oem_IPN', 'oem_part_desc',
                                     'old_cat', 'old_msq', 'new_cat', 'new_msq', 'altRpr_supplier', 'altRpr_SPN',
                                     'altRpr_IPN', 'altRpr_desc', 'tool_killer', 'supplier_rpr_ind', 'fb_lead_time',
                                     'rpr_lead_time', 'uom', 'consumable_ind', 'PM_item_ind', 'suplrQty_pertool',
                                     'process', 'machine_type', 'complete_key', 'notvalid_key', 'logged_SQL'])

    df_Complete = df_Complete.astype(str)
    df_Open = DataFrame(rows_Open,
                        columns=['file_saved_by', 'file_save_date', 'supplier', 'validity', 'req_type', 'status',
                                 'Notes', 'req_date', 'requestor', 'approver', 'ECC_WP', 'deplete_ind',
                                 'parts_family_ind', 'oem_supplier', 'oem_SPN', 'oem_IPN', 'oem_part_desc', 'old_cat',
                                 'old_msq', 'new_cat', 'new_msq', 'altRpr_supplier', 'altRpr_SPN', 'altRpr_IPN',
                                 'altRpr_desc', 'tool_killer', 'supplier_rpr_ind', 'fb_lead_time', 'rpr_lead_time',
                                 'uom', 'consumable_ind', 'PM_item_ind', 'suplrQty_pertool', 'process', 'machine_type',
                                 'complete_key', 'notvalid_key', 'logged_SQL'])
    df_Open = df_Open.astype(str)
    df_notValid = DataFrame(rows_notValid,
                            columns=['file_saved_by', 'file_save_date', 'supplier', 'validity', 'req_type', 'status',
                                     'Notes', 'req_date', 'requestor', 'approver', 'ECC_WP', 'deplete_ind',
                                     'parts_family_ind', 'oem_supplier', 'oem_SPN', 'oem_IPN', 'oem_part_desc',
                                     'old_cat', 'old_msq', 'new_cat', 'new_msq', 'altRpr_supplier', 'altRpr_SPN',
                                     'altRpr_IPN', 'altRpr_desc', 'tool_killer', 'supplier_rpr_ind', 'fb_lead_time',
                                     'rpr_lead_time', 'uom', 'consumable_ind', 'PM_item_ind', 'suplrQty_pertool',
                                     'process', 'machine_type', 'complete_key', 'notvalid_key', 'logged_SQL'])
    df_notValid = df_notValid.astype(str)
    ##commit complete rows
    try:
        if len(rows_Complete) > 0:
            print(len(rows_Complete))
            cursor.executemany(
                "INSERT INTO [bom].[VFBOMIntake_Complete] (file_saved_by, file_save_date, supplier, validity, req_type, status, Notes, req_date, requestor, approver, ECC_WP, deplete_ind, \
                parts_family_ind, oem_supplier, oem_SPN, oem_IPN, oem_part_desc, old_cat, old_msq, new_cat, new_msq, altRpr_supplier, altRpr_SPN, altRpr_IPN, altRpr_desc, tool_killer,\
                supplier_rpr_ind, fb_lead_time, rpr_lead_time, uom, consumable_ind, PM_item_ind, suplrQty_pertool, process, machine_type, complete_key, notvalid_key, logged_SQL) \
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(df_Complete.itertuples(index=False, name=None))
            )
            conn.commit()
        success = True
        error_msg = None
    except (pyodbc.DataError, pyodbc.Error, pyodbc.ProgrammingError, pyodbc.OperationalError) as error:
        conn.rollback()
        success = False
        error_msg = error
        sendErrorEmail(file, "Complete", error_msg)
    log(success, project_name="VFBOM_IntakeScript", package_name="VFBOM_Intake.py",
        data_area='VFBOM Change Requests - Complete', row_count=len(rows_Complete),
        error_msg=error_msg)  # row_count is automatically set to 0 if error
    #
    ##commit complete rows
    try:
        if len(rows_Open) > 0:
            print(len(rows_Open))
            cursor.executemany(
                "INSERT INTO [bom].[VFBOMIntake_Open] (file_saved_by, file_save_date, supplier, validity, req_type, status, Notes, req_date, requestor, approver, ECC_WP, deplete_ind, \
                parts_family_ind, oem_supplier, oem_SPN, oem_IPN, oem_part_desc, old_cat, old_msq, new_cat, new_msq, altRpr_supplier, altRpr_SPN, altRpr_IPN, altRpr_desc, tool_killer,\
                supplier_rpr_ind, fb_lead_time, rpr_lead_time, uom, consumable_ind, PM_item_ind, suplrQty_pertool, process, machine_type, complete_key, notvalid_key, logged_SQL) \
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(df_Open.itertuples(index=False, name=None))
            )
            conn.commit()
        success = True
        error_msg = None
    except pyodbc.DataError as error:
        conn.rollback()
        success = False
        error_msg = error
        sendErrorEmail(file, "Open", error_msg)
    log(success, project_name="VFBOM_IntakeScript", package_name="VFBOM_Intake.py",
        data_area='VFBOM Change Requests - Open', row_count=len(rows_Open),
        error_msg=error_msg)  # row_count is automatically set to 0 if error
    #
    ##commit notValid rows
    try:
        if len(rows_notValid) > 0:
            print(len(rows_notValid))
            cursor.executemany(
                "INSERT INTO [bom].[VFBOMIntake_notValid] (file_saved_by, file_save_date, supplier, validity, req_type, status, Notes, req_date, requestor, approver, ECC_WP, deplete_ind, \
                parts_family_ind, oem_supplier, oem_SPN, oem_IPN, oem_part_desc, old_cat, old_msq, new_cat, new_msq, altRpr_supplier, altRpr_SPN, altRpr_IPN, altRpr_desc, tool_killer,\
                supplier_rpr_ind, fb_lead_time, rpr_lead_time, uom, consumable_ind, PM_item_ind, suplrQty_pertool, process, machine_type, complete_key, notvalid_key, logged_SQL) \
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(df_notValid.itertuples(index=False, name=None))
            )
        conn.commit()
        success = True
        error_msg = None
    except pyodbc.DataError as error:
        conn.rollback()
        success = False
        error_msg = error
        sendErrorEmail(file, "notValid", error_msg)
    log(success, project_name="VFBOM_IntakeScript", package_name="VFBOM_Intake.py",
        data_area='VFBOM Change Requests - notValid', row_count=len(rows_notValid),
        error_msg=error_msg)  # row_count is automatically set to 0 if error
    conn.close()


def copy_file_archive(file):
    """Function to move the file to the archive folder and append the name with the date and time it was archived
    Args:
    file - String of the full excel file path
    Returns:
    None
    """
    # now = datetime.datetime.now()
    # now = now.strftime("%m_%d_%Y_%H%M%S")
    print(file)
    file_og = file.replace('\\\\vmsoapgsmssbi06\VFBOM_Intake\Save_excel_in_this_folder\\', '')
    file_new = file_og.replace('.xlsx', '')
    file_new = file_new + "_archived" + ".xlsx"
    print(file_og)
    print(file_new)
    shutil.copy(os.path.join(r'\\vmsoapgsmssbi06\VFBOM_Intake\Save_excel_in_this_folder', file_og),
                os.path.join(r'\\vmsoapgsmssbi06\VFBOM_Intake\Archive', file_new))


def remove_dups():  # only removes exact duplicates (besides upload time and uploaded by) from the Complete table
    """Function to delete any duplicates that may exist in the "Complete" table, a duplicate is a two rows with every column the same except the file save time, file save by, and logged sql time
    The earliest of the dups is kept based on the 'logged_SQL' column in order to know the first date it was completed, not the most recent
    Args:
    None
    Returns:
    None
    """
    conn = pyodbc.connect(driver=params['SQL_DRIVER'], server=params['GSMDW_SERVER'], database=params['GSMDW_DB'],
                          Trusted_Connection='yes')
    cursor = conn.cursor()
    sql = "DELETE V2 FROM [bom].[VFBOMIntake_Complete] V1, [bom].[VFBOMIntake_Complete] V2 WHERE V1.[logged_SQL] < V2.[logged_SQL] AND V1.[complete_key] = V2.[complete_key]"
    x = "[supplier] = V2.[supplier] AND V1.[validity] = V2.[validity] AND V1.[req_type] = V2.[req_type] \
    AND V1.[status] = V2.[status] AND V1.[Notes] = V2.[Notes] AND V1.[req_date] = V2.[req_date] AND V1.[requestor] = V2.[requestor] AND V1.[approver] = V2.[approver] \
    AND V1.[oem_supplier] = V2.[oem_supplier] AND V1.[oem_SPN] = V2.[oem_SPN] AND V1.[oem_IPN] = V2.[oem_IPN] AND V1.[oem_part_desc] = V2.[oem_part_desc] AND V1.[old_cat] = V2.[old_cat] AND V1.[old_msq] = V2.[old_msq] \
    AND V1.[new_cat] = V2.[new_cat] AND V1.[new_msq] = V2.[new_msq] AND V1.[rpr_supplier] = V2.[rpr_supplier] AND V1.[machine_type] = V2.[machine_type] AND V1.[altRpr_SPN] = V2.[altRpr_SPN] AND V1.[altRpr_IPN] = V2.[altRpr_IPN] \
    AND V1.[altRpr_desc] = V2.[altRpr_desc] AND V1.[tool_killer] = V2.[tool_killer] AND V1.[supplier_rpr_ind] = V2.[supplier_rpr_ind] AND V1.[fb_lead_time] = V2.[fb_lead_time] AND V1.[rpr_lead_time] = V2.[rpr_lead_time] \
    AND V1.[uom] = V2.[uom] AND V1.[consumable_ind] = V2.[consumable_ind] AND V1.[PM_item_ind] = V2.[PM_item_ind] AND V1.[suplrQty_pertool] = V2.[suplrQty_pertool]"
    cursor.execute(sql)
    # delete_success, error_msg = executeSQL(sql)  # executeSQL is a function in Helper_Functions that does all the cursor initialization for you (and switches between dev and prod...)
    # if not delete_success:  # only log failures of the delete
    #     log(delete_success, project_name="VFBOM_IntakeScript", package_name="VFBOM_Intake.py", data_area="VFBOM Change Requests", row_count=0, error_msg=error_msg)
    # else:
    conn.commit()
    print("Succesfull commit")
    conn.close()


def CATChanges(machine_list, row):
    CATlines = []
    row = row.tolist()
    if str(row[30]) == "1272":
        for machine in machine_list:
            if machine.find("1272") != (-1) and machine.find("C4") == (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1272 C4":
        for machine in machine_list:
            if machine.find("1272") != (-1) and machine.find("C4") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1272":
        for machine in machine_list:
            if machine.find("1272") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227":
        for machine in machine_list:
            if machine.find("1227") != (-1) and machine.find("C4") == (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227 C4":
        for machine in machine_list:
            if machine.find("1227") != (-1) and machine.find("C4") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1227":
        for machine in machine_list:
            if machine.find("1227") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1274":
        for machine in machine_list:
            if machine.find("1274") != (-1) and machine.find("C4") == (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1274 C4":
        for machine in machine_list:
            if machine.find("1274") != (-1) and machine.find("C4") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1274":
        for machine in machine_list:
            if machine.find("1274") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1276":
        for machine in machine_list:
            if machine.find("1276") != (-1) and machine.find("C4") == (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1276 C4":
        for machine in machine_list:
            if machine.find("1276") != (-1) and machine.find("C4") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1276":
        for machine in machine_list:
            if machine.find("1276") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227 & 1272":
        for machine in machine_list:
            if machine.find("C4") == (-1) and (machine.find("1272") != (-1) or machine.find("1227") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227 C4 & 1272 C4":
        for machine in machine_list:
            if machine.find("C4") != (-1) and (machine.find("1272") != (-1) or machine.find("1227") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1227 & All 1272":
        for machine in machine_list:
            if machine.find("1272") != (-1) or machine.find("1227") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1272 & 1274":
        for machine in machine_list:
            if machine.find("C4") == (-1) and (machine.find("1272") != (-1) or machine.find("1274") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1272 C4 & 1274 C4":
        for machine in machine_list:
            if machine.find("C4") != (-1) and (machine.find("1272") != (-1) or machine.find("1274") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1272 & All 1274":
        for machine in machine_list:
            if machine.find("1272") != (-1) or machine.find("1274") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1274 & 1276":
        for machine in machine_list:
            if machine.find("C4") == (-1) and (machine.find("1274") != (-1) or machine.find("1276") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1274 C4 & 1276 C4":
        for machine in machine_list:
            if machine.find("C4") != (-1) and (machine.find("1274") != (-1) or machine.find("1276") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1274 & All 1276":
        for machine in machine_list:
            if machine.find("1274") != (-1) or machine.find("1276") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227 & 1274 & 1276":
        for machine in machine_list:
            if machine.find("C4") == (-1) and (
                    machine.find("1274") != (-1) or machine.find("1276") != (-1) or machine.find("1227") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "1227 C4 & 1274 C4 & 1276 C4":
        for machine in machine_list:
            if machine.find("C4") != (-1) and (
                    machine.find("1274") != (-1) or machine.find("1276") != (-1) or machine.find("1227") != (-1)):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All 1227 & All 1274 & All 1276":
        for machine in machine_list:
            if machine.find("1274") != (-1) or machine.find("1276") != (-1) or machine.find("1227") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All C4":
        for machine in machine_list:
            if machine.find("C4") != (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All Non-C4":
        for machine in machine_list:
            if machine.find("C4") == (-1):
                # append to row then apend row to CATlines
                line = row.copy()
                line.append(machine)
                CATlines.append(line)
    elif str(row[30]) == "All Processes":
        for machine in machine_list:
            line = row.copy()
            line.append(machine)
            CATlines.append(line)
    return CATlines


def sendErrorEmail(file_name, data_area, error_msg):
    """Function to email support users that the job has failed.
    Args:
    file_name: Name of file that failed
    data_area: Affected area.
    error_msg: Error message from Python.
    Returns:
    None.
    """
    now = datetime.datetime.now()
    time_now = now.strftime("%m/%d/%Y, %H:%M:%S")
    msg = EmailMessage()
    msg.set_content(
        'The Python Script "VFBOM_Intake.py" failed on {0}, loading rows into the {1} table on {2}.\n\nThe generated error message is as follows:\n\t{3}'.format(
            file_name, data_area, time_now, error_msg))
    msg['Subject'] = 'Python VFBOM_Intake Data Loader Failed' + file_name
    EMAIL_RECEIVERS = ['khushboo.saboo@intel.com', 'bryant.luy@intel.com','luis.calvo.guzman@intel.com', 'maria.sanabria.castillo@intel.com', 'maria.avellan.zumbado@intel.com']
    # Send the message via the Intel SMTP server.
    s = smtplib.SMTP('smtpauth.intel.com', 587)
    s.connect('smtpauth.intel.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(accounts['GSM Support'].username, accounts['GSM Support'].password)
    s.sendmail(accounts['GSM Support'].username, EMAIL_RECEIVERS, msg.as_string())
    s.quit()

if __name__ == "__main__":
    DrivePath = r"\\vmsoapgsmssbi06\VFBOM_Intake\Save_excel_in_this_folder"
    print(DrivePath)
    file_list = get_file_list(DrivePath)
    # if no files log no files
    if len(file_list) == 0:
        log_warning(project_name="VFBOM_IntakeScript", package_name="VFBOM_Intake.py", data_area="VFBOM Change Requests", file_path=DrivePath, warning_type="Missing")

    else:
        for file in file_list:
            print(file)
            ##if error opening file, skip the file and send email
            try:
                file_info = get_file_info(file)
            except:
                sendErrorEmail(file, "All tables", "Failed to open the file", )
                print("Couldn't open file")
                continue
            sheets = sheet_names(file)
            for df in sheets:
                try:
                    rows_ready = prepareData(file_info, df)
                except KeyError or IndexError or TypeError as error:
                    print("Couldn't transform data")
                    error_msg = error
                    sendErrorEmail(file, "All Tables", error_msg)
                    continue
                if rows_ready == None:
                    print("no MDA column")
                    sendErrorEmail(file, "All tables", "no MDA column", )
                    pass
                else:
                    upload2SQL(rows_ready, file)
            copy_file_archive(file)
        remove_dups()

#Execute stored procedure to populate VFBOMCombinedOutput table
proc_name = 'bom.VFBOMCalculations'
execute_succeeded, error_msg = executeStoredProcedure('bom.VFBOMCalculations')
if execute_succeeded:
    print('Successfully executed the bom.VFBOMCalculations stored procedure')
else:
    print(error_msg)