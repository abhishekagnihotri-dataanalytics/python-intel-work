#!/usr/bin/env python
# coding: utf-8

__author__ = "Kayla Guedes"
__email__ = "kayla.guedes@intel.com"
__description__ = "This script loads Qualtrics data and stages it in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 1:45 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import requests
import zipfile
import io
import pandas as pd
import os
import shutil
import stat
import errno
from time import time
from Helper_Functions import uploadDFtoSQL
from Logging import log
from Project_params import params
from Password import decrypt_password

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def handleRemoveReadonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO) # 0777
        # func(path)
    else:
        raise


if __name__ == "__main__":
    # Initialize parameters
    start_time = time()
    project_name = 'Qualtrics Survey Load'
    data_area = 'Consolidated Survey'
    apiToken = decrypt_password(b'gAAAAABjaZWFPd57rSP3W2t9v8b_M77X_TYseV1AigNYtl6ve97_fbmX30RUrdPSP0HVVvOwUP25whILxo1f3tR5gws5HwkUfuuh8xyLrtp4oVj0p6OjEMthMhw90SGjYWRmdb3xTIam')  # Pat Nevlin's <pat.a.nevlin@intel.com> Token
    surveyId = "SV_cIwxQvBnA5ZNSol"  # "SV_9S4VecFb5Q4CYDz"
    dataCenter = "az1"
    filename = "Consolidated GSC Survey.csv"
    fileFormat = "csv"

    ### Get Survey metadata from Qualtrics API ###
    baseUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}".format(dataCenter, surveyId)  # Reference API Docs: https://api.qualtrics.com/ZG9jOjg3NzY3Mw-managing-surveys
    response = requests.get(baseUrl, headers={"x-api-token": apiToken}, proxies={'http': 'http://proxy-dmz.intel.com:911'})
    frmt_resp = response.json()
    # print(frmt_resp)

    ### Get Survey Responses from Qualtrics API ###
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    downloadRequestUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter, surveyId)  # Reference API Docs: https://api.qualtrics.com/6b00592b9c013-start-response-export
    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken,
    }

    # Step 1: Creating Data Export
    downloadRequestPayload = '{"format":"' + fileFormat + '"}'
    downloadRequestResponse = requests.post(downloadRequestUrl, data=downloadRequestPayload, headers=headers, proxies={'http': 'http://proxy-dmz.intel.com:911'})
    if downloadRequestResponse.status_code == 200:
        progressId = downloadRequestResponse.json()["result"]["progressId"]
    else:
        error_msg = str(downloadRequestResponse.status_code)
        try:
            error_msg = error_msg + " - Error message: " + downloadRequestResponse.json()['meta']['error']['errorMessage']
        except KeyError:  # API response does not contain an error message
            pass
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
        raise Exception("Unable to connect to Qualtrics Survey")
    # print(downloadRequestResponse.text)

    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        # print("progressStatus=", progressStatus)
        requestCheckUrl = downloadRequestUrl + progressId
        requestCheckResponse = requests.get(requestCheckUrl, headers=headers)
        try:
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            # print("Download is " + str(requestCheckProgress) + " complete")
            progressStatus = requestCheckResponse.json()["result"]["status"]
        except KeyError as error_msg:   # API response does not contain "percentComplete" or "status"
            log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
            raise Exception("Qualtrics Survey export failed")

    # step 2.1: Check for error
    if progressStatus == "failed":
        log(False, project_name=project_name, data_area=data_area, error_msg="Failed to export Qualtrics Survey from API")
        raise Exception("Qualtrics Survey export failed")
    else:
        fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file
    requestDownloadUrl = downloadRequestUrl + fileId + '/file'
    requestDownload = requests.get(requestDownloadUrl, headers=headers, stream=True)

    # Step 4: Unzipping the file, checks if it's there, deletes the file and then unzips it
    try:
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")  # This creates a new folder called MyQualtricsDownload in the current working folder
    except PermissionError:
        shutil.rmtree("MyQualtricsDownload", ignore_errors=False, onerror=handleRemoveReadonly)
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")
    print('Complete')

    ### Format data for SQL using Python ###
    # Read csv file into DataFrame
    df = pd.read_csv("./myqualtricsdownload/" + filename, header=[0, 1], low_memory=False)
    # print(df.columns)

    # make a list of new headers, if column 0 = 1, then set it as column 0. If not =, then set as concatenation as them.
    new_cols = []
    for x in df.columns:
        # print(x[0])
        # print(x[1])
        if not x[0].startswith('Q'):
            new_cols.append(x[1])
        #    elif x[0].startswith('Q7'):
        #        new_cols.append(x[1])
        else:
            new_cols.append(x[0] + " " + x[1])
    df.columns = new_cols
    # print(df.columns)

    df = df.iloc[1:]  # delete top row
    df.reset_index()  # reset index
    # print(df.head(10))

    #### SURVEY KEY ####
    #
    # MC Questions:
    # Q1, Q1c, Q1d, Q1e, Q1f, Q1h, Q1i, Q1j, Q1k, Q1l, Q1o
    #
    # Reference Block MC Questions:
    # Q1
    #
    # Agreement Scale:
    # Q1a, Q1b, Q1g, Q1m, Q1n
    #
    # Reference Block Agreement Scale:
    # Q2
    #
    # Reference Block Free Text Questions:
    # Q1a, Q1b, Q3
    #
    #### END SURVEY KEY ####

    # Dynamically create mappings for multiple choice questions
    mc_text = dict()
    for qid in frmt_resp['result']['questions'].keys():
        question_name = frmt_resp['result']['questions'][qid]['questionName']
        # if frmt_resp['result']['qeustions'][qid]['questionName'] in mc_questions:  # question is multiple choice
        if frmt_resp['result']['questions'][qid]['questionType']['type'] == "MC":
            mc_text[question_name] = dict()
            for possible_answer in frmt_resp['result']['questions'][qid]['choices']:
                mc_text[question_name][possible_answer] = frmt_resp['result']['questions'][qid]['choices'][possible_answer]['choiceText']
                # print(possible_answer)
    # print(mc_text)

    # Mappings for open ended questions
    open_text = dict()
    for qid in frmt_resp['result']['questions'].keys():
        if frmt_resp['result']['questions'][qid]['questionType']['type'] == "TE":
            open_text[question_name] = dict()

    # Remove extra columns that will not be stored in SQL database
    df.drop(['Start Date', 'End Date', 'Response Type', 'IP Address', 'Progress', 'Duration (in seconds)', 'Finished',
             'Recipient Last Name', 'Recipient First Name', 'Recipient Email', 'External Data Reference',
             'Location Latitude', 'Location Longitude', 'Distribution Channel', 'User Language'], axis=1, inplace=True)
    # print(df.columns)

    # Create lists of question types
    multiple_choice_columns = []
    agreement_columns = []
    open_ended_columns = []
    for col in df.columns:
        question_number = col.split(' ')[0]
        #     print(question_number)
        if question_number in mc_text.keys():  # if question is in list of all questions that are multiple choice
            multiple_choice_columns.append(col)
        elif question_number.startswith('Q1a'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q1b'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q1g'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q1m'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q1n'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q3'):  # if question is a agreement scale
            agreement_columns.append(col)
    # print(multiple_choice_columns)

    # Mappings for agreement
    agree_text = {'1': 'Strongly Agree', '2': 'Somewhat Agree', '3': 'Neither agree nor disagree', '4': 'Somewhat Disagree', '5': 'Strongly Disagree'}

    for index, row in df.iterrows():
        for col in multiple_choice_columns:
            if not pd.isna(row.loc[col]):  # since they forgot to validate everything... ignore blank answers
                df.loc[index, col] = mc_text[col.split(' ')[0]][row.loc[col]]

        df.loc[index] = row
        for col in agreement_columns:
            if not pd.isna(row.loc[col]):  # ignore blank answers
                df.loc[index, col] = agree_text[row.loc[col]]

    for index, row in df.iterrows():
        for col in open_ended_columns:
            if not pd.isna(row.loc[col]):
                if col.startswith('Q1a'):
                    df.loc[index, col] = row.loc['Using the skills you acquired in this course, what business results, including monetary impact, have you achieved?  (do not include personal information; required response)']
                elif col.startswith('Q1b'):
                    df.loc[index, col] = row.loc['What obstacles have impeded you from applying the skills you gained in this course? (do not include personal information; required response)']
                elif col.startswith('Q3'):
                    df.loc[index, col] = row.loc['What would you change to this course to make it more relevant to your job? (do not include personal information; optional response)']
    # print(df.columns)

    df.to_csv("./myqualtricsdownload/Formatted_Output_Phase2.csv", index=False)

    # add database standards columns to end of DataFrame
    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

    # Insert DataFrame into SQL database
    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_GSC_L3'], data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_GSC_L3']))
    else:
        print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
