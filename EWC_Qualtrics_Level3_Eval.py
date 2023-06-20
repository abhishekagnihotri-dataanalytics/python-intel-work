#!/usr/bin/env python
# coding: utf-8

__author__ = "Kayla Guedes"
__email__ = "kayla.guedes@intel.com"
__description__ = "This script loads Qualtrics data and stages it in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 3:30 PM PST"

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
    data_area = 'L3 Assessment Survey'
    apiToken = decrypt_password(b'gAAAAABizbGQnSPFOIfHEiWq-SPlC6dVNnyztchxExGoRs6CyFrJmZxIMcbkT-vc5o-FBvh8MSr48VeRWGhT231M4VTaLqxRaMAXv0zeHRshNxFPZUvMIuT9VpzccNKzpOHL60zj_QmK')
    surveyId = "SV_9S4VecFb5Q4CYDz"  # "SV_9S4VecFb5Q4CYDz"
    dataCenter = "az1"
    filename = "Assembly Process Engineering L3 Assessment.csv"
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
        except KeyError as error_msg:  # API response does not contain "percentComplete" or "status"
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
    # print('File downloaded to MyQualtricsDownload')

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
    #  Q1, Q2, Q3, Q3a, Q3b
    #
    # Other Fields:
    #  Q1, Q2, Q3, Q3a, Q3b, Q6, Q7
    #
    # Free Response (delete):
    #  Q5, Q6a, Q8, Q9
    #
    # Choose Many:
    #  Q6, Q7
    #
    # Agreement Scale:
    #  Q4
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
    del mc_text['Q6']  # Remove Q6 from multiple choice list
    del mc_text['Q7']  # Remove Q7 from multiple choice list
    # print(mc_text)

    # Remove extra columns that will not be stored in SQL database
    df.drop(['Start Date', 'End Date', 'Response Type', 'IP Address', 'Duration (in seconds)', 'Finished',
             'Recipient Last Name', 'Recipient First Name', 'Recipient Email', 'External Data Reference',
             'Location Latitude', 'Location Longitude', 'Distribution Channel', 'User Language', 'Q7 - Topics Q7 - Topics',
             'Q7 - Parent Topics Q7 - Parent Topics'], axis=1, inplace=True)

    # Create lists of question types
    multiple_choice_columns = []
    agreement_columns = []
    select_many_columns = []
    for col in df.columns:
        question_number = col.split(' ')[0]
        #     print(question_number)
        if question_number in mc_text.keys():  # if question is in list of all questions that are multiple choice
            multiple_choice_columns.append(col)
        elif question_number.startswith('Q4'):  # if question is a agreement scale
            agreement_columns.append(col)
        elif question_number.startswith('Q6'):  # if question is select many options
            select_many_columns.append(col)
        elif question_number.startswith('Q7'):  # if question is select many options
            select_many_columns.append(col)
    # print(multiple_choice_columns)

    # Mappings for agreement
    agree_text = {'1': 'Strongly Agree', '2': 'Agree', '3': 'Neutral', '4': 'Disagree', '5': 'Strongly Disagree'}

    # Fill columns for text based questions with matching TEXT column
    for index, row in df.iterrows():
        for col in multiple_choice_columns:
            if not pd.isna(row.loc[col]):  # since they forgot to validate everything... ignore blank answers
                if col.startswith('Q1') and row.loc[col] == "4":  # if user chose other, then use user entered text as value
                    df.loc[index, col] = row.loc['Q1_4_TEXT How did you take the class? - Other (please specify) - Text']
                elif col.startswith('Q2') and row.loc[col] == "6":
                    df.loc[index, col] = row.loc[
                        'Q2_6_TEXT Since taking this course, were you able to utilize the Intel programs you learned about to help i... - Other (please specify) - Text']
                elif col.startswith('Q3a') and row.loc[col] == "14":
                    df.loc[index, col] = row.loc['Q3a_14_TEXT My module is: - Other (please specify) - Text']
                elif col.startswith('Q3b') and row.loc[col] == "7":
                    df.loc[index, col] = row.loc['Q3b_7_TEXT My module is: - Other - Text']
                elif col.startswith('Q3') and row.loc[col] == "3":
                    df.loc[index, col] = row.loc['Q3_3_TEXT My area is: - Other - Text']
                else:
                    try:
                        df.loc[index, col] = mc_text[col.split(' ')[0]][row.loc[col]]
                    except KeyError:
                        df.loc[index, col] = 'Old'

        df.loc[index] = row
        for col in agreement_columns:
            if not pd.isna(row.loc[col]):  # ignore blank answers
                df.loc[index, col] = agree_text[row.loc[col]]
        for col in select_many_columns:
            if not pd.isna(row.loc[col]):  # ignore blank answers
                if row.loc[col] == '1':
                    df.loc[index, col] = 'Yes'
        df.loc[index, 'Q7_6 How do you use the published course\nmaterials? (Select all that apply) - Selected Choice - Other (Please be specific)'] = row.loc['Q7_6_TEXT How do you use the published course\nmaterials? (Select all that apply) - Other (Please be specific) - Text']
        df.loc[index, 'Q6_10 Were there any barriers that prevented you from applying what you learned? (If yes, select all that apply) - Selected Choice - Other (please specify).'] = row.loc['Q6_10_TEXT Were there any barriers that prevented you from applying what you learned? (If yes, select all that apply) - Other (please specify). - Text']

    # Remove all columns with words 'TEXT' or 'How' in name
    for col in df.columns:
        if 'TEXT' in col or col.startswith('How'):  # or col.startswith('Q6a') or col.startswith('Q5') or col.startswith('Q8') or col.startswith('Q9'):
            df.drop([col], axis=1, inplace=True)
    # print(df.head(10))

    # Filter only survey responses greater than June 1 2020
    start_date = '2020-06-01'  # start date of new survey responses
    df = df.loc[df['Recorded Date'] >= start_date]  # filter for new survey responses
    # print(df)
    # print(df.columns)

    df.to_csv("./myqualtricsdownload/Formatted_Output.csv", index=False)

    # add database standards columns to end of DataFrame
    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

    # Insert DataFrame into SQL database
    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_Qualtrics_L3'], data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_Qualtrics_L3']))
    else:
        print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
