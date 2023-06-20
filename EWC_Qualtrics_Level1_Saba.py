#!/usr/bin/env python
# coding: utf-8

__author__ = "Kayla Guedes"
__email__ = "kayla.guedes@intel.com"
__description__ = "This script loads Qualtrics data and stages it in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from time import time
from Helper_Functions import uploadDFtoSQL, map_columns
from Logging import log

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass

if __name__ == "__main__":
    # Initialize parameters
    start_time = time()
    filename = "GSC L1 Report_initialpull.csv"

    # (1) Read csv file
    df = pd.read_csv(os.path.join(r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\WCS\PPS\Saba L1 Survey Data", filename), header=0, low_memory=False)
    print(df.columns)

    # (2) Fix date columns for SQL insert
    df['Scheduled Class Start Date'] = pd.to_datetime(df['Scheduled Class Start Date'], format='%d-%b-%y')

    # (3) Add LoadDtm to the end of the DataFrame
    df['LoadDate'] = pd.to_datetime('today')

    column_info = map_columns(table='survey.L1_Saba_Survey', df=df)
    for x in column_info.keys():
        print(column_info[x])

    # (4) Load data into database
    insert_succeeded, error_msg = uploadDFtoSQL(table='survey.L1_Saba_Survey', data=df, truncate=True)
    log(insert_succeeded, project_name='Saba L1 Survey Load', data_area='Saba Survey', row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print("Successfully loaded to SQL!")
    else:
        print(error_msg)

    print("--- %s seconds ---" % (time() - start_time))
