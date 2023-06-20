__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SSC_MarketIntelligence tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import shutil
import numpy as np
import pandas as pd
from datetime import datetime, date
from time import time
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, executeStoredProcedure
from Logging import log, log_warning


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


class Metric:
    def __init__(self, metric_name):
        self.table = params['Table_MI_' + metric_name]
        self.package_name = os.path.basename(sys.argv[0])
        self.data_area = 'Load_' + metric_name
        self.files = list()
        self.dataframes = list()
        self.hasData = False

    def addFile(self, file, df):
        self.files.append(file)
        self.dataframes.append(df)
        self.hasData = True
    
    def containsData(self):
        if self.hasData:
            return True
        else:
            return False


def convertDensity(density_str: str) -> float:
    """Function to consistently parse density in gigabytes.

    Args:
        density_str: [str] Density and unit.

    Returns:
        [float] Density.

    """
    if 'GB' in density_str:  # Gigabyte
        density = float(density_str.split('GB')[0])
    elif 'Gb' in density_str:  # Gigabit
        density = float(density_str.split('Gb')[0]) / 8.0
    elif 'MB' in density_str:  # Megabyte
        density = float(density_str.split('MB')[0]) / 1000.0
    elif 'Mb' in density_str:  # Megabit
        density = float(density_str.split('Mb')[0]) / 8000.0
    elif "KB" in density_str:  # Kilobyte
        density = float(density_str.split('KB')[0]) / 1000000.0
    else:  # unable to parse density from table
        density = np.nan
    return density


def parseDate(file_str: str, source: str) -> date:
    """Function to parse date from Excel file name as Upload Date.

    Args:
        file_str: [str] File name.
        source: [str] Either 'DeDIOS', 'DX', or 'Yole'.

    Returns:
        [datetime] Upload Date.

    """
    upload_dt = date.today()
    if source is 'DeDIOS':
        temp = file_str.split()[-1]
        year, month, day, _ = temp.split('.')
        if len(str(month)) < 2:
            month = '0' + str(month)
        if len(str(day)) < 2:
            day = '0' + str(day)
        date_str = month + ' ' + day + ' ' + year
        upload_dt = datetime.strptime(date_str, "%m %d %Y")
    elif source is 'Yole':
        temp = file_str.split()
        month = temp[-2]
        year = temp[-1].split('.')[0]
        date_str = month + ' ' + year
        upload_dt = datetime.strptime(date_str, "%B %Y")
    else:  # source is 'DX'
        temp = file_str.split()
        if len(temp[-2]) > 3:
            month = temp[-2].split('.')[0]
        else:
            month = temp[-2]
        year = temp[-1].split('.')[0]
        date_str = month + ' ' + year
        upload_dt = datetime.strptime(date_str, "%b %Y")

    # print(upload_dt)
    return upload_dt


def formatDataFrames(ordered_columns, df_list):
    """Function to combine multiple dataframes into single dataframe and order columns to match staging table.

    Args:
        ordered_columns: [list of str] Ordered list of pandas Dataframe column names to match staging table.
        df_list: [list of pandas DataFrames] Loaded Excel sheets as pandas Dataframes.

    Returns:
        [pandas DataFrame] One single dataframe formatted to match staging table.

    """
    if len(df_list) > 1:  # at least two of the excel files were loaded
        df = pd.concat(df_list, ignore_index=True, sort=False)  # combine dataframes into a single dataframe
    else:  # only one excel file was loaded
        df = df_list[0]

    for column in ordered_columns:  # iterate through all expected columns
        if column not in df.columns:  # if expected columns are not already in the dataframe
            df[column] = np.nan  # append columns to df so that it resembles the entire dataframe

    df = df.where(pd.notnull(df),
                  None)  # convert pandas NaN (not a number) values to python None type which is equivalent to SQL NULL
    df = df[ordered_columns]  # manually change sorting to database ordering
    return df


def loadYoleCAPEX(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path, sheet_name="capex", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at 20## or "First_Col"
    df = df.filter(regex=r'20([0-9]{2}$)|First_Col')

    # print(df.head(7)) # to test

    table_list = list()
    skip_flag = False

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'Total':
            skip_flag = True

        elif temp == 'Capex ($m)':
            skip_flag = False

        elif skip_flag:
            continue

        else:
            row_dict = dict()
            row_dict['row_item'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['year'] = s
                new_row['capex'] = df.iloc[row.Index, i] * 1000000.0
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['row_item', 'year', 'capex', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYoleDemand(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="supply_demand", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Q#-## or "First_Col"
    df = df.filter(regex=r'Q\d-\d+|First_Col')
    # print(df.head(7))  # to test
    table_list = list()
    skip_flag = True

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'Total':
            skip_flag = True

        elif temp == 'Sell-through demand (m Gb)':
            skip_flag = False

        elif skip_flag:
            continue

        else:
            row_dict = dict()
            row_dict['segment'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = '20' + s[-2:] + '-Q0' + s[1]  # reformat Qq-yy to YYyy-Qqq
                new_row['demand(Gb)'] = df.iloc[row.Index, i] * 1000000.0
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['segment', 'quarter', 'demand(Gb)', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYoleRevenue(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="ms_total", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Q#-## or "First_Col"
    df = df.filter(regex=r'Q\d-\d+|First_Col')
    # df = df.head(7)  # to test
    table_list = list()
    skip_flag = False
    current_section = 1
    section_dict = {1: 'supplier', 2: 'segment', 3: 'region'}

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows
        second_col = df.columns[1]

        # print(second_col)
        # print(type(second_col))
        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'Total':
            skip_flag = True
            current_section += 1

        elif temp == 'Revenue ($m)':
            skip_flag = False

        elif skip_flag:
            continue

        else:
            row_dict = dict()
            row_dict['type'] = section_dict[current_section]
            row_dict['row_item'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = '20' + s[-2:] + '-Q0' + s[1]  # reformat Qq-yy to YYyy-Qqq
                new_row['revenue'] = df.iloc[row.Index, i] * 1000000.0
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['type', 'row_item', 'quarter', 'revenue', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYoleProcessMix(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path, sheet_name="process_mix", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Q#-## or "First_Col"
    df = df.filter(regex=r'Q\d-\d+|First_Col')
    # print(df.head(7))  # to test

    offset = -1
    table_list = list()
    wafer_bit_flag = True

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'Process mix (% of wafers)':
            wafer_bit_flag = True

        elif temp == 'Process mix (% of bits)':
            wafer_bit_flag = False

        elif temp == 'Average process node (nm)':
            continue

        elif wafer_bit_flag:
            row_dict = dict()
            row_dict['process'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = '20' + s[-2:] + '-Q0' + s[1]  # reformat Qq-yy to YYyy-Qqq
                new_row['pctwafer'] = df.iloc[row.Index, i]
                table_list.append(new_row)

        else:
            for i in range(1, len(row) - 1):
                # print(offset + i)
                table_list[offset + i]['pctbit'] = df.iloc[row.Index, i]
            offset += len(row) - 2

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['process', 'quarter', 'pctwafer', 'pctbit', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYoleOpMargin(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path, sheet_name="financial", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at 20## or "First_Col"
    df = df.filter(regex=r'Q\d-\d+|First_Col')
    # print(df.head(7))  # to test

    table_list = list()
    skip_flag = True

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'Operating margin (%)':
            skip_flag = False

        elif skip_flag:
            continue

        elif temp == 'Average':
            break

        else:
            row_dict = dict()
            row_dict['supplier'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = '20' + s[-2:] + '-Q0' + s[1]  # reformat Qq-yy to YYyy-Qqq
                new_row['opmargin'] = df.iloc[row.Index, i]
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['supplier', 'quarter', 'opmargin', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYolePricing(file_path, upload_dt):
    # Extract -- Load Yole Price Table
    data = loadExcelFile(file_path=file_path, header_row=6)
    df = data[list(data.keys())[
        0]]  # since there is only one sheet, set the dataframe as the only value in the data dictionary

    rows_list = list()

    # Transform -- Remove blank columns and change label of second column
    df.drop(df.columns[0], axis=1, inplace=True)  # drop first column (blank)
    df.rename(columns={'Unnamed: 1': 'MemoryType'}, inplace=True)  # set label of unnamed column

    # Transform -- Convert data to 2-dimensional data for SQL table
    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows
        row_dict = dict()
        row_dict['source'] = "Yole"
        row_dict['upload_dt'] = upload_dt

        temp = getattr(row, '_2')  # get second column (1-indexed)
        if pd.isnull(temp):  # blank row
            # df.drop(row.Index, inplace=True)
            continue
        elif temp == "Historical" or temp == "Forecast":  # header row
            # df.drop(row.Index, inplace=True)
            continue
        else:
            temp = getattr(row, 'MemoryType')  # get second column (1-indexed)
            row_dict['memory_full'] = temp
            temp = temp.split()
            row_dict['density_GB'] = convertDensity(temp[1])
            row_dict['memory_tech'] = temp[0]
            if len(temp) > 2:
                row_dict['memory_tech2'] = temp[2].replace('-', '')  # remove hyphen

        for key in df.columns[1:]:  # column names are actually datetimes so we use these as the "keys"
            new_row = row_dict.copy()
            new_row['forecast_dt'] = key
            new_row['price'] = df.loc[row.Index, key]
            rows_list.append(new_row)

    return pd.DataFrame(rows_list)


def loadDeDIOSPricing(file_path, upload_dt):
    # Extract -- Load De DIOS Price Tables
    data = loadExcelFile(file_path=file_path,
                         header_row=[7, 8])  # data is a dict of dataframes of excel sheets within a document

    rows_list = list()
    for sheet in data.keys():
        df = data[sheet]

        # Transform -- Remove blank columns from DeDios tables
        blank_columns = list()
        for column_name in df.columns:
            if df[column_name].isnull().all():
                blank_columns.append(column_name)
        # print(blank_columns)
        df.drop(blank_columns, axis=1, inplace=True)  # drop blank columns
        df.drop(df.tail(3).index, inplace=True)  # drop last 3 rows (footer)

        # Transform -- Convert date strings into python datetime variables
        years = df[('Unnamed: 1_level_0', 'Year')]
        for i in range(len(years)):  # iterate through all values in the year column
            if not pd.isnull(years[i]):  # copy year values to match previous row
                current_year = str(years[i])
            else:
                df.loc[i, ('Unnamed: 1_level_0', 'Year')] = current_year
        dates = list()
        i = 0
        for month_abbr in df[('Unnamed: 2_level_0', 'Month')]:
            if not pd.isnull(month_abbr) and len(
                    month_abbr) == 3:  # skip blank rows and rows where month is not in short (3 character) format
                date_str = month_abbr + ' ' + str(years[i])
                full_date = datetime.strptime(date_str, "%b %Y")
                dates.append(full_date)
                i += 1
        df.drop([('Unnamed: 1_level_0', 'Year'), ('Unnamed: 2_level_0', 'Month')], axis=1,
                inplace=True)  # drop date columns

        # Transform -- Convert data to 2-dimensional data for SQL table
        for column in df:
            row_dict = dict()
            row_dict['source'] = "DeDIOS"
            row_dict['upload_dt'] = upload_dt
            # row_dict['memory_tech2'] = sheet  # Excel sheet name

            row_dict['memory_full'] = column[0].replace('SO/DIMM', 'SODIMM').replace('LR/RDIMM', 'LRDIMM')
            temp = column[
                0].split()  # column[0] is the full name of the memory (Excel row 8), split function coverts string into list of words in string (space delimited)
            i = 0
            while not temp[i][0].isalpha():  # find first string in temp that begins with letter (alphabetic char)
                i += 1
                if i == len(temp):  # case when all substrings in full memory name begin with numbers
                    i = 1  # assume second string is the memory type
                    break
            row_dict['memory_tech'] = temp[i]
            row_dict['memory_tech2'] = np.nan
            row_dict['density_GB'] = convertDensity(temp[0])
            row_dict['price_type'] = column[1]  # column[1] is either Average, High, or Low (Excel row 9)

            if len(temp) > 2:
                for item in temp[2:]:
                    if "x" in item:
                        row_dict['width'] = 'x' + item.split('x')[-1]

                    elif item[0].isalpha() and not item == row_dict['memory_tech']:
                        row_dict['memory_tech2'] = item.replace('LR/RDIMM', 'LRDIMM').replace('/', '').replace('-',
                                                                                                               '')  # remove / and - special characters
                # print(row_dict['memory_tech2'])

            j = 0
            for i in range(len(dates)):
                new_row = row_dict.copy()
                new_row['price'] = df[column][i]
                new_row['forecast_dt'] = dates[j]
                rows_list.append(new_row)
                j += 1

        # print(df.head())  # print first 5 rows of excel file
        # print(df.info())  # print data types of each column

    return pd.DataFrame(rows_list)


def loadDXPricing(file_path, upload_dt):
    # Extract -- Load DRAMeXchange Price Table
    df = loadExcelFile(file_path=file_path, sheet_name="Price Forecast", header_row=6)

    rows_list = list()

    # Transform -- Remove blank columns and change label of second column
    df.drop(df.columns[-1], axis=1, inplace=True)  # drop last column (blank)
    df.rename(columns={'Unnamed: 0': 'MemoryType', 'Unnamed: 1': 'contract_type'},
              inplace=True)  # set label of unnamed columns

    dates = list()
    year_abbr = int(df.columns[-1][:-1])
    for i in range(1, 13):
        dates.append(datetime(year_abbr, i, 1))

    row_dict = dict()
    row_dict['source'] = "DX"
    row_dict['upload_dt'] = upload_dt
    previous_memory_full = None

    # Transform -- Convert data to 2-dimensional data for SQL table
    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows
        temp = getattr(row, 'contract_type')
        if pd.isnull(temp):  # blank row
            dates = list()
            # temp_date = getattr(row, '_19')  # attempt to parse value from final column (index 19)
            temp_date = getattr(row, '_' + str(len(df.columns)))  # attempt to parse value from final column
            if not pd.isnull(temp_date):
                try:
                    if temp_date[-1].isalpha():  # check is temp_date contains a letter appended
                        temp_date = temp_date[:4]  # parse the first four characters as the year
                except TypeError:
                    pass  # already an integer
                finally:
                    for i in range(1, 13):
                        dates.append(datetime(int(temp_date), i, 1))
        else:  # row containing data
            temp = getattr(row, 'MemoryType')
            if pd.isnull(temp):  # MemoryType field is empty
                row_dict['memory_tech'] = previous_memory_full
                temp = previous_memory_full.split()
            else:
                row_dict['memory_full'] = temp
                previous_memory_full = temp
                temp = temp.split()
            row_dict['memory_tech'] = temp[0]
            row_dict['density_GB'] = convertDensity(temp[1].strip(','))
            row_dict['width'] = "x" + temp[2].split('x')[1]
            row_dict['speed_MHz'] = temp[3]
            row_dict['price_type'] = getattr(row, 'contract_type')

            for i in range(12):  # iterate through the monthly prices
                new_row = row_dict.copy()
                new_row['forecast_dt'] = dates[i]
                temp = getattr(row, '_' + str(i + 3))
                if temp == 0:
                    temp = np.nan
                new_row['price'] = temp  # offset two columns and another for 1-index
                rows_list.append(new_row)

    return pd.DataFrame(rows_list)


def loadDXSufficiency(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="Sufficiency", header_row=6)
    # print(df)

    # Transform -- Remove blank columns and change label of first column to 'First_Col'
    df.rename(columns={'Sufficiency (%)': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at  or "First_Col"
    df = df.filter(regex=r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)|First_Col')
    # print(df.head(7))  # to test
    table_list = list()
    skip_flag = False
    month2int = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
                 "Nov": 11, "Dec": 12}
    # print(df.columns[1:])
    s = list(df.columns[1:])
    # print(s)

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            skip_flag = True

        elif temp == 'Sufficiency (%)':
            s = row[2:]
            continue

        elif temp == 'WW':
            skip_flag = False

        if skip_flag:
            continue

        else:
            row_dict = dict()
            row_dict['row_item'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                new_row['month'] = datetime(int('20' + s[i - 1][-2:]), month2int[s[i - 1][:3]],
                                            1)  # reformat MMM YY to YYYY/MM/DD
                new_row['pct_sufficiency'] = df.iloc[row.Index, i]
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['row_item', 'month', 'pct_sufficiency', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadDXSegProd(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="WW DRAM Production ", header_row=6)
    # print(df)

    # Transform -- Remove blank columns and change label of first column to 'First_Col'
    df.rename(columns={'Month': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Months or "First_Col"
    df = df.filter(regex=r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)|First_Col')
    # print(df.head(7))  # to test
    table_list = list()
    skip_flag = False
    month2int = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
                 "Nov": 11, "Dec": 12}
    s = list(df.columns[1:])

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp == 'WW':
            skip_flag = True

        elif temp.strip() == 'Month':
            s = row[2:]

        elif temp == 'PC':
            skip_flag = False

        if skip_flag:
            continue

        else:
            row_dict = dict()
            row_dict['row_item'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                new_row['month'] = datetime(int('20' + s[i - 1][4:6]), month2int[s[i - 1][:3]], 1)  # reformat MMM YY MM
                new_row['production'] = df.iloc[row.Index, i] * 1000000.0
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['row_item', 'month', 'production', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadDXWaferStarts(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="WW Wafer Starts", header_row=6)
    # print(df)

    # Transform -- Remove blank columns and change label of first column to 'First_Col'
    df.rename(columns={'Unnamed: 0': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Q# or "First_Col"
    df = df.filter(regex=r'Q[1-4]|First_Col')
    # print(df.head(7))  # to test

    table_list = list()
    skip_flag = False

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = str(getattr(row, 'First_Col'))  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp.endswith('QoQ'):
            break

        else:
            row_dict = dict()
            row_dict['row_item'] = temp[:4]
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = s
                new_row['kpcs'] = df.iloc[row.Index, i]
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['row_item', 'quarter', 'kpcs', 'upload_dt']]
    # print(staging_df)

    return staging_df


def loadYoleWaferProdSupplier(file_path, upload_dt):
    # Extract -- Load data from Excel source file
    df = loadExcelFile(file_path=file_path, sheet_name="wafer_production", header_row=2)

    # Transform -- Remove blank columns and change label of second column to 'First_Col'
    df.rename(columns={'Unnamed: 1': 'First_Col'}, inplace=True)  # set label of unnamed column

    # Look only at Q#-## or "First_Col"
    df = df.filter(regex=r'Q\d-\d+|First_Col')
    # print(df.head(7))
    table_list = list()
    skip_flag = True

    for row in df.itertuples(index=True, name='Pandas'):  # iterate through all rows

        temp = getattr(row, 'First_Col')  # get first column value as temp
        if pd.isnull(temp):
            continue

        elif temp.strip() == 'Total wafer production':
            skip_flag = False

        elif skip_flag:
            continue

        elif temp == 'Total':
            break

        else:
            row_dict = dict()
            row_dict['row_item'] = temp
            row_dict['upload_dt'] = upload_dt

            for i in range(1, len(row) - 1):
                new_row = row_dict.copy()
                s = df.columns[i]
                new_row['quarter'] = '20' + s[-2:] + '-Q0' + s[1]  # reformat Qq-yy to YYyy-Qqq
                new_row['wafer_prod'] = df.iloc[row.Index, i]
                table_list.append(new_row)

    staging_df = pd.DataFrame(table_list)
    staging_df = staging_df[['row_item', 'quarter', 'wafer_prod', 'upload_dt']]
    # print(staging_df)

    return staging_df


if __name__ == "__main__":
    start_time = time()

    # List of all metrics with corresponding loader functions below. Make sure name matches Project_params exactly.
    dx_metrics = ['SegProd', 'Sufficiency', 'WaferStarts']
    yole_metrics = ['Revenue', 'CAPEX', 'OpMargin', 'ProcessMix', 'WaferProdSupplier', 'Demand']
    
    # Initalize variables
    metrics = dict()

    testing = False  # Set this to True when testing
    if testing:
        metric_name = 'SegProd'  # Enter name of file you would like to test here
        metrics[metric_name] = Metric(metric_name)  
    else:
        all_metrics = dx_metrics.copy()
        all_metrics.extend(yole_metrics)
        all_metrics.append('Pricing')
        for metric_name in all_metrics:
            metrics[metric_name] = Metric(metric_name)

    # Attempt to load from all Excel files in given directory
    #dir = os.listdir(params['FilePath_MarketIntelligence'])   # List all files (including folders) in directory
    #print(dir)
    upload_dt = datetime.today()
    (_, _, file_list) = next(os.walk(params['FilePath_MarketIntelligence']))  # List all files (excluding folders) in directory
    for file in file_list:
        full_file_path = os.path.join(params['FilePath_MarketIntelligence'], file)
        if not file.startswith('~'):  # ignore open files
            if file.startswith('De') or 'PRICE TABLES' in file:  # check which is the currrent file from the directory
                upload_dt = parseDate(file, 'DeDIOS')
                if not testing:
                    # Load DeDIOS Pricing
                    df = loadDeDIOSPricing(full_file_path, upload_dt)
                    metrics['Pricing'].addFile(file, df)

            elif file.startswith('Yole') and 'Pricing Monitor' in file:  # Yole DRAM Monthly Pricing Monitor
                upload_dt = parseDate(file, 'Yole')
                if not testing:
                    # Load Yole Pricing
                    df = loadYolePricing(full_file_path, upload_dt)
                    metrics['Pricing'].addFile(file, df)

            elif file.startswith('Yole') and 'Market Monitor' in file:  # Yole DRAM Market Monitor
                if testing:
                    df = getattr(sys.modules[__name__], "loadYole%s" % metric_name)(full_file_path, upload_dt)
                    metrics[metric_name].addFile(file, df)
                else:
                    for metric in yole_metrics:
                        df = getattr(sys.modules[__name__], "loadYole%s" % metric)(full_file_path, upload_dt)
                        metrics[metric].addFile(file, df)

            elif file.startswith('DX') or 'Platinum' in file:  # DX DRAM Platinum Datasheet
                if testing:
                    df = getattr(sys.modules[__name__], "loadDX%s" % metric_name)(full_file_path, upload_dt)
                    metrics[metric_name].addFile(file, df)
                else:
                    for metric in dx_metrics:
                        df = getattr(sys.modules[__name__], "loadDX%s" % metric)(full_file_path, upload_dt)
                        metrics[metric].addFile(file, df)
                    # Load DX Pricing
                    df = loadDXPricing(full_file_path, parseDate(file, 'DX'))
                    metrics['Pricing'].addFile(file, df)

            else:  # Unknown file
                # TO DO: Add error logging for unknown file
                print('Unknown file found:', file)

    if not any([metrics[metric_name].containsData() for metric_name in metrics.keys()]):  # if all lists of dataframes are empty, aka no files were loaded
        log_warning(project_name='MSS Market Intelligence', data_area='All Areas', file_path=params['FilePath_MarketIntelligence'], warning_type="Missing")
        # print('No new Excel files present in folder.')

    else:  # at least one Excel file was loaded
        successfully_loaded_files = set()

        for metric_name in metrics.keys():
            metric = metrics[metric_name]

            if metric.containsData():
                if metric_name == 'Pricing':  # Pricing combines three data sources into a single dataframe before uploading
                    db_column_list = ['source', 'upload_dt','forecast_dt', 'memory_full', 'memory_tech', 'memory_tech2', 'price', 'price_type', 'density_GB', 'width', 'speed_MHz']
                    staging_df = formatDataFrames(ordered_columns=db_column_list, df_list=metric.dataframes) # ensure all loaded dataframes match database format

                else:  # All other loaders besides Pricing
                    staging_df = metric.dataframes[0]
  
                #print(staging_df.head(60))  # print each dataframe for debugging purposes
                #print(staging_df.info())
                insert_succeeded, error_msg = uploadDFtoSQL(table=metric.table, data=staging_df, driver="{SQL Server}")
                log(insert_succeeded, project_name='MSS Market Intelligence', package_name=metric.package_name, data_area=metric.data_area, row_count=staging_df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    for file in metric.files:
                        successfully_loaded_files.add(file)  # add files to list of correctly loaded files

                if metric_name == 'Pricing':
                    execute_succeeded, error_msg = executeStoredProcedure(procedure_name=params['SP_MI_Pricing'])
                    log(execute_succeeded, project_name='MSS Market Intelligence',  package_name='SQL: ' + params['SP_MI_Pricing'], data_area=metric.data_area, row_count=0, error_msg=error_msg)

        for file in successfully_loaded_files:  # for all files that were successfully loaded into the database
            shutil.move(os.path.join(params['FilePath_MarketIntelligence'], file), os.path.join(params['Archive_Folder_MarketIntelligence'], file))  # Move Excel file to Archive folder after it has been loaded successfully

    print("--- %s seconds ---" % (time() - start_time))
