__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script loads Intel Top Secret (ITS) data into the GSCDW database on the sql2592-fm1s-in.amr.corp.intel.com,3181 server"""
__schedule__ = "Daily at 7:05 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from time import time
from Helper_Functions import uploadDFtoSQL, queryOdata, loadExcelFile, getLastRefresh
from Logging import log, log_warning


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def remove_blank_columns(df: pd.DataFrame):
    data_frame = df

    blank_columns = list()
    for column_name in df.columns:
        if data_frame[column_name].isnull().all():
            blank_columns.append(column_name)
    # print(blank_columns)
    data_frame.drop(blank_columns, axis=1, inplace=True)

    return data_frame


if __name__ == '__main__':
    start_time = time()

    # Initialize variables
    project_name = 'ATS SCCI Demand'

    #### BEGIN LOAD PSI PROCUREMENT FROM ODATA FEED ####
    data_area = 'PSI Procurement'
    table = 'Base.PSIProcurements'

    # dev and production OData urls
    odata_dev = 'https://ems-int.intel.com/odata/psiprocurements'  # ?$select=Id,Site,Material,ModuleDriver,ProductDriver
    odata_prod = 'https://ems.intel.com/odata/psiprocurements'

    query_succeeded, result, error_msg = queryOdata(odata_prod)
    if not query_succeeded:
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)
    else:
        try:
            df = pd.DataFrame(result['value'])

            df['ModuleTie'] = None  # Add missing column to data

            df = df[['Id', 'Site', 'Material', 'Qty', 'Rdd', 'Wbs', 'PrNumber', 'UnitOfMeasure', 'ModuleDriver',
                     'ProductDriver', 'ModuleTie', 'InactiveOn']]  # manually reorder columns to match SQL table

            df['LoadDtm'] = pd.to_datetime('today')
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column missing/changed in OData feed.')
    #### END LOAD PSI PROCUREMENT FROM ODATA FEED ####

    #### BEGIN LRP PSI REQUIREMENTS LOAD ####
    data_area = 'LRP PSI Requirements'
    table = 'Base.LRPPSIRequirements'
    excel_file = 'https://intel.sharepoint.com/:x:/r/sites/gscatsscci-SCCITabularModelDataSources/Shared%20Documents/SCCI%20Tabular%20Model%20Data%20Sources/Phx%20Capacity%20Report.xlsm?d=w0ec83b2891cf40348e2405eb65d72964&csf=1&web=1&e=wIXpay' # r"\\Vmsstmeshrs100.amr.corp.intel.com\D$\ATSData\Phx Capacity Report.xlsm"
    sheet_name = "DB Data"

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file on SharePoint Online
    try:
        df = loadExcelFile(excel_file, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
    except FileNotFoundError as error:
        log(False, project_name=project_name, data_area=data_area, error_msg='No file found. Unable to find Excel file {}'.format(excel_file))
        raise error

    if len(df.index)  == 0:
        log_warning(project_name=project_name, data_area=data_area, file_path=excel_file, warning_type='Not Modified')
        print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
    else:
        # Transform data
        df = remove_blank_columns(df)

        df['LRP RDD Year_Quarter'] = df['LRP RDD Year_Quarter'].apply(lambda x: x[:4] + '-Q0' + x[-1] if isinstance(x, str) else None)  # Convert Year Quarter into YYYY-Q0Q format

        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # Load data into SQL Server database
        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
    #### END LRP PSI REQUIREMENTS LOAD ####

    print("--- %s seconds ---" % (time() - start_time))
