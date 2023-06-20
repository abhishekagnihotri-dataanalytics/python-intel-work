__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SCRAM tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 10:00 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import datetime
from Helper_Functions import getLastRefresh, loadExcelFile, uploadDFtoSQL, querySSAS
from Logging import log


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    project_name = 'SCRAM'

    ### BEGIN Master File Load ###
    data_area = 'Master File'
    table = 'bcp.SCRAM_MasterFile'
    file_path = "https://intel.sharepoint.com/:x:/r/sites/MarysTeam/Shared%20Documents/SCRAM%20-%20Supply%20Chain%20Risk%20Analytics%20Model/SCRAM%20Data%20Entry.xlsx?d=w53d4cbeac9e840108b2fb4721af5f4e5&csf=1&web=1&e=6r9IUc"

    last_refreshed = getLastRefresh(project_name, data_area)
    df = loadExcelFile(file_path, sheet_name='Sheet1', last_upload_time=last_refreshed)
    if len(df.index) == 0:
        print('SCRAM Data Entry file has not been updated since last run. Skipping.')
    else:
        # print(df.columns)

        # Directly copied columns
        try:
            last_column = 'Supplier Site Residual Risk (SRS)'
            df['Location'] = df['Supplier Site Residual Risk (SRS)']

            sfh_column = [col for col in df.columns if col.startswith('SFH Q')][0]  # Determine name of SFH Cost Column since the quarter will change
            last_column = 'SFH'
            df['Financial'] = df[sfh_column]

            # Calculated columns
            last_column = 'Security Score using SRC Data were available'
            df['Security'] = df['Security Score using SRC Data were available']
            # df['Security'] = df['Security Score using SRC Data were available']

            # df['Single / Sole Source Risk'] = df['Sourcing Current (Availability)'].apply(lambda x: )

            paid_column = [col for col in df.columns if col.startswith('Global Supplier Paid')][0]  # Determine name of the Paid Column since the year will change
            last_column = 'Global Supplier Paid'
            df['Spending'] = df[paid_column].apply(lambda x: 'Very Low' if isinstance(x, int) and x < 2000000
                                                             else 'Low' if isinstance(x, int) and x < 5000000
                                                             else 'Medium' if isinstance(x, int) and x < 20000000
                                                             else 'High' if isinstance(x, int) and x < 50000000
                                                             else 'Very High' if isinstance(x, int) and x >= 50000000
                                                             else None)
        except (KeyError, IndexError):
            log(False, project_name=project_name, data_area=data_area, error_msg="Column {0} not found in the {1}.".format(last_column, data_area))

        keep_columns = ['Global ESD ID', 'Supplier Name', 'Sourcing Current (Availability)', 'Alternates Exist?',
                        'Supplier Diversification Plan in Place?', 'Constraint Commodity?', 'SOV/MFG Starts (Influence)',
                        'Supplier Concentration', 'Supplier China Concentration', 'Geographic Concentration', 'Time to Qual',
                        'Qualification', 'Location', 'Security', 'Financial', 'Sourcing', 'alternate sourcing risk (No alternates)',
                        'Capacity / Availability / DOI Dependency / Constraint', 'MRA / Tech Node / Qual / Ramp / New supplier risk',
                        'Spending', 'Merger / Acquisition / sale', 'COVID / Recovery']
        try:
            df.drop(df.columns.difference(keep_columns), axis=1, inplace=True)  # remove other columns (Power Query) python equivalent
            df = df[keep_columns]  # manually change column order
        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg="Column missing/changed in Master File.")

        # change Yes/No column to True/False
        boolean_columns = ['Alternates Exist?', 'Supplier Diversification Plan in Place?', 'Constraint Commodity?']
        for col in boolean_columns:
            df[col] = df[col].apply(lambda x: False if isinstance(x, str) and x.lower() == 'no' else True if isinstance(x, str) else None)

        # add database standards columns to end of DataFrame
        df['LoadDtm'] = datetime.now()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, categorical=['Global ESD ID'], truncate=True)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ### END Master File Load ###

    ### BEGIN Domain Mapping Load ###
    data_area = 'Domains'
    table = 'bcp.SCRAM_Domains'

    last_refreshed = getLastRefresh(project_name, data_area)
    df = loadExcelFile(file_path, sheet_name='Domains', last_upload_time=last_refreshed)
    if len(df.index) == 0:
        print('SCRAM Data Entry file has not been updated since last run. Skipping.')
    else:
        # print(df.columns)

        df.drop(['Department'], axis=1, inplace=True)  # Remove unnecessary column

        # add database standards columns to end of DataFrame
        df['LoadDtm'] = datetime.now()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, truncate=True)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ### END Domain Mapping Load ###

    # ### BEGIN Sourcing Load ###
    # table = 'bcp.SourcingbyCommoditySupplier'
    #
    # statement = """EVALUATE
    #
    #                 DISTINCT(
    #                     SELECTCOLUMNS(
    #                         FILTER(
    #                             SUMMARIZE(
    #                                     Spends,
    #                                     Commodity[Commodity Code],
    #                                     Commodity[Commodity Description],
    #                                     Commodity[High Level Org],
    #                                     Commodity[Sourcing Org],
    #                                     Commodity[Spend Category],
    #                                     Commodity[Product Category],
    #                                     Supplier[Supplier ID Number],
    #                                     Supplier[Parent Supplier Name (SIH)],
    #                                     Supplier[Supplier Child ID],
    #                                     Supplier[Supplier Child Name],
    #                                     Calendar[Fiscal_Year]
    #                             ),
    #                             Calendar[Fiscal_Year] > CONVERT(YEAR(TODAY()) - 2, STRING)
    #                             && NOT(ISBLANK(Commodity[Commodity Code]))
    #                         ),
    #                         "Commodity Code", Commodity[Commodity Code],
    #                         "Commodity Description", Commodity[Commodity Description],
    #                         "High Level Org", Commodity[High Level Org],
    #                         "Sourcing Org", Commodity[Sourcing Org],
    #                         "Spend Category", Commodity[Spend Category],
    #                         "Product Category", Commodity[Product Category],
    #                         "Parent Supplier ID", Supplier[Supplier ID Number],
    #                         "Parent Supplier Name", Supplier[Parent Supplier Name (SIH)],
    #                         "Child Supplier ID", Supplier[Supplier Child ID],
    #                         "Child Supplier Name", Supplier[Supplier Child Name]
    #                     )
    #                 )
    #                 ORDER BY
    #                     [Parent Supplier ID] ASC,
    #                     [Parent Supplier Name] ASC,
    #                     [Child Supplier ID] ASC,
    #                     [Child Supplier Name] ASC,
    #                     [Commodity Description] ASC,
    #                     [High Level Org] ASC,
    #                     [Sourcing Org] ASC,
    #                     [Spend Category] ASC,
    #                     [Product Category] ASC
    #             """
    #
    # df = querySSAS(statement, server='GSM_SupplyChainMetrics.intel.com', model='GSM_SupplyChainMetrics')
    # if len(df.index) == 0:
    #     print('Unable to load from GSM_SupplyChainMetrics cube.')
    # else:
    #     print(df.columns)
    #
    #     # add database standards columns to end of DataFrame
    #     df['LoadDtm'] = datetime.now()
    #     df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
    #
    #     sql_columns = ['Commodity Code', 'Commodity Description', 'High Level Org', 'Sourcing Org', 'Spend Category',
    #                    'Product Category', 'Supplier ID Number', 'Parent Supplier Name (SIH)', 'Supplier Child ID',
    #                    'Supplier Child Name',
    #                    # 'Sourcing', 'Other', 'CommoditySupplierKey',
    #                    'LoadDtm', 'LoadBy']
    #
    #     insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, columns=sql_columns, categorical=['[Parent Supplier ID]', '[Child Supplier ID]'], truncate=True)
    #     # log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    #     if insert_succeeded:
    #         print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    #     else:
    #         print(error_msg)
    # ### END Sourcing Load ###
