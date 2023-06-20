__author__ = "N/A"
__email__ = "N/A"
__description__ = "Loading spends raw data into sql staging"
__schedule__ = "N/A"
import os
import pandas as pd
import numpy as np
from time import time
from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import ProgrammingError
import sqlalchemy_hana
import urllib
from Helper_Functions import queryHANA, map_columns, uploadDFtoSQL
from Project_params import params
from Password import accounts


def queryHANA1(statement: str, environment: str = 'Production', credentials: dict = None, single_sign_on: bool = False) -> pd.DataFrame:
    result = pd.DataFrame()

    servers = {
        'Production': {'server': 'sapehpdb.intel.com', 'port': '31015'},
        'Pre-DEV': {'server': 'sapeh1db.intel.com', 'port': '33715'},
        'QA/CONS': {'server': 'sapehcdb.intel.com', 'port': '31215'},
        'Development': {'server': 'sapnbidb.intel.com', 'port': '31115'},
        'Benchmark': {'server': 'sapehbdb.intel.com', 'port': '31015'},
        'Production Support': {'server': 'sapehsdb.intel.com', 'port': '31315'},
    }

    if single_sign_on:
        engine = create_engine('hana://{address}:{port}'.format(address=servers[environment]['server'], port=servers[environment]['port']))
    else:
        if credentials is None:
            credentials = {'username': accounts['HANA'].username, 'password': accounts['HANA'].password}
        engine = create_engine('hana://{user}:{password}@{address}:{port}'.format(user=credentials['username'], password=credentials['password'], address=servers[environment]['server'], port=servers[environment]['port']))

    conn = engine.connect().execution_options(stream_results=True)
    chunks = pd.read_sql(statement, con=conn, chunksize=10000)
    for chunk_df in chunks:
        if len(result.index) == 0:
            result = chunk_df
        else:
            result = result.append(chunk_df, ignore_index=True)

    return result


def uploadDFtoSQL1(table: str, data: pd.DataFrame, columns: list = None, categorical: list = None, truncate: bool = True,
                  chunk_size: int = 10000, driver: str = params['SQL_DRIVER'], server: str = params['GSMDW_SERVER'],
                  database: str = params['GSMDW_DB'], username: str = None, password: str = None) -> list:
    """Function to insert Pandas DataFrame into SQL Server database.

    Args:
        table: Name of the table within the database.
        data: [pandas DataFrame] Data to be uploaded.
        columns: [list of str] Ordered list of columns within table.
        categorical: [list of str] Columns to convert from numeric to text (indicating they are categorical i.e. supplier id)
        truncate: [bool] Truncate table prior to loading?
        chunk_size: [int] How large of chunks you would like to use during inserts
        driver: [str] Which SQL driver to use. Default from Project_params file.
        server: [str] Which server to connect to. Default from Project_params file.
        database: [str] Which database to connect to. Default from Project_params file.
        username: [str] Username to use for SQL Server Authentication. Default uses Windows Authentication.
        password: [str] Password to use for SQL Server Authentication. Default uses Windows Authentication.

    Returns:
        [two element list] [boolean] If the insert statement executed properly. True for success, False otherwise. [str] Error message.

    """
    if chunk_size == 10000 and data.shape[0] <= 10000:  # do not chunk when DataFrame is less than or equal to 10000 rows
        chunk_size = 0

    table_schema = table.split('.')[0].replace("[", "").replace("]", "")
    table_name = table.split('.')[1].replace("[", "").replace("]", "")

    success_bool = False
    error_msg = None

    # # If user specified to truncate Table prior to loading new data
    # if truncate:
    #     if params['GSMDW_SERVER'] == 'sql3266-fm1-in.amr.corp.intel.com,3181' and params['GSMDW_DB'] == 'gscdw':  # only for new Supply Chain BI Production environment
    #         execute_statement = "SET NOCOUNT ON;\nDECLARE @ret int\nEXEC @ret = ETL.spTruncateTable '{}'\nSELECT @ret;".format(table)  # use Stored Procedure to truncate table
    #     else:
    #         execute_statement = "TRUNCATE TABLE " + table + ";"
    #
    #     print("TRUNCATE TABLE " + table + ";")
    #     try:
    #         if execute_statement.startswith('TRUNCATE'):  # Normal truncation
    #             cursor.execute(execute_statement)
    #         else:  # Stored Procedure truncation
    #             sp_success_bool = not bool(cursor.execute(execute_statement).fetchone()[0])  # Zero return values in SQL Server indicate success
    #             if not sp_success_bool:  # Stored Procedure failed to return success message
    #                 conn.rollback()
    #                 error_msg = "Table " + table + " could not be truncated. Check the spelling of your table name and ensure it is present on the database you are connection to. This function will not attempt to load data now because of possible duplication."
    #                 return [sp_success_bool, error_msg]
    #     except pyodbc.ProgrammingError:
    #         conn.rollback()  # rollback to previous database state
    #         success_bool = False
    #         error_msg = "Table " + table + " could not be truncated. Check the spelling of your table name and ensure it is present on the database you are connection to. This function will not attempt to load data now because of possible duplication."
    #         return [success_bool, error_msg]

    # format DataFrame values for SQL
    data.replace(r'^\s*$', np.nan, regex=True, inplace=True)  # replace field that's entirely spaces (or empty) with NaN
    if categorical:
        for col in categorical:
            if data[col].dtype == np.float64 or data[col].dtype == np.int64:
                data[col] = data[col].map(lambda x: '{:.0f}'.format(x))  # avoid pandas automatically using scientific notation for large numbers
                data[col].replace('nan', np.nan, inplace=True)  # when line above converts float to string it incorrectly converts nulls
    data.replace({pd.NaT: None, np.nan: None, 'NaT': None, 'nan': None}, inplace=True)  # convert pandas NaT (not a time) and numpy NaN (not a number) values to python None type which is equivalent to SQL NULL

    # Use sqlalchemy to connect to database instead of pyodbc
    connection_string = "DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;integrated security=true".format(driver=driver, server=server, database=database)
    # noinspection PyUnresolvedReferences
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus(connection_string)), echo=False)

    # Enable executemany functionality for faster insert with pandas to_sql
    @listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    if truncate:
        table_behavior = 'replace'
    else:
        table_behavior = 'append'


    try:
        if chunk_size <= 0:
            print('Loading first chunk')
            data.to_sql(name=table_name, con=engine, schema=table_schema, if_exists=table_behavior, index=False)
        else:
            data.to_sql(name=table_name, con=engine, schema=table_schema, if_exists=table_behavior, index=False, method='multi', chunksize=chunk_size)  # Write data to sql using the specified chunk_size
        success_bool = True
    except ProgrammingError as error:
        success_bool = False
        error_msg = error.args[1].split('\n')[0]

    return [success_bool, error_msg]


if __name__ == "__main__":
    # table = 'Base.PurchaseOrderDetailsQueryHana'
    table = 'Base.PaidDetailQuery'

    initial_start_time = time()

    for authorization_year in [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]:
        truncate_var = True if authorization_year == 2020 else False

        print('Extracting {0} data from HANA...'.format(authorization_year))

        if table == 'Base.PurchaseOrderDetailsQueryHana':
            pickle_file_path = './{}_Purchase_Order_Details_Matt.pkl'.format(authorization_year)

            query = """SELECT "PONbr"
                    ,"POLineNbr"
                    ,"MaterialNbr_IPN"
                    ,"SupplierId"
                    ,"MaterialNbr_IPN_Desc"
                    ,"WBSNumber"
                    ,"CompanyCd"
                    ,"CompanyName"
                    ,"GLAcctNbr"
                    ,"GLAcctName"
                    ,"ProductCategoryCd"
                    ,"ProductCategoryName"
                    ,"SourcingOrganizationNm"
                    ,"HighLevelOrganizationCd"
                    ,"HighLevelSupplyChainOrganizationNm"
                    ,"PurchOrgCd"
                    ,"PurchOrgName"
                    ,"POLineFirstAuthorizationYear"
                    ,"POLineFirstAuthorizationQuarter"
                    ,"POLineFirstAuthorizationMonth"
                    ,"PurchaseDocumentLineCreateDt"
                    ,"CostCenterCd"
                    ,"CommitsCurrencyTransactionAmt"
                    ,"InvoicedAgainstCommitsUSDAmt"
                    ,"PaidAgainstCommitsUSDAmt"
                    ,"ReleaseGrp"
                    ,"IncoTermsDesc"
                    ,"GlobalBusinessOrganizationDunsNm"
                    ,"InvoiceSupplierId"
                    ,"GlobalBusinessOrganizationDunsNbr"
                    ,"SplrName"
                    ,"BusinessOrganizationDunsNbr"
                    ,"BusinessOrganizationDunsNm"
                    ,sum("CommitsCurrencyTransactionAmt") AS "CommitsCurrencyTransactionAmtSum"
                    ,sum("PaidAgainstCommitsUSDAmt") AS "PaidAgainstCommitsUSDAmtSum"
                    ,sum("InvoicedAgainstCommitsUSDAmt") AS "InvoicedAgainstCommitsUSDAmtSum"
                FROM "_SYS_BIC"."intel.sourceidp.consumption.procurement/PurchaseOrderDetailsQuery"(
                    --'PLACEHOLDER' = ('$$IP_Supplier$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_POLineFirstAuthorizationQuarter$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_CompanyCode$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_PurchasingOrganization$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_PurchaseDocumentTypeCd$$', '''*'''),
                    'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateStartDt$$', '{authorization_year}0101'),  
                    'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateEndDt$$', '{authorization_year}1231'), 
                    --'PLACEHOLDER' = ('$$IP_Requisitioner$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_GoodsIndicator$$', '''*'''), 
                    'PLACEHOLDER' = ('$$IP_PurchaseDocumentDeleteInd$$', 'N'), 
                    --'PLACEHOLDER' = ('$$IP_POLineFirstAuthorizationWW$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_POCreator$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_POLineFirstAuthorizationMonth$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_PurchaseDocumentLineNbr$$', '''*'''), 
                    'PLACEHOLDER' = ('$$IP_POLineFirstAuthorizationYearFrom$$', '{authorization_year}'),
                    'PLACEHOLDER' = ('$$IP_POLineFirstAuthorizationYearTo$$', '{authorization_year}')
                    --'PLACEHOLDER' = ('$$IP_PurchaseDocumentNbr$$', '''*'''), 
                    --'PLACEHOLDER' = ('$$IP_CommodityCode$$', '''*'''), 
                )
                --where "MaterialNbr_IPN" = '500219716'
                GROUP BY "PONbr"
                    ,"SupplierId"
                    ,"POLineNbr"
                    ,"MaterialNbr_IPN"
                    ,"MaterialNbr_IPN_Desc"
                    ,"WBSNumber"
                    ,"CompanyCd"
                    ,"CompanyName"
                    ,"GLAcctNbr"
                    ,"GLAcctName"
                    ,"ProductCategoryCd"
                    ,"ProductCategoryName"
                    ,"SourcingOrganizationNm"
                    ,"HighLevelOrganizationCd"
                    ,"HighLevelSupplyChainOrganizationNm"
                    ,"PurchOrgCd"
                    ,"PurchOrgName"
                    ,"POLineFirstAuthorizationYear"
                    ,"POLineFirstAuthorizationQuarter"
                    ,"POLineFirstAuthorizationMonth"
                    ,"PurchaseDocumentLineCreateDt"
                    ,"CostCenterCd"
                    ,"CommitsCurrencyTransactionAmt"
                    ,"InvoicedAgainstCommitsUSDAmt"
                    ,"PaidAgainstCommitsUSDAmt"
                    ,"ReleaseGrp"
                    ,"IncoTermsDesc"
                    ,"GlobalBusinessOrganizationDunsNm"
                    ,"InvoiceSupplierId"
                    ,"GlobalBusinessOrganizationDunsNbr"
                    ,"GlobalBusinessOrganizationDunsNm"
                    ,"SplrName"
                    ,"BusinessOrganizationDunsNbr"
                    ,"BusinessOrganizationDunsNm" 
            """.format(authorization_year=authorization_year)

        elif table == 'Base.PaidDetailQuery':
            pickle_file_path = './{}_Paid_Details_Matt.pkl'.format(authorization_year)

            query = """
                SELECT "AccountDocumentNbr"
                    ,"AccountingDocumentExpenseLineNbr"
                    ,"AccountDocumentLiabilityLineNbr"
                    ,"InvoiceNbr"
                    ,"InvoiceLineNbr"
                    ,"PurchaseOrderNbr"
                    ,"PurchaseOrdertLineNbr"
                    ,"ClearingDocumentNbr"
                    ,"InvoiceFiscalYr"
                    ,"PurchaseDocumentUpdateDt"
                    ,"PurchaseDocumentCreateDt"
                    ,"AccountDocumentTypeCd"
                    ,"GeneralLedgerTransactionTypeCd"
                    ,"FulfillmentProcessCd"
                    ,"SupplierRemitToId"
                    ,"SupplierRemitToNm"
                    ,"SupplierOrderFromId"
                    ,"SupplierOrderFromNm"
                    ,"CommitsTransactionCurrencyAmt"
                    ,"InvoiceTransactionCurrencyCd"
                    ,"PurchaseOrderTransactionCurrencyCd"
                    ,"SupplierInvoiceNbr"
                    ,"ProductCategoryCd"
                    ,"ProductCategoryNm"
                    ,"PaymentDocumentYearNbr"
                    ,"PaymentDocumentWorkWeekNbr"
                    ,"PaymentDocumentQuarterNbr"
                    ,"PaymentDocumentMonthNbr"
                    ,"PaymentDocumentDt"
                    ,"InvoicePaymentTermsCd"
                    ,"InvoicePaymentTermsNm"
                    ,"CostCenterCd"
                    ,"CostCenterNm"
                    ,"CompanyCd"
                    ,"PaidTransactionAmt"
                    ,"EarnedDiscountTransactionCurrencyAmt"
                    ,"DiscountLostTransactionCurrencyAmt"
                    ,sum("PaidAmt") AS "PaidAmt"
                    ,sum("DiscountLostAmt") AS "DiscountLostAmt"
                    ,sum("EarnedDiscountAmt") AS "EarnedDiscountAmt"
                    ,sum("CommitsAmt") AS "CommitsAmt" 
                FROM "_SYS_BIC"."intel.sourceidp.consumption.procurement/PaidDetailQuery"(
                    --'PLACEHOLDER' = ('$$IP_PmntDocQuarter$$', '''*'''),
                    --'PLACEHOLDER' = ('$$IP_Supplier$$', '''*'''),
                    'PLACEHOLDER' = ('$$IP_PaymentDocStartDate$$', '01/01/{payment_year}'),
                    'PLACEHOLDER' = ('$$IP_PaymentDocEndDate$$', '12/31/{payment_year}'),
                    --'PLACEHOLDER' = ('$$IP_SupplierID$$', '''*'''),
                    'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateStartDt$$', '01/01/{payment_year}'),
                    'PLACEHOLDER' = ('$$IP_PurchaseDocumentCreateEndDt$$', '12/31/{payment_year}'),
                    --'PLACEHOLDER' = ('$$IP_CostCenter$$', '''*'''),
                    'PLACEHOLDER' = ('$$IP_PmntDocYearFrom$$', '{payment_year}'),
                    'PLACEHOLDER' = ('$$IP_PmntDocYearTo$$', '{payment_year}'),
                    'PLACEHOLDER' = ('$$IP_SupplierRemitTo$$', '''*'''),
                    'PLACEHOLDER' = ('$$IP_PostingStartDate$$', '01/01/{payment_year}'),
                    'PLACEHOLDER' = ('$$IP_PostingEndDate$$', '12/31/{payment_year}')
                    --'PLACEHOLDER' = ('$$IP_PmntDocMonth$$', '''*'''),
                    --'PLACEHOLDER' = ('$$IP_PmntDocWorkWeek$$', '''*'''),
                )
                GROUP BY "AccountDocumentNbr"
                    ,"AccountingDocumentExpenseLineNbr"
                    ,"AccountDocumentLiabilityLineNbr"
                    ,"InvoiceNbr"
                    ,"InvoiceLineNbr"
                    ,"PurchaseOrderNbr"
                    ,"PurchaseOrdertLineNbr"
                    ,"ClearingDocumentNbr"
                    ,"InvoiceFiscalYr"
                    ,"PurchaseDocumentUpdateDt"
                    ,"PurchaseDocumentCreateDt"
                    ,"AccountDocumentTypeCd"
                    ,"GeneralLedgerTransactionTypeCd"
                    ,"FulfillmentProcessCd"
                    ,"SupplierRemitToId"
                    ,"SupplierRemitToNm"
                    ,"SupplierOrderFromId"
                    ,"SupplierOrderFromNm"
                    ,"CommitsTransactionCurrencyAmt"
                    ,"InvoiceTransactionCurrencyCd"
                    ,"PurchaseOrderTransactionCurrencyCd"
                    ,"SupplierInvoiceNbr"
                    ,"ProductCategoryCd"
                    ,"ProductCategoryNm"
                    ,"PaymentDocumentYearNbr"
                    ,"PaymentDocumentWorkWeekNbr"
                    ,"PaymentDocumentQuarterNbr"
                    ,"PaymentDocumentMonthNbr"
                    ,"PaymentDocumentDt"
                    ,"InvoicePaymentTermsCd"
                    ,"InvoicePaymentTermsNm"
                    ,"CostCenterCd"
                    ,"CostCenterNm"
                    ,"CompanyCd"
                    ,"PaidTransactionAmt"
                    ,"EarnedDiscountTransactionCurrencyAmt"
                    ,"DiscountLostTransactionCurrencyAmt"
            """.format(payment_year=authorization_year)
        else:
            pickle_file_path = ''
            query = ""

        start_time = time()
        if os.path.isfile(pickle_file_path):
            df = pd.read_pickle(pickle_file_path)
        else:
            df = queryHANA1(query, single_sign_on=True)
            df.to_pickle(pickle_file_path)
        print('Retrieved {0} rows from {1}'.format(df.shape[0], table))
        print("--- %s seconds ---" % (time() - start_time))

        # Transform data
        if table == 'Base.PurchaseOrderDetailsQueryHana':
            # format date columns
            df['PurchaseDocumentLineCreateDt'] = pd.to_datetime(df['PurchaseDocumentLineCreateDt'], format='%m/%d/%Y', errors='coerce').dt.date

            # format float columns
            for col in ['CommitsCurrencyTransactionAmt', 'InvoicedAgainstCommitsUSDAmt', 'PaidAgainstCommitsUSDAmt',
                        'CommitsCurrencyTransactionAmtSum', 'PaidAgainstCommitsUSDAmtSum', 'InvoicedAgainstCommitsUSDAmtSum']:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif table == 'Base.PaidDetailQuery':
            # format date columns
            for col in ['PurchaseDocumentUpdateDt', 'PurchaseDocumentCreateDt', 'PaymentDocumentDt']:
                df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce').dt.date

            # format float columns
            for col in ['PaidTransactionAmt', 'EarnedDiscountTransactionCurrencyAmt', 'DiscountLostTransactionCurrencyAmt',
                        'PaidAmt', 'DiscountLostAmt', 'EarnedDiscountAmt', 'CommitsAmt']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # Load data to SQL
        start_time = time()
        print('Loading {0} data into SQL in {1} table...'.format(authorization_year, table))
        success, error_msg = uploadDFtoSQL(table=table, data=df, truncate=truncate_var, chunk_size=5000)  # old Helper Function
        if success:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
        else:
            print(error_msg)
        print("--- %s seconds ---" % (time() - start_time))

    print("--- %s seconds total ---" % (time() - initial_start_time))
