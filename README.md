Python Repository
=====

## Installing Python

Follow the instruction videos on the [GSC_Analytics Teams](https://intel.sharepoint.com/:f:/r/sites/Self-ServiceAnalyticsTeams/Shared%20Documents/General/Instructions%20Documents?csf=1&web=1&e=sNcbTV) to download and install python 3.9.
Once Python has been installed, run the following command(s) to import all package dependencies in this repository.
```
cd C:/GitHub/operations-python
pip install -r Requirements.txt
```

---
## Reuseable Functions

This repository contains the following reusable functions. Click on the Function name to jump to the documentation for each.

<b>Extract</b> (Reading data from other sources):

| **Data Source**  | **Function**                                                            | **Notes**                                                        |
|------------------|-------------------------------------------------------------------------|------------------------------------------------------------------|
| Email            | Helper_Functions.[downloadEmailAttachment](#downloadEmailAttachment)    |                                                                  |
| Excel file       | Helper_Functions.[loadExcelFile](#loadExcelFile)                        | Supports Excel loads on both Shared Drives and SharePoint Online |
| CSV file         | pandas.[read_csv](#readCSV)                                             |                                                                  |
| SharePoint List  | Helper_Functions.[loadSharePointList](#loadSPList)                      |                                                                  |
| SQL Server       | Helper_Functions.[querySQL](#querySQL)                                  |                                                                  |
| SAP HANA (IDP)   | Helper_Functions.[queryHANA](#queryHANA)                                |                                                                  |
| Teradata (EDW)   | Helper_Functions.[queryTeradata](#queryTeradata)                        |                                                                  |
| SSAS (Tabular)   | Helper_Functions.[querySSAS](#querySSAS)                                |                                                                  |
| Salesforce       | Helper_Functions.[querySalesforce](#querySalesforce)                    |                                                                  |
| Smartsheet       | Helper_Functions.[readSmartsheet](#readSmartsheet)                      |                                                                  |
| Intel API Portal | Helper_Functions.[queryAPIPortal](#queryAPIPortal)                      |                                                                  |
| OData Feed       | Helper_Functions.[queryOdata](#queryOdata)                              |                                                                  |

<b>Transform</b> (manipulate data):
- [Pandas Cheat Sheet](https://github.com/intel-innersource/frameworks.business.analytics.python.operations-python/blob/main/Documentation/PandasCheatSheet.md)

<b>Load</b> (Writing/Inserting data into SQL Database):
- Helper_Functions.[map_columns](#mapColumns)
- Helper_Functions.[getLastRefresh](#getLastRefresh)
- Helper_Functions.[uploadDFtoSQL](#uploadDFtoSQL)
- Helper_Functions.[executeSQL](#executeSQL)
- Helper_Functions.[executeStoredProcedure](#executeStoredProcedure)
- Helper_Functions.[truncate_table](#truncateTable)
- Logging.[log](#log)

<b>Other</b>
- Password.[encrypt_password](#encryptPassword)
- Password.[decrypt_password](#decryptPassword)

[//]: # (Markdown comment syntax)

---
<!----><a name="downloadEmailAttachment"></a>
### Download Email Attachment

Download an email attachment using the email subject line and attachment file name and move it to a destination folder.

||Helper_Functions.*downloadEmailAttachment*(destination, email_subject, file=None, exact_match=True, delete_email=False)| 
|---|:---|
|**Parameters:** |__destination__: __str__ <br /> Folder path to store the downloaded Email attachment|
| |__email_subject__: __str__ <br /> Subject of email containing file|
| |__file__: __str__ or __None, optional (default None)__ <br /> File name to search for in email (if None provided, all files in email are moved)|
| |__exact_match__: __bool, optional (default True)__  <br /> If True, only email attachments whose file name matches the provided file name exactly will be downloaded. If False, download email attachments whose file name contains the provided file name text. Use False when file name is dynamic (i.e. date is appended to end of file each month)|
| |__delete_email__: __bool, optional (default False)__  <br /> If True, moves email to Deleted Items folder after downloading attachment(s). If False, marks email as Read but leaves it in current inbox|
|**Returns:**|__file_exists__: __bool__ <br /> If the file was successfully downloaded to destination|

#### Example

```python
from Helper_Functions import downloadEmailAttachment

destination_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\SSC\Memory"
excel_file = 'example.xlsx'

if downloadEmailAttachment(destination_path, 'Test Email', excel_file, exact_match=True, delete_email=False):
    print('Successfully moved email attachment {} to {}'.format(excel_file, destination_path))
```

---
<!----><a name="loadExcelFile"></a>
### Read Excel File

Read Excel sheet(s) into Pandas DataFrame for Python consumption.

<table>
  <tbody>
    <tr>
      <th></th>
      <th align="left">Helper_Functions.<i>loadExcelFile</i>(file_path, sheet_name=None, header_row=0, na_values=None, keep_default_na=True, credentials=None, last_upload_time=None)</th>
    </tr>
    <tr>
      <td valign="top"><b>Parameters:</b></td>
      <td><b>file_path</b>: <b>str</b> <br /> Full path to Excel file (supports both local files reachable by Operational Analytics team system account and SharePoint Online Excel file URLs) </td>
    </tr>
    <tr>
      <td></td>
      <td><b>sheet_name</b>: <b>str</b> or <b>list[str]</b> or <b>int</b> or <b>None, optional (default None)</b> <br /> Name of sheet(s) to read from Excel file. Can provide sheet number (zero-indexed) instead. Default (sheet_name=None) reads all sheets in file </td>
    </tr>
    <tr>
      <td></td>
      <td><b>header_row</b>: <b>int</b> or <b>list[int]</b> or <b>None, optional (default 0)</b> <br /> Row(s) that header begins on, 0-indexed for Python. List of two ints implies multi-row header. Note multi-row headers can only be max two rows. Use <i>None</i> if no header. Default (header_row=0) treats first row as header </td>
    </tr>
    <tr>
      <td></td>
      <td><b>na_values</b>: <b>list[str]</b> or <b>None, optional (default None)</b> <br /> Additional strings to recognize as Not a Number (NaN). By default the following values are interpreted as NaN: '', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '&lt;NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null'. See <a href="https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html#:~:text=rows%20to%20parse.-,na_values,-scalar%2C%20str%2C%20list">Pandas read_excel Docs</a> for details. If you wish to only have your na_values provided be treated as NaN, use keep_default_na flag=True as below. </td>
    </tr>
    <tr>
      <td></td>
      <td><b>keep_default_na</b>: <b>bool, optional (default True)</b> <br /> Whether or not to include the default NaN values when parsing the data. Depending on whether na_values is passed in, the behavior is as follows:
        <ul>
          <li>If <i>keep_default_na</i> is True, and <i>na_values</i> are specified, <i>na_values</i> is appended to the default NaN values used for parsing</li>
          <li>If <i>keep_default_na</i> is True, and <i>na_values</i> are not specified, only the default NaN values are used for parsing.</li>
          <li>If <i>keep_default_na</i> is False, and <i>na_values</i> are specified, only the NaN values specified <i>na_values</i> are used for parsing.</li>
          <li>If <i>keep_default_na</i> is False, and <i>na_values</i> are not specified, no strings will be parsed as NaN.</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td></td>
      <td><b>credentials</b>: <b>dict</b> or <b>None, optional (default None)</b> <br /> SharePoint Online site credentials (from <a href="https://portal.azure.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/~/RegisteredApps">Azure Active Directory App registration</a>) as a python dictionary. Must include keys <i>client_id</i> and <i>client_secret</i>. Default (credentials=None) uses Operational Analytics team's client_id and client_secret from PAM Safe </td>
    </tr>
    <tr>
      <td></td>
      <td><b>last_upload_time</b>: <b>datetime</b> or <b>None, optional (default None)</b> <br /> Date/time when the file was last uploaded to the database. If present, this function will automatically check the last modified datetime from a file before reading, then if last_upload_date >= last_modified_date, the file read is skipped. Default (last_upload_date=None) assumes the file has never been uploaded before and will automatically read it. </td>
    </tr>
    <tr>
      <td valign="top"><b>Returns:</b></td>
      <td><b>df</b>: <b>Pandas DataFrame</b> or <b>dict of Pandas DataFrames</b> <br /> When only one sheet specified, return Pandas DataFrame of the sheet. When multiple sheets specified, return dictionary of each sheet as separate DataFrames with the key of Sheet name </td>
    </tr>
  </tbody>
</table>

See [Pandas read_excel Docs](https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html#:~:text=rows%20to%20parse.-,na_values,-scalar%2C%20str%2C%20list) for details|

#### Example -- Read Excel file stored on VM Share

```python
import os
from Helper_Functions import loadExcelFile

shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\SSC\Memory"
excel_sheet_name = 'sheet1'

(_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
for excel_file in file_list:
    if not excel_file.startswith('~'):  # ignore open files
        if excel_file == 'Expected File Name': # explict file comparison 
        # if 'Some Partial Str' in excel_file: # implicit file comparison (i.e. matching a file that has date appended to it)
            df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name)
            print(df.columns)
```

#### Example 2 -- Read Excel file stored on SharePoint Online

```python
from Helper_Functions import getLastRefresh, loadExcelFile

sharepoint_excel_link = "https://intel.sharepoint.com/:x:/r/sites/Self-ServiceAnalyticsTeams/Shared%20Documents/General/SCDA%20AGS_Roles_Entitlements.xlsx?d=w252db8fe7f2e4f0ba8d2e7ebd08407f8&csf=1&web=1&e=T3oiZo"
sheet_name = 'Sheet1'
project_name = 'Example'
data_area = 'AGS'

last_refreshed = getLastRefresh(project_name='Example', data_area='AGS')  # get the last upload date/time from the sql database
df = loadExcelFile(sharepoint_excel_link, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
if len(df.index) == 0:  # DataFrame is empty
    print('Skipped {0} as it has not been modified since the last upload.'.format(data_area))
else:
    print(df.columns)
```

---
<!----><a name="readCSV"></a>
### Load Data from a CSV file
See a full list of parameters on the [Pandas Docs](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)

#### Example 

```python
import os
import pandas as pd

shared_drive = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\ATS_SCCI_Data"
csv_file = 'Test Supplier Capacities.csv'

df = pd.read_csv(os.path.join(shared_drive, csv_file), delimiter=',', quotechar='"', header=0)
```

---
<!----><a name="loadSPList"></a>
### Read SharePoint Online List

Read SharePoint Online List into Pandas DataFrame for Python consumption.\
Prerequisite: Please remember to contact the SharePoint site owner and set up access for GSCAnalyticsTeams (SCES OA) app to read/write. [Documentation Here](https://intel.sharepoint.com/:w:/r/sites/Self-ServiceAnalyticsTeams/Shared%20Documents/General/Instructions%20Documents/Connect%20Python%20to%20SharePoint%20Online.docx?d=wcf8cf93b31464795b12536a5b951e3f4&csf=1&web=1&e=Euxg18)

||Helper_Functions.*loadSharePointList*(sp_list, list_name, credentials=None, decode_column_names=True, remove_metadata=True, last_upload_time=None)| 
|---|:---|
|**Parameters:** |__sp_list__: __str__ <br /> Full path to SharePoint Online site|
| |__list_name__: __str__ <br /> Name of the SharePoint List to read|
| |__credentials__: __dict__ or __None, optional (default None)__ <br /> SharePoint Online site credentials (from [Azure Active Directory App registration](https://portal.azure.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/~/RegisteredApps)) as a python dictionary. Must include keys *client_id* and *client_secret*. Default (credentials=None) uses Operational Analytics team's client_id and client_secret from PAM Safe|
| |__decode_column_names__: __bool, optional (default True)__ <br /> If the function should decode the URL encodings in the column names pulled from SharePoint. Default (decode_column_names=True) removes URL encodings from column names prior to returning DataFrame|
| |__remove_metadata__: __bool, optional (default True)__ <br /> If the function should remove the metadata columns that are present in the SharePoint List. Default (remove_metadata=True) removes all metadata columns that are automatically generated when creating a new SharePoint List|
| |__last_upload_time__: __datetime__ or __None, optional (default None)__ <br /> Date/time when the SharePoint List was last uploaded to the database. If present, this function will automatically check the last modified datetime from the List before reading, then if last_upload_date >= last_modified_date, the List read is skipped. Default (last_upload_date=None) assumes the List has never been uploaded before and will automatically read it. |
|**Returns:**|__df__: __Pandas DataFrame__ <br /> Returns a Pandas DataFrame representation of SharePoint List|

#### Example

```python
from Helper_Functions import loadSharePointList

config = {
    'client_id': '<your_sharepoint_client_id>',
    'client_secret': '<your_sharepoint_client_secret>'
}

sp_site = 'https://intel.sharepoint.com/sites/Self-ServiceAnalyticsTeams'
list_name = 'NSI Repository'

df = loadSharePointList(sp_site=sp_site, list_name=list_name, credentials=config, decode_column_names=True, remove_metadata=False)
print(df.shape)  # prints a tuple with the number of rows and columns
```

---
<!----><a name="querySQL"></a>
### Load Data from SQL Server

Load data from SQL Server database into a Pandas DataFrame

||Helper_Functions.*querySQL*(statement, driver=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> SQL statement to execute to return a table|
| |__driver__: __str, optional (default None)__ <br /> Which SQL Server driver to use. Driver must be installed on your machine to use (check 32-bit ODBC Administrator to see full list of drivers). [Here](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) is a list of all the supported drivers and their compatibilities. Default (None) driver info is pulled in from Project_params.py file|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__success_bool, df, error_msg__: __[bool, Pandas DataFrame, str]__ <br /> First value states if the SQL query executed properly. True for successful query, False otherwise. Second value is a Pandas DataFrame of the result of the query. Third value is the error message text. As such, this value will be None if the query succeeded|

#### Example -- Import data to use within Python

```python
from Helper_Functions import querySQL

query = """SELECT * 
           FROM dbo.Intel_Calendar"""

query_succeeded, df, error_msg = querySQL(query)
if query_succeeded:
    print(df.columns)  # print list of columns returned from query
    print(df.head(10))  # print first ten rows of DataFrame
else:
    print(error_msg)
```

#### Example 2 -- Copy data from one database to another

```python
from Helper_Functions import querySQL, uploadDFtoSQL
from Project_params import params

# initialize variables
source_server = 'sql1717-fm1-in.amr.corp.intel.com,3181'
source_db = 'gsmdw'
table = 'sbg.SubstrateAllocationForecast'

# generate query (or enter your own query here)
query = """SELECT *
           FROM {}""".format(table)

query_succeeded, df, error_msg = querySQL(query, server=source_server, database=source_db)  # load data into dataframe
if query_succeeded:
    insert_succeeded, error_msg = uploadDFtoSQL(table, df, truncate=True)  # upload dataframe to SQL
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], table, source_server, params['GSMDW_SERVER']))
    else:
        print(error_msg)
else:
    print(error_msg)
```

---
<!----><a name="queryHANA"></a>
### Load Data from SAP HANA

Load data from SAP HANA database into a Pandas DataFrame

||Helper_Functions.*queryHANA*(statement, environment='Production', credentials=None, single_sign_on=False)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> SAP HANA query to execute to return a table|
| |__environment__: __str, optional (default 'Production')__ <br /> Which SAP HANA environment configuration to use. See "Environments" column in table below for list of acceptable values. Default ('Production') uses the Production SAP Hana server and port|
| |__credentials__: __dict__ or __None, optional (default None)__ <br /> HANA DB account credentials as a python dictionary. Must include keys *username* and *password*. Default (credentials=None) uses Operational Analytics team's HANA Database Account SYSH_SCES_OPSRPT|
| |__single_sign_on__: __bool,  optional (default False)__ <br /> If the function should use Windows Single Sign-on as credentials to connect to SAP HANA database. Default (single_sign_on=False) uses Operational Analytics team's HANA Database Account SYSH_SCES_OPSRPT|
|**Returns:**|__df__: __[Pandas DataFrame]__ <br /> Pandas DataFrame of the result of the query

#### SAP HANA Environments
| Environment          |  SID  | Server Name / Vanity Host Name | Instance Number | Custom Port | Hana Web IDE URL                             |
|----------------------|-------|--------------------------------|-----------------|-------------|----------------------------------------------|
| **Production**       |**EHP**| **sapehpdb.intel.com**         | **10**          | **31015**   | **https://sapehpdb.intel.com/sap/hana/ide/** |
| Pre-DEV              |  EH1  | sapeh1db.intel.com             | 37              | 33715       | https://sapeh1db.intel.com/sap/hana/ide      |
| Development          |  NBI  | sapnbidb.intel.com             | 11              | 31115       | https://sapnbidb.intel.com/sap/hana/ide/     |
| Benchmark            |  EHB  | sapehbdb.intel.com             | 10              | 31015       | https://sapehbdb.intel.com/sap/hana/ide/     |
| Production Support   |  EHS  | sapehsdb.intel.com             | 13              | 31315       | https://sapehsdb.intel.com/sap/hana/ide/     |
| QA/CONS              |  EHC  | sapehcdb.intel.com             | 12              | 31215       | https://sapehcdb.intel.com/sap/hana/ide/     |

#### Example

```python
from Helper_Functions import queryHANA

query = """SELECT *
           FROM "_SYS_BIC"."intel.scidp.supply.public/MaterialsPlanningCurrentWeekMfgOrderView"
        """

df = queryHANA(query, environment='Production')
print(df)
```

---
<!----><a name="queryTeradata"></a>
### Load Data from Teradata

Load data from Teradata database into a Pandas DataFrame

||Helper_Functions.*queryTeradata*(statement, server='TDPRD1.intel.com', credentials=None)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> Teradata query to execute to return a table|
| |__server__: __str, optional (default 'TDPRD1.intel.com')__ <br /> Which Teradata server to connect to. Default ('TDPRD1.intel.com') is the Production Teradata server|
| |__credentials__: __dict__ or __None, optional (default None)__ <br /> Teradata DB credentials as a python dictionary. Must include keys *username* and *password*. Default (credentials=None) uses Operational Analytics team's DB Account APPL_GSM_BI_01|
|**Returns:**|__df__: __[Pandas DataFrame]__ <br /> Pandas DataFrame of the result of the query

#### Example

```python
from Helper_Functions import queryTeradata

query = """SELECT TOP 10 *
           FROM Calendar.v_clndr_day
           """

df = queryTeradata(query)
print(df)
```

---
<!----><a name="querySSAS"></a>
### Load Data from SQL Server Analysis Services Tabular Model

Load data from SSAS Tabular Model into a Pandas DataFrame

||Helper_Functions.*querySSAS*(statement, server, model)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> DAX query to execute to return a table|
| |__server__: __str__ <br /> Which Analysis Services server to connect to|
| |__model__: __str__ <br /> Which model (database) on the Analysis Services server to connect to|
|**Returns:**|__df__: __[Pandas DataFrame]__ <br /> Pandas DataFrame of the result of the query|

#### Example -- Import entire table from SSAS Model

```python
from Helper_Functions import querySSAS

server_name = "GSM_Supplier_Safety.intel.com"
cube_name = "GSM_Supplier_Safety"

query = """EVALUATE MQI"""  # MQI is a table within the Supplier Safety model

df = querySSAS(query, server=server_name, model=cube_name)
print(df)
```

#### Example 2 -- Use DAX expression to pull data from SSAS Model

```python
from Helper_Functions import querySSAS

server_name = "GSM_SupplyChainMetrics.intel.com"
cube_name = "GSM_SupplyChainMetrics"

query = """EVALUATE
            SELECTCOLUMNS(
                FILTER(
                    SUMMARIZE(Spends,
                        Calendar[Rolling1YrFlg],
                        Supplier[Global Parent Supplier ID],
                        Commodity[High Level Org],
                        Commodity[Sourcing Org],
                        "PaidAmt", SUM(Spends[Paid Amount])
                    ),
                    Calendar[Rolling1YrFlg]  = "True" && [PaidAmt] > 0
                    && Supplier[Global Parent Supplier ID] <> Blank()
                ),
                "Global", Supplier[Global Parent Supplier ID],
                "Org", Commodity[High Level Org],
                "S_Org", Commodity[Sourcing Org],
                "PaidAmt", [PaidAmt]
            )
            ORDER BY [Global] ASC, [PaidAmt] DESC
	    """

df = querySSAS(query, server=server_name, model=cube_name)
print(df)
```

---
<!----><a name="querySalesforce"></a>
### Load data from Salesforce

Load data from Salesforce into a Pandas DataFrame

||Helper_Functions.*querySalesforce*(statement, credentials=None)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> Salesforce Object Query Language (SOQL) statement to execute to return a table|
| |__credentials__: __dict__ or __None, optional (default None)__ <br /> Salesforce credentials as a python dictionary. Must include keys *username*, *password*, and *security_token*. Default (credentials=None) uses Operational Analytics team's ICAP production account|
|**Returns:**|__df__: __Pandas DataFrame__ <br /> Pandas DataFrame of the result of the query|

#### Example

```python
from Helper_Functions import querySalesforce

credentials = {'username': 'YOUR USERNAME', 
               'password':'YOUR PASSWORD', 
               'security_token': 'YOUR SECURITY TOKEN'
               }

query = """select name, owner.name 
           from account 
           where ICAP_Investment_Status__c='Active' 
           order by name asc
           """

df = querySalesforce(statement=query, credentials=credentials)
print(df)
```

---
<!----><a name="readSmartsheet"></a>
### Read Smartsheet

Read Smartsheet into Pandas DataFrame for Python consumption. Rows that contain all NULLs will be automatically excluded.

||Helper_Functions.*readSmartsheet*(sheet_name, access_token=accounts['Smartsheet'].password, doc_type="Sheet", page_size=100, last_upload_time=None)| 
|---|:---|
|**Parameters:** |__sheet_name__: __str__ <br /> Name of Smartsheet Sheet or Summary Report to read|
| |__access_token__: __str, optional (default accounts['Smartsheet'].password)__ <br /> Smartsheet credentials (specific to a given Smartsheet user). See steps below for how to obtain this token. Default (accounts['Smartsheet'].password) uses the Operational Analytics team's faceless account's, sys_SCdata, credentials to connect|
| |__doc_type__: __str, optional (default "Sheet")__ <br /> Which document type you want to access on Smartsheet. Either "Sheet" or "Summary Report"|
| |__page_size__: __int, optional (default 100)__ <br /> Used for API pagination. The maximum number of items to return per page. The largest *page_size* supported as of May 2021 is 2500 [Reference: [Smartsheet Community Post](https://community.smartsheet.com/discussion/79517/is-there-a-different-max-number-for-number-of-rows-in-a-report)]. Default (page_size=100) reads 100 rows per page in the Smartsheet Sheet|
| |__last_upload_time__: __datetime__ or __None, optional (default None)__ <br /> Date/time when the Smartsheet Sheet was last uploaded to the database. If present, this function will automatically check the last modified datetime from the Sheet before reading, then if last_upload_date >= last_modified_date, the Sheet read is skipped. Default (last_upload_date=None) assumes the Sheet has never been uploaded before and will automatically read it.|
|**Returns:**|__df__: __Pandas DataFrame__ <br /> Returns a Pandas DataFrame representation of the sheet|

#### How to grant Operational Analytics team access to Smartsheet collateral:
Owner of a Smartsheet Workspace will need to grant "Viewer" access to **sys_SCdata@intel.com** account.

---
#### How to obtain API Access Token from Smartsheet (individual user access):
1. If you don't already have a license, you will need to apply for the AGS entitlement [Smartsheet Licensed User Access](https://ags.intel.com/identityiq/ui/rest/redirect?rp1=/accessRequest/accessRequest.jsf&rp2=accessRequest/manageAccess/add?filterKeyword=Smartsheet%20Licensed%20User%20Access%26quickLink=Request%20Access). Cost is $600 per person, billed to your Cost Center annually in July.
2. Login to your [Smartsheet account](https://app.smartsheet.com/home). Choose "Sign in with your company account" to use Single Sign-on.
3. In the bottom left corner, click on your profile and then “Apps & Integrations...”.
4. Navigate to the "API Access" tab on the bottom left in your Personal Settings window.
5. Click “Generate new access Token” and give your access token a name. Example name: "connect-sdk-test".
6. A token will be generated against the name. Don't forget to copy it as it won't be available later on.
7. Finally, you will see a screen with a list of your current tokens which means your access token has been created successfully.

---
#### Example

```python
from Helper_Functions import readSmartsheet

access_token = "xxx"
sheet_name = "2021 Supply Chain Capability Portfolio"

df = readSmartsheet(sheet_name, access_token, doc_type="Sheet")
```

---
<!----><a name="queryAPIPortal"></a>
### Load Data from Intel API Portal

Load data from [Intel API Portal](https://api-portal-internal.intel.com/) into Pandas DataFrame

||Helper_Functions.*queryAPIPortal*(url, client_id=accounts['HANA'].client_id, client_secret=accounts['HANA'].client_secret)| 
|---|:---|
|**Parameters:** |__url__: __str__ <br /> A working REST URL to send an HTTP request for the Intel API Portal, should end with '$format=JSON' in order for the function to successfully return a DataFrame|
| |__client_id__: __str,  optional (default accounts['HANA'].client_id)__ <br /> The client id from the app you are using to send the HTTP request to the API. Default (accounts['HANA'].client_id) is the OAA team's APIGEE app|
| |__client_secret__: __str,  optional (default accounts['HANA'].client_secret)__ <br /> The client secret from the app you are using to send the HTTP request to the API. Default (accounts['HANA'].client_secret) is the OAA team's APIGEE app|
|**Returns:**|__df__: __[Pandas DataFrame]__ <br /> Panda**s DataFrame of the result of the API call**

#### Example

```python
from Helper_Functions import queryAPIPortal

df = queryAPIPortal(url="https://apis-internal-sandbox.intel.com/ilm/issue/v1/issues?$count=2&$format=JSON")
```

#### Example 2 -- Query with multiple filters

```python
from Helper_Functions import queryAPIPortal

df = queryAPIPortal(url="https://apis-internal-sandbox.intel.com/item/v2/user-defined-attribute-item-revision-details?$filter=\"UserDefinedAttributeId\" IN ('90105','90110','90152')&$format=JSON")
```

---
<!----><a name="queryOdata"></a>
### Load Data from OData feed

Load data from Odata feed into python dictionary

||Helper_Functions.*queryOdata*(url, username=None, password=None)| 
|---|:---|
|**Parameters:** |__url__: __str__ <br /> A working REST URL to send an HTTP request to an Odata feed, should return a JSON object order for the function to successfully return a dictionary|
| |__username__: __str,  optional (default None)__ <br /> Basic Authentication username. Default (None) uses Windows Credentials to connect to Odata feed|
| |__password__: __str,  optional (default None)__ <br /> Basic Authentication password. Default (None) uses Windows Credentials to connect to Odata feed|
|**Returns:**|__success_bool, data, error_msg__: __[bool, dict, str]__ <br /> First value states if the Odata query executed properly. True for successful query, False otherwise. Second value is a python dictionary of the result of the query. Third value is the error message text. As such, this value will be None if the query succeeded|

#### Example

```python
import pandas as pd
from Helper_Functions import queryOdata

query_succeeded, data, error_msg = queryOdata(url="https://certtracker.intel.com/odata/SubstrateSupplierFactoryQuals")
if query_succeeded:
    print(data)  # Copy/paste the output of this line in a JSON viewer online to determine the data structure of your result before calling the next line.
    df = pd.DataFrame(data['value'])  # Convert a list inside the dictionary to DataFrame. Note your dictionary might not have a key called "value". For example, the key may be called "contents" or "elements" instead.
```

---
<!----><a name="mapColumns"></a>
### Map Columns

Function to compare SQL table column definitions with a DataFrame's contents. Used in the _uploadDFtoSQL_ function to provide more robust error messages on SQL insert failures.

||Helper_Functions.*map_columns*(table, df, display_result=True, export_result_destination=None, sql_columns=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__table__: __str__ <br /> Name of a SQL table|
| |__df__: __Pandas DataFrame__ or __None__ <br /> Data to be inserted into a SQL table. If no data provided, then still returns SQL table column mapping|
| |__display_result__: __bool, optional (default True)__ <br /> Whether to print the column mapping or not. Default (display_result=True) prints the mapping|
| |__export_result_destination__: __str, optional (default None)__ <br /> Full path (including or not including file name) of where to export the column mapping. File name must be .csv format if included. If folder (directory) is given then creates file called `TABLE_NAME_Mapping.csv`. Default (export_result_destination=None) does not export the mapping|
| |__sql_columns__: __list__ or __None, optional (default None)__ <br /> List of columns from SQL table to include. Default (None) attempts to use all columns|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__column_dtypes__: __dict__ <br /> Representation of the column mapping between the source (DataFrame) and destination (SQL table).|

#### Example -- Lookup data type and size of columns in a SQL table

```python
from Helper_Functions import map_columns

map_columns(table='audit.PythonUnitTest', df=None, display_result=True)

# Output: 
# |Destination Column Name                      |Destination Data Type    |Destination Max Length   |
# |---------------------------------------------|-------------------------|-------------------------|
# |RequestID                                    |float                    |None                     |
# |EventName                                    |nvarchar                 |255                      |
# |TimeStamp                                    |float                    |None                     |
# |LoadDtm                                      |datetime                 |None                     |
# |LoadBy                                       |nvarchar                 |25                       |
```

#### Example 2 -- Compare data type and size of the columns in a DataFrame with corresponding columns in a SQL table

```python
import os
from datetime import datetime
import pandas as pd
from Helper_Functions import map_columns

example = [{'RequestID': 1,
            'EventName': 'Event 1',
            'TimeStamp': datetime.today().timestamp(),
            'LoadDtm': datetime.now().replace(microsecond=0),  # SQL Server only stores 3 digit microsecond in the [datetime] object
            'LoadBy': 'AMR\\' + os.getlogin().upper()
            }]

df = pd.DataFrame(example)

map_columns(table='audit.PythonUnitTest', df=df, display_result=True)

# Output: 
# |Source Column Name                           |Source Data Type         |Source Max Length        |Destination Column Name                      |Destination Data Type    |Destination Max Length   |
# |---------------------------------------------|-------------------------|-------------------------|---------------------------------------------|-------------------------|-------------------------|
# |RequestID                                    |int64                    |1                        |RequestID                                    |float                    |None                     |
# |EventName                                    |object                   |7                        |EventName                                    |nvarchar                 |255                      |
# |TimeStamp                                    |float64                  |nan                      |TimeStamp                                    |float                    |None                     |
# |LoadDtm                                      |datetime64[ns]           |nan                      |LoadDtm                                      |datetime                 |None                     |
# |LoadBy                                       |object                   |12                       |LoadBy                                       |nvarchar                 |25                       |
```

---
<!----><a name="getLastRefresh"></a>
### Get Last Refresh Date

Function to get Last Refresh Datetime from SQL audit table

||Helper_Functions.*getLastRefresh*(project_name, data_area)| 
|---|:---|
|**Parameters:** |__project_name__: __str__ <br /> Name of project that is associated with the Python module|
| |__data_area__: __str__ <br /> Affected GSC organzation's data area|
|**Returns:**|__last_refreshed__: __Datetime or None__ <br /> Datetime representing the last refresh (as pulled from the audit.processing_log or None when unable to determine last refresh date time.|

#### Example

```python
from Helper_Functions import getLastRefresh

last_refreshed = getLastRefresh(project_name='SPARC Deliverables', data_area='Audit')
print(last_refreshed)
```

---
<!----><a name="uploadDFtoSQL"></a>
### Upload Pandas DataFrame to SQL Server

Insert Pandas DataFrame into SQL Server database table

||Helper_Functions.*uploadDFtoSQL*(table, data, columns=None, categorical=None, truncate=True, chunk_size=10000, driver=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__table__: __str__ <br /> Name of the destination table within the SQL database|
| |__data__: __Pandas DataFrame__ <br /> Data (in tabular form) to be uploaded to SQL database|
| |__columns__: __list of str__ or __None, optional (default None)__ <br /> Ordered list of columns within destination table matching the order of the Pandas DataFrame columns. Can be a subset of the columns (i.e. you are only loading some columns and will populate the rest of the columns later). Default (columns=None) assumes DataFrame contains all destination table columns and the order matches the columns as they appear in the SQL Server|
| |__categorical__: __list of str__ or __None, optional (default None)__ <br /> List of Pandas DataFrame column names that should be converted to categorical (text) for SQL upload. Use this for columns like "Supplier ID" or "Part Number" that format as scientific notation when uploading as a number. Default (columns=None) does not do any formatting on these categorical numeric columns|
| |__truncate__: __bool, optional (default True)__ <br /> If the destination table should be truncated prior to loading the new data. Default (truncate=True) truncates the table prior to loading|
| |__chunk_size__: __int, optional (default 10000)__ <br /> Number of rows to insert at a given time. Default (chunk_size=10000) does not chunk if less than 10000 rows in the dataset and will attempt to insert all data at once. If the dataset is greater than 10000 rows, chunking will be used. Use this when you need to insert hundreds of thousands of rows or more of data. Enter chunk_size=0 if you **do not** want to use chunking
| |__driver__: __str, optional (default None)__ <br /> Which SQL Server driver to use. Driver must be installed on your machine to use (check 32-bit ODBC Administrator to see full list of drivers). [Here](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) is a list of all the supported drivers and their compatibilities. Default (None) driver info is pulled in from Project_params.py file|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__insert_succeeded, error_msg__: __[bool, str]__ <br /> First value states if the SQL insert statement executed properly. True for successful insert, False otherwise. Second value is the error message text. As such, this value will be None if insert succeeded|

#### Example

```python
import pandas as pd
from Helper_Functions import uploadDFtoSQL

# initialize variable
table = 'stage.stg_MSS_MI_CAPEX'

# read data from source
df = pd.DataFrame()  # import data into a DataFrame here (can utilize other helper functions)

insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, columns=None, categorical=None, truncate=False)
if insert_succeeded:
    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
else:
    print(error_msg)
```

---
<!----><a name="executeSQL"></a>
### Execute query or statement on SQL Server

Execute a statement on SQL Server remotely

||Helper_Functions.*executeSQL*(statement, driver=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__statement__: __str__ <br /> SQL statement to execute|
| |__driver__: __str, optional (default None)__ <br /> Which SQL Server driver to use. Driver must be installed on your machine to use (check 32-bit ODBC Administrator to see full list of drivers). [Here](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) is a list of all the supported drivers and their compatibilities. Default (None) driver info is pulled in from Project_params.py file|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__success_bool, error_msg__: __[bool, str]__ <br /> First value states if the statement executed properly. True for successful execution, False otherwise. Second value is the error message text. As such, this value will be None if procedure execution succeeded|

#### Example -- Trigger a DELETE statement on the SQL Server

```python
from Helper_Functions import executeSQL
from logging import log

project_name = 'Example Code'
memory_tech = 'LPDDR3'

delete_statement = """DELETE FROM ssc.MSS_MI_Memory_Pricing WHERE [Memory_Tech] = '{}'""".format(memory_tech)
delete_success, error_msg = executeSQL(delete_statement)
if not delete_success:
    print(error_msg)
    log(delete_success, project_name=project_name, data_area=memory_tech, error_msg=error_msg)
else:
    print('Successfully executed the query!')
```

#### Example 2 -- Call a UPDATE statement on the SQL Server

```python
from Helper_Functions import executeSQL

upi = '2000-106-050'

update_statement = """UPDATE PBI.t_Global_MorpatSubstratePathFindingAttributes 
                      SET [curr_ind] = 'N'
                      WHERE [assembly_upi] = '{}'
                    """.format(upi)

update_succeeded, error_msg = executeSQL(update_statement)
if update_succeeded:
    print('Successfully executed the update!')
else:
    print(error_msg)
```

---
<!----><a name="executeStoredProcedure"></a>
### Execute Stored Procedure on SQL Server

Execute a Stored Procedure on SQL Server remotely. Note: this function assumes that the return value of the stored procedure is 0 (zero) for success (this is the default for SQL Server).

||Helper_Functions.*executeStoredProcedure*(procedure_name, *parameters, driver=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__procedure_name__: __str__ <br /> Name of the stored procedure to execute within the SQL database|
| |__parameters__: __str__ or __list of str, optional__ <br /> Any parameters for the stored procedure|
| |__driver__: __str, optional (default None)__ <br /> Which SQL Server driver to use. Driver must be installed on your machine to use (check 32-bit ODBC Administrator to see full list of drivers). [Here](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) is a list of all the supported drivers and their compatibilities. Default (None) driver info is pulled in from Project_params.py file|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__success_bool, error_msg__: __[bool, str]__ <br /> First value states if the stored procedure executed properly. True for successful execution, False otherwise. Second value is the error message text. As such, this value will be None if procedure execution succeeded|

#### Example

```sql
CREATE PROCEDURE [dbo].[Example_for_Python]
( @debug char(1) = 'N' )
AS

BEGIN
	IF (@debug <> 'Y')
	BEGIN
		RETURN 1  -- a non-zero value evaluates to False in Python, indicating some error has occurred		
	END
	-- This function automatically returns 0 for success, by default in SQL Server, which gets converted to True by Python in the Helper Function
END
```

```python
from Helper_Functions import executeStoredProcedure

proc_name = 'dbo.Example_for_Python'

execute_succeeded, error_msg = executeStoredProcedure(proc_name)  # returns error since default param is 'N'
execute_succeeded, error_msg = executeStoredProcedure(proc_name, 'N')  # returns error
execute_succeeded, error_msg = executeStoredProcedure(proc_name, 'Y')  # returns success
if execute_succeeded:
    print('Successfully executed the {} stored procedure'.format(proc_name))
else:
    print(error_msg)
```

---
<!----><a name="truncateTable"></a>
### Truncate a Table in a SQL Server Database

Truncate a table in a SQL Server Database.

||Helper_Functions.*truncate_table*(table, driver=None, server=None, database=None, username=None, password=None)| 
|---|:---|
|**Parameters:** |__table__: __str__ <br /> Name of the table in SQL which is the target to truncate|
| |__driver__: __str, optional (default None)__ <br /> Which SQL Server driver to use. Driver must be installed on your machine to use (check 32-bit ODBC Administrator to see full list of drivers). [Here](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) is a list of all the supported drivers and their compatibilities. Default (None) driver info is pulled in from Project_params.py file|
| |__server__: __str, optional (default None)__ <br /> Which server to connect to. Default (None) server name is pulled in from Project_params.py file|
| |__database__: __str, optional (default None)__ <br /> Which database to connect to. Default (None) database name is pulled in from Project_params.py file|
| |__username__: __str, optional (default None)__ <br /> Name of SQL Database Account for which to use for SQL Server Authentication. Password must also be provided to use. Default (None) uses Windows Authentication for the current user|
| |__password__: __str, optional (default None)__ <br /> Password to the SQL Database Account provided. Default (None) uses Windows Authentication for the current user|
|**Returns:**|__success_bool, error_msg__: __[bool, str]__ <br /> First value states if the table was truncated properly. True for successful execution, False otherwise. Second value is the error message text. As such, this value will be None if truncate was successful|

#### Example

```python
from Helper_Functions import truncate_table,uploadDFtoSQL

table_name = 'dbo.Example_for_Python'

truncate_succeeded, error_message = truncate_table(table=table_name)
if not truncate_succeeded:
    print(error_message)
else:
    # do some looping with multiple inserts like so
    uploadDFtoSQL(table=table_name, data=df, truncate=False)
```

---
<!----><a name="log"></a>
### Log Success/Failure Message to SQL Server

Abstract logging function that automatically logs either success or failure messages to SQL Server 

||Logging.*log*(sql_stmt_succeeded, project_name, data_area, package_name=os.path.basename(sys.argv[0]), row_count=0, error_msg=None)| 
|---|:---|
|**Parameters:** |__sql_stmt_succeeded__: __bool__ <br /> If the SQL statement was successful|
| |__project_name__: __str__ <br /> Name of project that is associated with the Python module|
| |__data_area__: __str__ <br /> Affected GSC organzation's data area|
| |__package_name__: __str__ <br /> Name of Python module that is executing log function. Default (package_name=os.path.basename(sys.argv[0])) uses the file that was invoked by the original python command as the package name.|
| |__row_count__: __int__ <br /> Number of rows successfully inserted into database table. Default (row_count=0) signifies no rows were inserted|
| |__error_msg__: __pypyodbc.Error__ <br /> Error message text from failed insert statement. Default (error_msg=None) does not display an error message in log|
|**Returns:**||

#### Example

```python
from Helper_Functions import uploadDFtoSQL
from Project_params import params
from Logging import log

# Update parameters for this script
params['EMAIL_ERROR_RECEIVER'].append('brian.zimmerlich@intel.com') # Add a single user to email failure notification for this specific script
params['EMAIL_ERROR_RECEIVER'].extend(['wayne.chen@intel.com', 'jordan.makis@intel.com']) # Add two or more users to email failure notification for this specific script

# import data into DataFrame here and do any manipulations to make it look like the SQL table format

insert_succeeded, error_msg = uploadDFtoSQL(table='stage.stg_MSS_MI_CAPEX', data=df, truncate=False)
log(insert_succeeded, project_name='MSS Market Intelligence', data_area='CAPEX', row_count=df.shape[0], error_msg=error_msg)
if insert_succeeded:
    print('Successfully inserted data into stage.stg_MSS_MI_CAPEX')  # or add files to list of correctly loaded files
else:
    print(error_msg)
```

---
<!----><a name="encryptPassword"></a>
### Encrypt a Password

Function to encrypt a string using Operational Analytics secret key

||Password.*encrypt_password*(password)| 
|---|:---|
|**Parameters:** |__password__: __str__ <br /> Password to be encrypted|
|**Returns:**|__binary_password__: __bytes__ <br /> Binary string representation of the encrypted password|

#### Example

```python
from Password import encrypt_password

encrypted_password = encrypt_password(password="Password123")
print(encrypted_password)
```

---
<!----><a name="decryptPassword"></a>
### Decrypt a Password

Function to decrypt a string using Operational Analytics secret key

||Password.*decrypt_password*(binary_password)| 
|---|:---|
|**Parameters:** |__binary_password__: __bytes__ <br /> Binary string representation of the encrypted password|
|**Returns:**|__password__: __str__ <br /> Password as plain text|

#### Example

```python
from Password import decrypt_password

decrypted_password = decrypt_password(binary_password=b'gAAAAABiIRtdalGV7QNnRU1JFtyRLFaWbAUz8OGGELUERMjpO0_-I3snxnT5DTmfGBj1E5Ez5mdZPKnCVDnBPSWPJMBokBTSFeYsfSK5GJUCHY-RggAACgkwMCaCv6xYGHK7VCbTIHrM')
print(decrypted_password)
```
