__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the Supply Risk Solutions dashboard by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Thrice Daily at 12:15 AM, 8:17 AM, and 4:15 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from time import time, sleep
import shutil
from Helper_Functions import downloadEmailAttachment, loadExcelFile, queryHANA, uploadDFtoSQL, getLastRefresh
from Logging import log, log_warning


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    # initialize variables
    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'BCP SRS Dashboard'

    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\BCP\SRS"

    # Download emailed Excel files from mailbox
    srs_files = ['Sites and Emergency Contacts', 'Site Impact (Latest Responses Last 14 Days)']
    for subject in srs_files:
        file_name = subject + '.xlsx'

        conn_error_msg = ''
        retries = 0
        while retries < 3:
            try:
                print('Attempting to download {} from gsmariba@intel.com'.format(file_name))
                downloadEmailAttachment(shared_drive_folder_path, email_subject=subject, file='.xlsx', exact_match=False, delete_email=True)
                break
            except ConnectionResetError as error:
                print(error)
                conn_error_msg = error
                sleep(30)  # sleep 30 seconds
                retries += 1  # add an additional count to retries

        if retries == 3:  # previous loop was unable to connect to the email after three retries
            log(False, project_name=project_name, data_area=subject, error_msg=conn_error_msg)

    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            ### BEGIN Sites and Emergency Contacts Load ###
            if 'Sites and Emergency Contacts' in excel_file:
                data_area = 'Sites and Emergency Contacts'
                file_path = os.path.join(shared_drive_folder_path, excel_file)
                sheet_name = 'Sites and Emergency Contacts'
                table = 'bcp.SRS_Sites_and_Emergency_Contacts'

                # Extract data from Excel file
                df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=1)

                # Transform data
                df.drop(df.tail(1).index, inplace=True)  # drop last row
                df['Upload_Date'] = datetime.today()
                df['Site Reference'] = [x[:10] if isinstance(x, str) else x for x in df['Site Reference']]  # Fix issue with multiple Supplier ESDIDs assigned to a site

                # Load data to Database
                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['Site Reference'], truncate=True)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')
            ### END Sites and Emergency Contacts Load ###

            ### BEGIN Site Impact Last 14 Days Load  ###
            elif 'Site Impact (Latest Responses Last 14 Days)' in excel_file:
                data_area = 'Site Impact (Latest Responses Last 14 Days)'
                file_path = os.path.join(shared_drive_folder_path, excel_file)
                sheet_name = 'Site Impact (Latest Responses L'
                table = 'bcp.SRS_Site_Impact_14_Day_Responses'

                # Extract data from Excel file
                df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=0)

                # Transform data
                df['Upload_Date'] = datetime.today()
                df['Time Of Response'] = df['Time Of Response'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S UTC') if isinstance(x, str) else x)  # convert string to datetime
                df['Site Reference'] = [x[:10] if isinstance(x, str) else x for x in df['Site Reference']]  # Fix issue with multiple Supplier ESDIDs assigned to a site

                # Load data to Database
                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['Site Reference'], truncate=True)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')
            ### END Site Impact Last 14 Days Load  ###

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    ### BEGIN Site Disaster History Load ###
    data_area = 'Site Disaster History'
    file_path = 'https://intel.sharepoint.com/:x:/r/sites/gscbusinesscontinuity-SRS_Dashboard/Shared%20Documents/SRS_Dashboard/Site%20Disaster%20History.xlsx?d=w794dafe4bfcf46e790510416d93d2328&csf=1&web=1&e=OpwoHt'
    sheet_name = 'Site Disaster History'
    table = 'bcp.SRS_Site_Disaster_History'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file
    df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=1, last_upload_time=last_refreshed)
    if len(df.index) == 0:
        # log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
    else:
        # Transform data
        df.drop(df.tail(1).index, inplace=True)  # drop last row
        # print(df.columns)

        # remove blank columns
        blank_columns = list()
        for col in df.columns:
            if df[col].isnull().all():
                blank_columns.append(col)
        # print(blank_columns)
        df.drop(blank_columns, axis=1, inplace=True)

        try:
            df['Disaster Percentile'] = df['Disaster Percentile'] / 100  # convert percentile number to decimal

            # Fix issue with multiple Supplier ESDIDs assigned to a site
            df['Site Reference'] = [x[:10] if isinstance(x, str) else x for x in df['Site Reference']]

            # Coalesce Subcontractor and Subtier Company into a single field
            df['SubContractor Name'] = df['Subcontractor'].combine_first(df['Subtier Company'])

            # Merge data from SRS API stored in HANA to get Site Identifier (Id)
            statement = """WITH SiteInfo AS (
                                SELECT
                                     SiteRelationships."GlobalSupplierIdentifier"
                                     ,Sites."SupplierSiteIdentifier"
                                     ,Sites."SupplierSubContractorName"
                                     ,Sites."SiteName"
                                     ,Sites."CityNm"
                                     ,Sites."CountrySubdivisionNm"
                                     ,Sites."CountryNm"
                                     ,Sites."PhysicalStreetAddressTxt"
                                     ,ROW_NUMBER() OVER(PARTITION BY Sites."SupplierSiteIdentifier" ORDER BY Sites."ChangeDtm" DESC) AS "RowNum"
                                FROM "_SYS_BIC"."d.SelfService.Supplier/SRSSupplierSites" Sites
                                LEFT OUTER JOIN (SELECT "GlobalSupplierIdentifier", "SupplierSiteIdentifier" FROM "_SYS_BIC"."d.SelfService.Supplier/SRSSupplierSites" WHERE "DeleteInd" = 'N') SiteRelationships
                                    ON Sites."SupplierSiteIdentifier" = SiteRelationships."SupplierSiteIdentifier"
                                WHERE Sites."DeleteInd" = 'N'
                            )
                            SELECT "GlobalSupplierIdentifier" AS "Site Reference"
                                 ,NULLIF("SupplierSubContractorName", '') AS "SubContractor Name"
                                 ,"SiteName" AS "Site Name"
                                 ,"CityNm" AS "City"
                                 ,"CountrySubdivisionNm" AS "State"
                                 ,"CountryNm" AS "Country"
                                 ,MIN("SupplierSiteIdentifier") AS "Site Id"
                            FROM SiteInfo
                            WHERE "RowNum" = 1
                            GROUP BY "GlobalSupplierIdentifier", "SupplierSubContractorName", "SiteName", "CityNm", "CountrySubdivisionNm", "CountryNm"
                            ORDER BY MIN("SupplierSiteIdentifier") ASC
                        """
            df1 = queryHANA(statement)
            if len(df1.index) == 0:
                log(False, project_name=project_name, data_area=data_area, error_msg='Unable to retrieve SiteId information from SRS table in HANA.')
            else:
                df_final = df.merge(df1, how='left', on=['Site Reference', 'SubContractor Name', 'Site Name', 'City', 'State', 'Country'])
                df_final.drop(['SubContractor Name'], axis=1, inplace=True)
                # print(df_final.columns)

                df_final['LoadDtm'] = datetime.today()

                # Load data to Database
                insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df_final, categorical=['Site Reference'], truncate=True)
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df_final.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df_final.shape[0], table))

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column missing/changed in Excel file.')
    ### END Site Disaster History Load ###

    ### BEGIN Supplier Mapping Load ###
    data_area = 'Supplier Mapping'
    # file_path = 'https://intel.sharepoint.com/:x:/r/sites/gscbusinesscontinuity-SRS_Dashboard/Shared%20Documents/SRS_Dashboard/SRS%20Supplier%20Mapping%20for%20BI%20Rev3.xlsx?d=w9ae01c97d1f34b3dae3d38b4a5957967&csf=1&web=1&e=mTcO3U'  # Old file
    file_path = 'https://intel.sharepoint.com/:x:/r/sites/gscbusinesscontinuity-SRS_Dashboard/Shared%20Documents/SRS_Dashboard/SRS%20Dashboard%20Supplier%20Mapping%20Table%2008262022.xlsm?d=w00c2cded6e4f44789897c4ecc0644a4a&csf=1&web=1&e=DdWBy4'
    sheet_name = 'Supplier Mapping'
    table = 'bcp.SRS_Supplier_Mapping'

    # Determine last upload date
    last_refreshed = getLastRefresh(project_name=project_name, data_area=data_area)

    # Extract data from Excel file
    df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
    if len(df.index) == 0:
        # log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
        print('"{}" Excel file has not been updated since last run. Skipping.'.format(data_area))
    else:
        # Transform data
        # df['Upload_Date'] = datetime.today()
        # # print(df.columns)

        # # Change Yes/No column to True/False
        # df['Count in Compliance Indicators'] = df['Count in Compliance Indicators'].apply(lambda x: True if isinstance(x, str) and x.lower() == 'yes' else False)
        #
        # # Fix issue with multiple Supplier ESDIDs assigned to a site
        # df['Supplier (Reference) ESDID in SRS'] = [x[:10] if isinstance(x, str) else x for x in df['Supplier (Reference) ESDID in SRS']]
        #
        # # # The following code fixes an issue uploading empty dates when Date formatting is applied in Excel
        # # df['Ratifaction Date'].map(lambda x: datetime.strftime(datetime.strptime(x, '%Y/%m/%d'), '%Y-%m-%d') if type(x) is str else None)  # format Date column
        # # df = df.astype({'Ratifaction Date': str})  # change type of date column to text

        df.drop(df.columns.difference(['Physical Address ESDID', 'busns_org_nm', 'ctry_nm', 'Country Risk',
                                       'addr_usg_type_cmv_id', 'Target for SRS?']
                                      ), axis=1, inplace=True, errors='raise')  # remove other columns

        df['addr_usg_type_cmv_id'] = df['addr_usg_type_cmv_id'].str.title()  # Convert Location Usage Type column to "Title Case" where every word is capitalized

        df.drop_duplicates(subset=['Physical Address ESDID', 'ctry_nm', 'addr_usg_type_cmv_id'], keep='first', inplace=True)  # Remove duplicates caused by different phys_addr_ids

        df['LoadDtm'] = datetime.today()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        # Load data to Database
        # insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['Global Parent Supplier ID', 'Supplier (Reference) ESDID in SRS'], truncate=True)  # for old file
        insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, categorical=['Physical Address ESDID'], truncate=True)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ### END Supplier Mapping Load ###

    print("--- %s seconds ---" % (time() - start_time))
