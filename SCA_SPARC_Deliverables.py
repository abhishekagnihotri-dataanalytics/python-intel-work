__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script supports the SPARC program for the Supply Chain Responsibility (formerly Assurance) - Supplier Report Card (SRC) scoring
                     by loading data from Excel files on SharePoint Online to the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"""
__schedule__ = "Daily at 12:50 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from time import time
from datetime import datetime
import numpy as np
import pandas as pd
from xlrd import XLRDError
from office365.runtime.client_request_exception import ClientRequestException
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, getLastRefresh, queryHANA, map_columns
from Logging import log


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()

    # initialize variables
    params['EMAIL_ERROR_RECEIVER'].append('jarom.walker@intel.com')  # add Jarom to list of email error receivers
    project_name = 'SPARC Deliverables'

    files = {
        'Escalation': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Operations%20-%20Supplier%20Escalations/SPARC_Escalation.xlsx?d=w7906a341e68b463e9091e76eaeb8763f&csf=1&web=1&e=xoARje",
        'Forced Labor': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Assessment/SPARC_ForcedBondedLabor.xlsx?d=w705912ab54ad4af28838c6418b036942&csf=1&web=1&e=5DZFNK",
        'Intel Minerals': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Responsible%20Minerals/Power%20BI%20Supporting%20Documents/SPARC_AdditionalMinerals.xlsm?d=w3a92b977f7194b138bbc350d34d1a1e9&csf=1&web=1&e=zh5mOm",
        '3TG': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Responsible%20Minerals/Power%20BI%20Supporting%20Documents/2021%20Dashboard%20Update/SPARC_3TG.xlsm?d=wea656acbe36c4afd9a00481f766177d2&csf=1&web=1&e=fmBdtt",
        'Cobalt': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Responsible%20Minerals/Power%20BI%20Supporting%20Documents/2021%20Dashboard%20Update/SPARC_Cobalt.xlsm?d=w26bce3cf2b2b41a29480e0167d43ab9a&csf=1&web=1&e=DzPNGd",
        'Excursions': "https://intel.sharepoint.com/:x:/r/sites/scasystems-businessprocessarchitecture/Shared%20Documents/General/Projects/SPARC%20SRC%20Scoring/Excursions%20tracker.xlsm?d=wa1a8fbf0ec2e47ba9585346ff348b690&csf=1&web=1&e=yqwmoG",
        'SRS': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/SCA%20Governance/SPARC%20Deliverable%20Trackers/SPARC_SRS.xlsm?d=w16e30ee476954a31922cbce9cf153401&csf=1&web=1&e=E9xgW8",

        # The following areas were consolidated into "Contract Compliance MFC" Program Area during Salesforce conversion
        # 'Audit': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Assessment/SPARC_Audit.xlsm?d=we3d575160d6d46a78ec3222a0a397a88&csf=1&web=1&e=Z62bej",
        # 'Self Assessment': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Assessment/SPARC_SelfAssessment.xlsm?d=w04515f50815f4362b8a98718f1f3d0e0&csf=1&web=1&e=PY9PJ5",
        # 'MFC': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Contract%20Compliance%20Audit%20Program/SRC/SPARC_ContractComplianceMFC.xlsx?d=w4b846a2f604a42838e4e0164d9cd713b&csf=1&web=1&e=SjVY8m",
        # 'Climate Change': "https://intel.sharepoint.com/:x:/r/sites/gscsupplierresponsibilityforcommoditymanagers/Shared%20Documents/Environmental%20Footprint/SPARC_ClimateChange.xlsm?d=w1b18bebf83784a5dbc855ec488d7d15c&csf=1&web=1&e=8mmDDp",
        # 'Water Use': "https://intel.sharepoint.com/:x:/r/sites/gscsupplierresponsibilityforcommoditymanagers/Shared%20Documents/Environmental%20Footprint/SPARC_WaterUse.xlsm?d=w9e6ebcaf449a429ca881d9d4e32d7045&csf=1&web=1&e=BBcEHl",
        # 'CDP Approval': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Operations%20-%20SPARC%20Program/Environmental%20Performance%20Program/SPARC_CDPApproval.xlsx?d=wa5c1be1803e64361a1552247b2e4944a&csf=1&web=1&e=kWahUJ",
        # 'Ultra Low Risk': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Responsibility%20-%20Assessment/SPARC_Risk_Training_and_Attestation.xlsm?d=w3dfc54b4d3764785a4cd7bd701dbd5c2&csf=1&web=1&e=LFjTAH",
        # 'OnSite Safety': "https://intel.sharepoint.com/:x:/r/sites/msosconsitesuppliersafetyprogram/Shared%20Documents/General/SPARC%20Program_Onsite%20Supplier%20Safety/SPARC_OnSiteSupplierSafety.xlsx?d=wfb1164cf68b34804bf7df148972db01b&csf=1&web=1&e=HvR8hN",
        # 'Green Chemistry': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Chemical%20Regulatory/SPARC%20Deliverable%20Trackers/SPARC_GreenChemistry.xlsx?d=w04ed8555b5eb4c07ab5f0df0f565e6c9&csf=1&web=1&e=ZNYtm4",
        # 'PCDC': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Chemical%20Regulatory/SPARC%20Deliverable%20Trackers/SPARC_PCDC.xlsx?d=w71a5422ac8d04f32a2396763ef30d6b0&csf=1&web=1&e=M7DI6q",
        # 'PFAS': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Chemical%20Regulatory/SPARC%20Deliverable%20Trackers/SPARC_PFAS.xlsx?d=w429ee4950f6b4a7881e1243c2b596bcd&csf=1&web=1&e=TAOpRt",
        # 'Substance Articles': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Chemical%20Regulatory/SPARC%20Deliverable%20Trackers/SPARC_SubstanceArticles.xlsx?d=w58cc8733bcb349cca569983c96907a11&csf=1&web=1&e=Fq4zJq",
        # 'Responsible Leaders': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/SPARC%20Misc/Responsible%20Leaders%20Program/SPARC_ResponsibleLeaders.xlsx?d=w624d6321d093466a98c6afa384b7470f&csf=1&web=1&e=8WI662",
        # 'EU Data Transfer': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Supply%20Chain%20Security%20and%20Privacy/SPARC%20Deliverable%20Trackers/SPARC_EUDataTransfer.xlsm?d=w59a430ffefb64b8d8f05712581281aad&csf=1&web=1&e=Y7wSvY",
        # 'Section 889': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Supply%20Chain%20Security%20and%20Privacy/SPARC%20Deliverable%20Trackers/SPARC_Section889.xlsm?d=w25ba2e209048429ea44b517093c0d0a3&csf=1&web=1&e=1gVPPP",
        # 'Sec Risk Assessment': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/Supply%20Chain%20Security%20and%20Privacy/SPARC%20Deliverable%20Trackers/SPARC_SecurityFindings.xlsm?d=w7c346644aa504acdb9f078a12f682d75&csf=1&web=1&e=wXDiX6",
        # 'QMS': "https://intel.sharepoint.com/:x:/r/sites/SupplyChainAssurance/Shared%20Documents/SCA%20Governance/SPARC%20Deliverable%20Trackers/SPARC_QMS.xlsm?d=w0cc934a8a4854bdabaf219799f51463a&csf=1&web=1&e=NubTKM",
    }

    for area in files.keys():
        # determine the last refresh datetime for each file
        last_refreshed = getLastRefresh(project_name=project_name, data_area=area)  # set this to None if you'd like to force a reload of the data regardless of last modified time on the file

        # read each Excel file from SharePoint
        try:
            df = loadExcelFile(file_path=files[area], sheet_name='BI Capable {}'.format(area), header_row=0, last_upload_time=last_refreshed)
        except XLRDError as error:
            print(error)
            print('Unable to load {0}.'.format(area))
            df = pd.DataFrame()
        except ClientRequestException:
            log(False, project_name=project_name, data_area=area, error_msg='Unable to read Excel file from SharePoint Online site. Name of the file has changed.')
            print('Skipped {0} due to error.'.format(area))
            continue

        try:
            if len(df.index) == 0:  # DataFrame is empty
                print('Skipped {0} as it has not been modified since the last upload.'.format(area))
                continue
        except AttributeError:  # df is NoneType
            log(False, project_name=project_name, data_area=area, error_msg='Unable to read Excel file from SharePoint Online site')
            print('Skipped {0} due to error.'.format(area))
            continue

        try:
            # force supplier id columns to length 10 if numeric (supplier id) or include the word tier as in "Tier 2"
            if area == 'SRS':
                df.rename(columns={'Site ID (Not Required)': 'Site ID'}, inplace=True)

            supplier_id_cols = ['Global ID', 'Site ID']
            for col in supplier_id_cols:
                df[col] = df[col].apply(lambda x: x if isinstance(x, int) else round(x) if isinstance(x, float) and not np.isnan(x) else x[:10] if isinstance(x, str) and x.isnumeric() else x if isinstance(x, str) and 'tier' in x.lower() else None)

            # format specific areas to match respective sql tables
            if area in ['Escalation', 'Cobalt', '3TG']:  # these areas have one blank column at the end of their template
                df.drop(['Deliverable Detail 3'], axis=1, inplace=True)
            elif area == 'Forced Labor':  # these areas have two blank columns at the end of their template
                df.drop(['Deliverable Detail 2', 'Deliverable Detail 3'], axis=1, inplace=True)
            # else:  # these areas have three blank columns at the end of their template
            #     df.drop(['Deliverable Detail 1', 'Deliverable Detail 2', 'Deliverable Detail 3'], axis=1, inplace=True)

            if area == 'Excursions':
                df['Meets Intel Standards'] = df['Meets Intel Standards'].apply(lambda x: True if isinstance(x, str) and x == 'Meets Intel standards' else False)
            else:
                # format date columns
                date_cols = ['Deliverable Assigned Date', 'Deliverable Due Date', 'Deliverable Received Date']
                last_column = ''
                try:
                    for col in date_cols:
                        last_column = col
                        df[col] = df[col].apply(lambda x: datetime.strptime(x, '%m/%d/%Y') if isinstance(x, str) and not x.isspace() else x if isinstance(x, datetime) else None)
                except ValueError:
                    log(False, project_name=project_name, data_area=area, error_msg="Bad date value in the {0} column of the {1} file.".format(last_column, area))
                    continue

                df['Deliverable Status'].fillna('NA', inplace=True)  # for Deliverable Status column, replace NULL values with string "NA"

        except KeyError as error_msg:
            error_msg = '{0}. Template changed for {1}.'.format(error_msg, area)
            log(False, project_name=project_name, data_area=area, error_msg=error_msg)
            print(error_msg)
            continue

        # remove any extra columns from template (if they exist)
        del_cols = []
        for col in df.columns:  # iterate over every column in dataframe
           if 'Unnamed' in col:  # example column name: "Unnamed: 13"
               del_cols.append(col)
        if del_cols:
            df.drop(del_cols, axis=1, inplace=True)

        # logic to calculate sql table name based on area (area is provided in Python dict)
        if area == 'SRS':
            table = 'scs.SPARC_BusinessContinuity'
        elif area in ['3TG', 'Cobalt']:
            table = 'scs.SPARC_{}Minerals'.format(area)
        else:
            table = 'scs.SPARC_{}'.format(area.replace(' ', ''))
        # print(table)

        # add database standards columns to end of DataFrame
        df['LoadDtm'] = datetime.now()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, categorical=supplier_id_cols, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
        else:
            print(error_msg)

    ### BEGIN HANA Supplier Deliverable Management (SDM) Load ###
    area = 'SDM'
    table = 'scs.SPARC_SupplierDeliverableManagement'

    query = """SELECT "GlobalUltimateSupplierId"
                      ,"GlobalSupplierNm"
                      ,"SupplierId"
                      ,"SupplierNm"
                      ,"SupplierPerformanceDeliverableProgramNm"
                      ,"SupplierPerformanceDeliverableTypeNm"
                      ,"SupplierDeliverableRequestAssignmentDt"
                      ,"SupplierDeliverableRequestDueDt"
                      --,"SupplierDeliverableRequestStartDt"
                      ,"SupplierDeliverableRequestResponseReceivedDt"
                      ,"SupplierPerformanceDeliverableStandardizedStatusNm"
                      --,"ScoredInSupplierReportCardInd"
                      --,"DeliverableDoesNotMeetIntelStandardsCd"
                      --,"DeliverableGapClosingActionCd"
                      --,"DeliverableHighRiskIssueCd"
                      ,"DeliverableTrackingIntelCommentTxt"
                      --,"UniqueDeliverableId"
                      ,"CampusNm"
                      ,"CampusCd"
                      ,"CampusCityNm"
                      ,"CampusCountrySubdivisionNm"
                      ,"CampusCountryNm"
                      ,"AuditTypeNm"
                      ,"SubContractorNm"
                      ,"OffshoreDevelopmentCenterId" 
                FROM "_SYS_BIC"."d.SelfService.Supplier/SupplierPerformanceDeliverableManagement"(
                  'PLACEHOLDER' = ('$$IP_SRCEligibleIndicator$$', '''*'''), -- Include both 'Y' and 'N' values 
                  'PLACEHOLDER' = ('$$IP_DoesntMeetIntelStandardsCd$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_DeliverableStartStartDt$$', '2020-01-01'), 
                  'PLACEHOLDER' = ('$$IP_DeliverableStartEndDt$$', '2023-12-31'),
                  'PLACEHOLDER' = ('$$IP_DeliverableDueStartDt$$', '2020-01-01'),
                  'PLACEHOLDER' = ('$$IP_DeliverableDueEndDt$$', '2023-12-31'), 
                  'PLACEHOLDER' = ('$$IP_DeliverableNotificationStartDt$$', '2020-01-01'),
                  'PLACEHOLDER' = ('$$IP_DeliverableNotificationEndDt$$', '2022-12-31'), 
                  'PLACEHOLDER' = ('$$IP_GlobalSupplierNm$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_DeliverableStatusCd$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_GapClosingActionsCd$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_SiteNm$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_GlobalESDID$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_SiteESDID$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_DeliverableNm$$', '''*'''), 
                  'PLACEHOLDER' = ('$$IP_HighRiskIssuesCd$$', '''*'''),
                  'PLACEHOLDER' = ('$$IP_ProgramNm$$', '''*''')
                )"""
    df = queryHANA(query, environment='Production', single_sign_on=False)

    # convert date text columns into datetime
    try:
        for col in ['SupplierDeliverableRequestAssignmentDt', 'SupplierDeliverableRequestDueDt', 'SupplierDeliverableRequestResponseReceivedDt']:  # date columns
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='raise')
    except ValueError as error:  # date is not formatted correctly
        log(False, project_name=project_name, data_area=area, error_msg=error)
        raise error

    df['SupplierDeliverableRequestResponseReceivedDt'].mask(df['SupplierPerformanceDeliverableStandardizedStatusNm'].isin(['Not Applicable', 'Incomplete', 'Incomplete (Overdue)']), None, inplace=True)  # Remove received date if deliverable is "Incomplete"

    # revert statuses to match their original names/capitalization in SDM
    statuses = {'Not Applicable': 'NA', 'Incomplete (Overdue)': 'Incomplete (overdue)', 'Complete (with Issues)': 'Complete (with issues)'}
    df['SupplierPerformanceDeliverableStandardizedStatusNm'].replace(statuses, inplace=True)  # replace "NA" text with fully spelled out "Not Applicable" status

    # add database standards columns to end of DataFrame
    df['LoadDtm'] = datetime.now()
    df['LoadBy'] = 'AMR\\SYS_SCDATA' # 'AMR\\' + os.getlogin().upper()

    # # Debugging - uncomment the following line of code if you face an upload error with column number mismatch
    # map_columns(table, df)

    insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, categorical=['GlobalUltimateSupplierId', 'SupplierId'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
    log(insert_succeeded, project_name=project_name, data_area=area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
    ### END HANA SDM Load ###

    ### BEGIN Org Mapping Load ###
    area = 'SCS Supplier Updates'
    table = 'scs.SPARC_Supplier_Org_Mapping'
    file_path = "https://intel.sharepoint.com/:x:/r/sites/scasystems-businessprocessarchitecture/Shared%20Documents/General/Org%20Mapping/SCA_Manual_OrgMapping.xlsx?d=w28d9275b30ef4544973b8ab86907ed49&csf=1&web=1&e=2A6Y1s"
    sheet_name = 'Org_Mapping'

    last_refreshed = getLastRefresh(project_name=project_name, data_area=area)
    df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=1, last_upload_time=last_refreshed)  # header is second row in the file
    try:
        if len(df.index) == 0:  # DataFrame is empty
            print('Skipped {0} as it has not been modified since the last upload.'.format(area))
        else:
            df.drop(df.columns.difference(['Global Parent ID', 'Major Org', 'Sub Org', 'Business Risk Manager',
                                           'Sourcing Org Risk Lead', 'Segmented']),axis=1, inplace=True)  # remove other columns

            df['Segmented'] = df['Segmented'].apply(lambda x: True if type(x) == str and x.lower() == 'y' else False)  # Change Yes/No column to True/False

            df['LoadDtm'] = datetime.now()
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

            insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, categorical=['Global Parent ID'], truncate=True,driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
            else:
                print(error_msg)
    except AttributeError:  # df is NoneType
        log(False, project_name=project_name, data_area=area, error_msg='Unable to read Excel file from SharePoint Online site')
        print('Error loading {}.'.format(area))
    ### END Org Mapping Load ###

    ### BEGIN Program and Deliverable Exclusions Load ###
    area = 'Deliverable Exclusions'
    table = 'scs.SPARC_Deliverable_Exclusions'
    file_path = "https://intel.sharepoint.com/:x:/r/sites/scasystems-businessprocessarchitecture/Shared%20Documents/General/Projects/SPARC%20SRC%20Scoring/List%20of%20SPARC%20Deliverables.xlsx?d=web509b97e08e43cab31e4c887b74c51e&csf=1&web=1&e=TCeIfq"
    sheet_name = 'Sheet1'

    last_refreshed = getLastRefresh(project_name=project_name, data_area=area)
    df = loadExcelFile(file_path=file_path, sheet_name=sheet_name, header_row=0, last_upload_time=last_refreshed)
    if len(df.index) == 0:  # DataFrame is empty
        print('Skipped {0} as it has not been modified since the last upload.'.format(area))
    else:
        try:
            df['Include in Score? (Yes/No)'] = df['Include in Score? (Yes/No)'].apply(lambda x: True if isinstance(x, str) and x.lower() == 'yes' else False)  # convert yes/no column to True/False
        except KeyError as error_msg:
            log(False, project_name=project_name, data_area=area, error_msg='{0}. Template changed for {1}.'.format(error_msg, area))
            exit()

        df['LoadDtm'] = datetime.now()
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        insert_succeeded, error_msg = uploadDFtoSQL(table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
        log(insert_succeeded, project_name=project_name, data_area=area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
        else:
            print(error_msg)
    ### END Program and Deliverable Exclusions Load ###

    print("--- %s seconds ---" % (time() - start_time))
