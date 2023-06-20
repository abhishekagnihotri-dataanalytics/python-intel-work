__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_SES_Data tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Six times daily at 12:00 AM, 4:00 AM, 8:00 AM, 12:00 PM, 4:00 PM, 8:00 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from datetime import datetime
import pandas as pd
from numpy import nan
from string import ascii_letters
import smartsheet
from Helper_Functions import getLastRefresh, readSmartsheet, uploadDFtoSQL, queryAPIPortal, map_columns
from Logging import log, log_warning
from Password import accounts


# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


def get_sheet_links(view_type: str = 'gantt'):
    result = dict()
    smartsheet_client = smartsheet.Smartsheet(access_token=accounts['Smartsheet'].password)
    smartsheet_client.errors_as_exceptions(True)
    index_result = smartsheet_client.Sheets.list_sheets(include_all=True)
    for sheet in index_result.data:  # type: smartsheet.Smartsheet.models.sheet.Sheet
        result[sheet.name] = "{0}?view={1}".format(sheet.permalink, view_type)

    return result


if __name__ == "__main__":
    ### BEGIN Implementation Tracker Section ###
    # initialize variables
    project_name = 'Substrate Equipment'
    data_area = 'Implementation Tracker'
    report_name = 'Full Implementation Tracker Export'
    file_path = 'https://app.smartsheet.com/reports/xpWvPm6JhGxr59JfcvCVfvFq9WhRF88mmHgXGjc1?view=grid'
    table = 'ats.Substrate_Equipment_Implementation_Tracker'

    # Load Worker data from API Portal
    df_workers = queryAPIPortal(url="https://apis-internal.intel.com/worker/v6/worker-snapshot-details?$filter=\"EmployeeStatusCd\"<>'T'&$select=CorporateEmailTxt,FullNm&$format=JSON")
    df_workers.dropna(inplace=True)  #  Remove workers that do not have a valid email

    # Load data from Smartsheet
    df = pd.DataFrame()
    try:
        df = readSmartsheet(report_name, doc_type="Summary Report", page_size=100, last_upload_time=None)
    except FileNotFoundError as error:
        error_msg = '{0}. Perhaps the Report has been renamed on Smartsheet? The previous Report link was: {1}'.format(error.args[0], file_path)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)

    if len(df.index) == 0:  # case when Summary Report hasn't been modified since the last upload
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
    else:  # DataFrame is not empty
        df.drop(['CND', 'Committed to Current SDD Float', 'Created', 'Created By', 'Eq Lead Time', 'Intel Qual Finish',
                 'Intel Qual Start', 'Outdated', 'MRCL Finish', 'MRCL Start', 'Prev SDD', 'RDD to Committed SDD Float',
                 'RDD to Current SDD gap', 'RTD', 'SIFIS Gap Closure', 'SIRFIS Gap Closure', 'SIRFIS GR Date', 'SIRFIS State',
                 'SIRFIS Supplier Comment', 'TI Sched Team', 'Transit Mode', 'Transit Orgin', 'Transit Origin', 'Transit Time',
                 'Update Due'
                 ], axis=1, inplace=True, errors='ignore')  # remove unnecessary columns

        df.replace({"#NO MATCH": None}, inplace=True)  # Remove all instances of "#NO MATCH"

        last_column = 'Unknown?'  # for debugging purposes
        try:
            # Convert dates to proper format for database upload
            date_columns = ['Acceptance', 'Acceptance Criteria Documented', 'Audit Performance', 'Audit Prep Work', 'Auto Initial Test Plan',
                            'Auto Test Results', 'Automation', 'Committed SDD', 'Components Tracker Submission', 'Components in-house',
                            'Controller Auto Finish', 'Controller Auto Start', 'Current SDD', 'Development', 'Draft S/G', 'EFEM Integration',
                            'EMP WG Approval', 'Equipment Layout', 'Escalation tree & phone chart', 'Fac Pre-design Reqs Complete',
                            'Final Design Review', 'Final S/G', 'Final UDS', 'FSE Intel Site training', 'FSE L3 Cert', 'FSE Names & Frames',
                            'FSE Prep', 'FSE Readiness', 'Functional Test', 'Id prod specific tooling', 'Install documentation',
                            'Install jigs fixtures pre-fac kits on site', 'Install jigs fixtures pre-fac kits PO',
                            'Install kit on site', 'Install Micro Schedule', 'Intel Training Plan', 'IQ doc reqs defined',
                            'IQ names travel & cert plans', 'ISMI', 'Last Update', 'Micro Schedule', 'Op & Maint Manuals Rev0',
                            'Pre-Install Reqs', 'RDD', 'RSL', 'S2/S8 & NFPA 79 at Supplier', 'S2/S8 & NFPA 79 Submission to Intel',
                            'Sel Start', 'Selection Completed', 'SI', 'Simulator', 'Spares in stock', 'Spares PO', 'Spares Readiness',
                            'Spares Stocking Strategy', 'Supplier Facets Onboarding', 'Supplier FSE H/C Plan', 'Supplier Kit ready',
                            'Supplier Qual Finish', 'Supplier Qual Start', 'Tool Design', 'Tool Install', 'Training Cert',
                            'Training Syllabi', 'STD']
            # print('There are {} date columns...'.format(len(date_columns)))
            for col in date_columns:
                last_column = col
                # df[col] = df[col].apply(lambda x: x if isinstance(x, datetime) else datetime.strptime(x, '%Y-%m-%d') if isinstance(x, str) else None)
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce').dt.date

            last_column = 'Modified'
            df['Modified'] = pd.to_datetime(df['Modified'], format='%Y-%m-%dT%H:%M:%SZ')

            last_column = 'PO #'
            df['PO #'] = df['PO #'].apply(lambda x: str(x)[:10] if isinstance(x, float) else x)  # remove decimal point formatting on PO #

            # Lookup Smartsheet Sheet links for each Sheet Name using the API
            last_column = 'Sheet Name'
            hyperlinks = get_sheet_links(view_type='gantt')
            df['Sheet Link'] = [hyperlinks[sheet_name] for sheet_name in df['Sheet Name']]  # iterate over the "Sheet Name" column

            # Lookup Employee Name by Email using Worker reference table
            df = df.copy().merge(df_workers, how='left', left_on='CM', right_on='CorporateEmailTxt')  # map Commodity Manager Email to Name
            df = df.copy().merge(df_workers, how='left', left_on='SCE', right_on='CorporateEmailTxt')  # map SCE Email to Name
            df.rename(columns={'FullNm_x': 'CM Name', 'FullNm_y': 'SCE Name'}, inplace=True)
            df.drop(['CorporateEmailTxt_x', 'CorporateEmailTxt_y'], axis=1, inplace=True)

            # Append columns for logging
            df['LoadDtm'] = datetime.today()
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
            df['LoadSource'] = file_path

            # Manually map column names from SQL to DataFrame
            sql_column_order = ["SheetName",  "PurchaseOrderNumber", "PurchaseOrderCreatedDate", "AcceptanceDate", "AcceptanceCriteriaDocumentedDate",
                                "AcceptanceCriteriaStatus", "AcceptanceStatus", "AcceptanceVariance", "AllComponentsInHouse",
                                "AllComponentsInHouseVariance", "AuditPerformanceDate", "AuditPerformanceCompletion", "AuditPerformanceComment",
                                "AuditPerformanceVariance", "AuditPrepComment", "AuditPrepVariance", "AuditPrepWorkDate",
                                "AuditPrepWorkCompletion", "AutoInitialTestStatus", "AutoInitialTestPlanDate", "AutoTestResultsDate",
                                "AutoTestResultsStatus", "AutomationDate", "AutomationCompletion", "AutomationVariance", "Baselined",
                                "CEID", "CommodityManagerEmail", "CommittedSDD", "ComponentsComment", "ComponentsTrackerSubmissionDate",
                                "ComponentsInHouseDate", "ComponentsTrackerStatus", "ControllerAutoFinishDate", "ControllerAutoStartDate",
                                "CurrentSupplierDockDate","DesignandDevPhase", "DevelopmentDate", "DevelopmentComment", "DevelopmentPAS",
                                "DevelopmentVariance", "DraftSGDate", "DraftSGStatus", "EFEMIntegrationDate", "EFEMIntegration Completion",
                                "EFEMIntegrationVariance", "Effort", "EffortStatus", "EMPWGApprovalDate", "EMPWGApprovalStatus",
                                "EntityCode", "EquipmentLayoutDate", "EquipmentLayoutStatus", "EscalationTreeandPhoneChartDate",
                                "EscalationTreeandPhoneChartStatus", "EquipmentSoftwareQualityOwner", "FacilitiesPreDesignReqsCompletion",
                                "FacilitiesPreDesignReqsComment", "FacilitiesPreDesignReqsCompleteDate", "FacilitiesPreDesignReqsVariance",
                                "FinalDesignReviewDate", "FinalDesignReviewStatus", "FinalSGDate", "FinalSGStatus", "FinalUDSDate",
                                "FinalUDSStatus", "FSEIntelSiteTrainingDate", "FSEIntelSiteTrainingStatus", "FSEL3CertDate",
                                "FSEL3CertStatus", "FSENamesandFramesDate", "FSENamesandFramesStatus", "FSEPrepDate", "FSEPrepCompletion",
                                "FSEPrepComment", "FSEPrepVariance", "FSEReadinessDate", "FSEReadinessCompletion", "FSEReadinessComment",
                                "FSEReadinessVariance", "FunctionalTestDate", "FunctionalTestStatus", "IdProdSpecificToolingDate",
                                "IdProdSpecificToolingStatus", "InstallDocumentationDate", "InstallDocumentationStatus",
                                "InstallJigsFixturesPreFacKitsonSiteDate", "InstallJigsFixturesPreFacKitsonSiteStatus", "InstallJigsFixturesPreFacKitsPODate",
                                "InstallJigsFixturesPreFacKitsPOStatus", "InstallkitonsiteDate", "InstallkitonsiteStatus",
                                "InstallMicroScheduleDate", "InstallMicroScheduleComment", "InstallMicroScheduleStatus", "InstallMicroScheduleVariance",
                                "IntakeStatus", "IntelTrainingPlanDate", "IQDocReqsDefinedDate", "IQDocReqsDefinedStatus",
                                "IQNamesTravelandCertPlansDate", "IQNamesTravelandCertPlansStatus", "IsLink", "ISMIDate", "ISMIStatus",
                                "LastUpdate", "LateTasksReport", "LinkName", "LLTComponentsOrdered", "LLTComponentsVariance", "MicroScheduleDate",
                                "MicroScheduleDaysVariance", "MicroScheduleReceivedStatus", "ModifiedDateTime",  "ModifiedBy", "ModifiedBy2",
                                "OpandMaintManualsRev0Date", "OpandMaintManualsRev0Status", "PreInstallReqsDate", "PreInstallReqsCompletion",
                                "PreInstallReqsComment", "PreInstallReqsVariance", "PreShipandInstallationPhase", "PriorNames",
                                "ProjectStatus", "ProjectStatuslink", "RequiredDockDate", "RSLDate", "RSLStatus", "S2S8andNFPA79atSupplierDate",
                                "S2S8andNFPA79atSupplierStatus", "S2S8andNFPA79SubmissiontoIntelDate", "S2S8andNFPA79SubmissiontoIntelStatus",
                                "SCE", "SCESelectionTimelineCompleted", "SelectionStartDate", "SelectionCompletedDate", "SESGroup",
                                "SESGroupMgr", "SIDate", "SICompletion", "SIComment", "SIVariance", "SimulatorDate", "SimulatorStatus",
                                "SparesComment", "SparesinStockDate", "SparesinStockStatus", "SparesPODate", "SparesPOStatus",
                                "SparesReadinessDate", "SparesReadinessCompletion", "SparesReadinessVariance", "SparesStockingStrategyDate",
                                "SparesStockingStrategyStatus", "SPTDFunctionalArea", "SPTDProgram", "SupplierName", "SupplierFacetsOnboardingDate",
                                "SupplierFacetsOnboardingStatus", "SupplierFSEHCPlanDate", "SupplierFSEHCPlanStatus", "SupplierKitReadyDate",
                                "SupplierKitReadyStatus", "SupplierQualFinishDate", "SupplierQualStartDate", "SWAutoRep", "ToolDescription",
                                "ToolDesignDate", "ToolDesignCompletion", "ToolDesignComments", "ToolDesignVariance", "ToolDockingComment",
                                "ToolInstallDate", "ToolInstallCompletion", "ToolInstallComment", "ToolInstallVariance", "TrainingCertDate",
                                "TrainingCertStatus", "TrainingPlanCompletion", "TrainingPlanVariance", "TrainingPrepComment",
                                "TrainingSyllabiDate", "TrainingSyllabiStatus", "SupplierTenderDate", "SheetLink", "CommodityManagerName",
                                "SCEName", "LoadDtm", "LoadBy", "LoadSource"]

            # # Uncomment the below line of code to debug truncation error in SQL insert
            # column_info = map_columns(table=table, df=df, sql_columns=sql_column_order, display_result=True)

            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, columns=sql_column_order, categorical=['PO #', 'Supplier'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError as error:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column "{0}" not found in Smartsheet Summary Report.'.format(last_column))
        except (ValueError, TypeError) as error:
            log(False, project_name=project_name, data_area=data_area, error_msg='Bad value present in column "{0}" of Smartsheet Summary Report'.format(last_column))
    ### END Implementation Tracker Section ###

    ### BEGIN Checkbook Section ###
    # initialize variables
    data_area = 'Checkbook'
    report_name = 'Checkbook Smartsheet export'
    file_path = 'https://app.smartsheet.com/reports/CJRj6JrFv5hVGxJR69C72VQQGhw8c957wmVCwgx1?view=grid'
    table = 'ats.Substrate_Equipment_Checkbook'

    # Extract data from Smartsheet
    df = pd.DataFrame()
    try:
        df = readSmartsheet(report_name, doc_type="Summary Report", page_size=100, last_upload_time=None)
    except FileNotFoundError as error:
        error_msg = '{0}. Perhaps the Report has been renamed on Smartsheet? The previous Report link was: {1}'.format(error.args[0], file_path)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)

    if len(df.index) == 0:  # case when Summary Report hasn't been modified since the last upload
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
    else:  # DataFrame is not empty
        # print(df.columns)

        # Transform data
        df.replace({"#NO MATCH": None}, inplace=True)  # Remove all instances of "#NO MATCH"

        last_column = 'Unknown?'  # for debugging purposes
        try:
            # Convert dates to proper format for database upload
            date_columns = ['CCB Date Reviewed', 'Date EFEM escalation was submitted', 'EFEM Date Needed by OEM', 'EFEM Expected Ship Date',
                            'EFEM PO Issue Date', 'LP Date Needed by OEM', 'LP Expected Ship Date', 'LP PO Issue Date',
                            'NEW EFEM Expected Ship Date', 'NEW LP Expected Ship Date', 'Selection Complete', 'Selection Start']
            for col in date_columns:
                last_column = col
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce').dt.date

            datetime_columns = ['Created', 'Modified']
            for col in datetime_columns:
                last_column = col
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ')

            # Format True/False columns for database upload
            tf_columns = ['EFEM Constraint', 'IsLink', 'LP Constraint', 'P-Spec incl Section 9 Auto Reqs', 'Quote and Tool PO incl SW Automation',
                          'Separate PO Expected for S/W Auto', 'ZBB']
            for col in tf_columns:
                last_column = col
                else_value = None  # Default blanks as SQL nulls
                if col in ['IsLink', 'ZBB']:
                    else_value = False  # Convert blanks to False
                df[col] = df[col].apply(lambda x: True if isinstance(x, str) and x.lower() == 'true' else
                                                  False if isinstance(x, str) and x.lower() == 'false' else
                                                  x if isinstance(x, bool) else
                                                  else_value)  # value if blank in Smartsheet

            # Convert from thousands into 0-base
            per_thousands_columns = ['Automation / PEER $K', 'CCB Approved Cost $K', 'Org Auto / PEER $K', 'Org Tool Cost incl. EFEM $K',
                                     'Prev Tool Cost', 'Previous CCB Approved Cost']
            for col in per_thousands_columns:
                last_column = col
                df[col] = df[col] * 1000

            last_column = 'CCB Status'
            df['CCB Status'] = df['CCB Status'].apply(lambda x: 'Not Reviewed' if (x is nan or x is None) else x)  # Force blank entries to be "Not Reviewed"

            # Lookup Employee Name by Email using Worker reference table
            last_column = 'CM Owner Email'
            df = df.copy().merge(df_workers, how='left', left_on='CM Owner', right_on='CorporateEmailTxt')  # map CM Owner Email to Name
            last_column = 'SCE Owner Email'
            df = df.copy().merge(df_workers, how='left', left_on='SCE Owner', right_on='CorporateEmailTxt')  # map SCE Owner Email to Name
            df.rename(columns={'FullNm_x': 'CM Name', 'FullNm_y': 'SCE Name'}, inplace=True)
            df.drop(['CorporateEmailTxt_x', 'CorporateEmailTxt_y'], axis=1, inplace=True)

            df['LoadDtm'] = datetime.today()
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
            df['LoadSource'] = file_path

            sql_column_order = ["CEID", "Adder", "AutomationOrPEERCost", "AutomationNotes", "CCBApprovedCost",
                                "CCBApprover", "CCBReviewDate", "CCBReviewComments", "CCBStatus", "CMOwnerEmail",
                                "Comment", "CostStatus", "CreatedDatetime",
                                "EFEMEscalationSubmittedDate", "EFEMComments", "EFEMConstraint", "EFEMOEMNeededDate",
                                "EFEMEscalationStatus", "EFEMExpectedShipOriginalDate", "EFEMIntegrator",
                                "EFEMPOIssueDate", "EFEMPriority", "EFEMQuantityOrdered", "EFEMSupplier", "Effort",
                                "EffortStatus", "EntityCode", "EPICInfo", "EquipmentLeadTime",
                                "EquipmentSoftwareQualityOwner", "FOUPBatch", "IntakeStatus", "IsLink", "ItemOrdered",
                                "LinkName", "LoadPortSupplier", "LoadPortConstraint", "LoadPortOEMNeededDate",
                                "LoadPortExpectedShipOriginalDate", "LoadPortPOIssueDate", "ModifiedDatetime",
                                "ModifiedBy", "ModuleOwner", "EFEMExpectedShipNewDate", "LoadPortExpectedShipNewDate",
                                "OEMPONumbertoEFEM", "OEMPONumbertoLoadPort", "OriginalAutomationOrPEERCost",
                                "OriginalToolCostIncludingEFEM", "PspecIncludesSection9AutomationRequirements",
                                "PanelFOUPKitModelNumber", "PaymentDeviation", "PathfindingProgramPriority",
                                "PreviousToolCost", "PreviousCCBApprovedCost", "PriorNames", "ProjectType",
                                "QuoteAndToolPOincludesSoftwareAutomation", "SCEOwnerEmail",
                                "SelectionCompleteDate", "SelectionStartDate", "SelectionType",
                                "SeparatePOExpectedforSoftwareAutomation", "SESGroup", "SESGroupManager", "SPNOrdered",
                                "SPTDFuntionalArea", "SupplierUses3rdPartyDevforSoftwareAutomation", "Suppliers",
                                "SoftwareAutomationRep", "ToolDescription", "ZBB", "CMOwnerName", "SCEOwnerName",
                                "LoadDtm", "LoadBy", "LoadSource"]

            # # Uncomment the below line of code to debug truncation error in SQL insert
            # column_info = map_columns(table=table, df=df, display_result=True, sql_columns=sql_column_order)

            # Load data to Database
            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}", columns=sql_column_order)
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError as error:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column "{0}" not found in Smartsheet Summary Report.'.format(last_column))
    ### END Checkbook Section ###

    ### BEGIN Selection Tracker Section ###
    # initialize variables
    data_area = 'Selection Tracker'
    report_name = 'Selection Trackers Export'
    file_path = 'https://app.smartsheet.com/reports/xP7xWM538HpCx5G643XFPQX8QCxRFj2j3QcGMMr1?view=grid'
    table = 'ats.Substrate_Equipment_Selection_Tracker'

    # Extract data from Smartsheet
    df = pd.DataFrame()
    try:
        df = readSmartsheet(report_name, doc_type="Summary Report", page_size=100, last_upload_time=None)
    except FileNotFoundError as error:
        error_msg = '{0}. Perhaps the Report has been renamed on Smartsheet? The previous Report link was: {1}'.format(error.args[0], file_path)
        log(False, project_name=project_name, data_area=data_area, error_msg=error_msg)

    if len(df.index) == 0:  # case when Summary Report hasn't been modified since the last upload
        log_warning(project_name=project_name, data_area=data_area, warning_type='Not Modified')
    else:  # DataFrame is not empty
        df.drop(['FoK'], axis=1, inplace=True, errors='ignore')  # remove unnecessary column

        # Transform data
        df.replace({"#NO MATCH": None, "#UNPARSEABLE": None}, inplace=True)  # Remove all instances of "#NO MATCH" AND "#UNPARSEABLE"

        last_column = 'Unknown?'  # for debugging purposes
        try:
            # Convert dates to proper format for database upload
            date_columns = ['Auto & ESQ', 'Baseline DD', 'Contract Signed Date', 'CPA', 'EMP SCS Risk Assessment Approval', 'Final UDS',
                            'Init Spec Parm Def Date', 'Initial EHS & Facilities RA Review of UDS', 'ISMI', 'P-Spec Closure', 'PO',
                            'Preliminary UDS', 'Pricing and Support Neg', 'Projected Dock Date', 'SCS Approval', 'Sel Start',
                            'Selection Completed', 'TIER Neg Approval', 'Last Update']
            for col in date_columns:
                last_column = col
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce').dt.date

            datetime_columns = ['Created', 'Modified']
            for col in datetime_columns:
                last_column = col
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ')

            # Format True/False columns for database upload
            tf_columns = ['Baselined', 'SCE Selection Timeline Completed', 'ZBB']
            for col in tf_columns:
                last_column = col
                df[col] = df[col].apply(lambda x: True if isinstance(x, str) and x.lower() == 'true' else
                                                  False if isinstance(x, str) and x.lower() == 'false' else
                                                  x if isinstance(x, bool) else
                                                  False)  # value if blank in Smartsheet

            variance_columns = ['Automation and ESQ variance', 'CPA Funding Variance', 'Pricing Neg Variance', 'SCS Approval Variance', 'Spec closure variance']
            for col in variance_columns:
                last_column = col
                df[col] = df[col].apply(lambda x: float(x.rstrip(ascii_letters)) if isinstance(x, str) else  # remove any trailing alphabetical characters from decimal
                                                  x if isinstance(x, float) or isinstance(x, int) else
                                                  None)

            last_column = 'Entity Code'
            df['Entity Code'] = df['Entity Code'].apply(lambda x: None if isinstance(x, str) and len(x) > 10 else x)

            # Lookup Smartsheet Sheet links for each Sheet Name using the API
            last_column = 'Sheet Name'
            hyperlinks = get_sheet_links(view_type='grid')
            df['Sheet Link'] = [hyperlinks[sheet_name] for sheet_name in df['Sheet Name']]  # iterate over the "Sheet Name" column

            df['LoadDtm'] = datetime.today()
            df['LoadBy'] = 'AMR\\' + os.getlogin().upper()
            df['LoadSource'] = file_path

            # # Uncomment the below line of code to debug truncation error in SQL insert
            # map_columns(table=table, df=df, display_result=True)

            # Load data to Database
            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))

        except KeyError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Column "{0}" not found in Smartsheet Summary Report.'.format(last_column))
        except ValueError:
            log(False, project_name=project_name, data_area=data_area, error_msg='Alphabetical character present in the "{0}" numeric column in Smartsheet Summary Report.'.format(last_column))
    ### END Selection Tracker Section ###
