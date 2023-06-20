from os import path
import argparse


shared_folder_path = r"\\vmsoapgsmssbi06.amr.corp.intel.com\gsmssbi"

params = {
            ### Driver Info
            'SQL_DRIVER': "{ODBC Driver 17 for SQL Server}",  # Other options "{SQL Server}" or "{SQL Server Native Client 11.0}"

            ### Password Secret
            'PASS_KEY': r'\\VMSOAPGSMSSBI06.amr.corp.intel.com\gscBoveda\OAAencryption.key',

            ### Email configuration
            'EMAIL_ERROR_RECEIVER': ['matthew1.davis@intel.com', 'grp_gsm_ssbi_support@intel.com'],
            # To get email alerts on failures, add either of the following lines to the beginning of your script:
            # params['EMAIL_ERROR_RECEIVER'].append('chelsea.gorius@intel.com')  # add a single user
            # params['EMAIL_ERROR_RECEIVER'].extend(['megan.whissen@intel.com', 'jarom.walker@intel.com'])  # add multiple users at once
            'EMAIL_SERVER': "outlook.intel.com",
            'EMAIL_SERVER_PORT': 993,

            ### SQL tables
            # MSS Market Intelligence
            'Table_MI_Pricing': "stage.stg_MSS_MI_Memory_Pricing",
            'Table_MI_Revenue': "stage.stg_MSS_MI_Revenue",
            'Table_MI_CAPEX': "stage.stg_MSS_MI_CAPEX",
            'Table_MI_OpMargin': "stage.stg_MSS_MI_OpMargin",
            'Table_MI_ProcessMix': "stage.stg_MSS_MI_ProcessMix",
            'Table_MI_SegProd': "stage.stg_MSS_MI_SegProd",
            'Table_MI_Sufficiency': "stage.stg_MSS_MI_Sufficiency",
            'Table_MI_WaferStarts': "stage.stg_MSS_MI_WaferStarts",
            'Table_MI_WaferProdSupplier': "stage.stg_MSS_MI_WaferProdSupplier",
            'Table_MI_Demand': "stage.stg_MSS_MI_Demand",
            # Supplier Safety
            'Table_Construct_Secure': "wcs.Construct_Secure_Supplier_Status",
            'Table_EHS': "wcs.EHS_HLVEs",
            'Table_Escalations': "scs.Escalation_Tracker",
            # EWC Qualtrics Survey
            'Table_GSC_L3': "survey.GSC_L3",
            'Table_Qualtrics_L3': "survey.L3Assessment_Survey",
            # AM Quarterly Awards
            'Table_AM_Qtrly_Awards': "ats.AM_Quarterly_Awards",
            'Table_AM_Award_Recip': "ats.AM_Quarterly_Award_Recipients",
            # ATS Operations
            'Table_ATS_ERM': "ats.ERM_Schedule",
            'Table_ATS_Cost_Ops_Goals': "ats.CostOps_Goals",
            'Table_ATS_Cost_Ops': "ats.CostOps_Affordability_Tracker",
            'Table_ATS_RR_ATM': "ats.Ramp_Readiness_ATM_Details",
            'Table_ATS_RR_Sub': "ats.Ramp_Readiness_Substrates",
            'Table_ATS_RR_Summary': "ats.Ramp_Readiness_Summary",
            'Table_ATS_RC': "ats.Risk_and_Controls",
            'Table_ATS_Wellnomics': "ats.Wellnomics",
            'Table_ATS_Wellnomics_Dept': "ats.Wellnomics_by_Area",
            'Table_ATS_PP_Actuals': "ats.Piece_Parts_Spends",
            'Table_ATS_PP_Forecast': "ats.Piece_Parts_POR",
            'Table_ATS_LCG': "ats.LCG_Mix",
            'Table_ATS_UA': "ats.Upside_Availability",
            'Table_ATS_UA_Sub': "ats.Upside_Availability_Substrates",
            'Table_SRC_Deadlines': "src.Deadlines",
            'Table_ATS_Supl_Trans_Sub': "ats.Supplier_Transparency_Substrates",
            'Table_ATS_Excursions_Sub': "ats.Supplier_Internal_Excursions_Substrates",
            # ATS Supplier Ops
            'Table_AT_FHR_Matrix': "ats.Rapid_Ratings_AT_FHR_Matrix",
            'Table_AT_Ratio_Report': "ats.Rapid_Ratings_AT_Quick_Ratios_Report",
            # ATS NSI Checklist
            'Table_NSI_Checklist': "ats.NSI_Checklist",
            'Table_NSI_Tracker': "ats.NSI_Tracker",
            'Table_NSI_Suppliers': "ats.NSI_Suppliers",
            # SRS BCP
            'Table_SRS_Site_Impact': 'bcp.SRS_Live_Site_Impact',
            'Table_SRS_Live_Events': 'bcp.SRS_Live_Event_Incidents',
            # SCQI RoadMap
            'Table_RoadMap': 'scqi.RoadMap',
            # Other/Overlapping
            'Table_Log': "audit.processing_log",

            ### Stored Procedures
            'SP_MI_Pricing': "ssc.Load_MSS_MI_Memory_Pricing",
            
            ### Archive Folders
            'Archive_Folder_MarketIntelligence': path.join(shared_folder_path, "SSC\Memory\MarketIntelligence", "Archive"),
            'Archive_Folder_PRF': path.join(shared_folder_path, "SSC\Memory\Price_PRF", "Archive"),
            'Archive_Folder_Demand_PRF_RTF': path.join(shared_folder_path, "SSC\Memory\Demand_PRF_RTF", "Archive"),

            ### SharedDrive File Paths
            'FilePath_MarketIntelligence': path.join(shared_folder_path, "SSC\Memory\MarketIntelligence"),
         }


class ProductionConfig:
    """
    Production configurations
    """
    GSMDW_SERVER = "sql1717-fm1-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gsmdw"

    debug_msg = "Using the gsmdw database on the PROD server."


class DevelopmentConfig:
    """
    Test configurations
    """
    GSMDW_SERVER = "sql1944-fm1-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gsmdw_tst"

    debug_msg = "Using the gsmdw_tst database on the TEST server."


class StagingPreprodConfig:
    """
    Staging environment Preprod configurations
    """
    GSMDW_SERVER = "sql2943-fm1-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gscdw"

    debug_msg = "Using the gscdw database on the STAGING PREPROD server."


class StagingProductionConfig:
    """
    Staging environment Production configurations
    """
    GSMDW_SERVER = "sql3266-fm1-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gscdw"

    debug_msg = "Using the gscdw database on the STAGING PRODUCTION server."


class HTZStagingProductionConfig:
    """
    Staging environment Production configurations
    """
    GSMDW_SERVER = "sql2652-fm1s-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gscdw"

    debug_msg = "[Intel Top Secret] Using the gscdw database on the HTZ STAGING PRODUCTION server."


class HTZStagingPreprodConfig:
    """
    Staging environment Production configurations
    """
    GSMDW_SERVER = "sql2592-fm1s-in.amr.corp.intel.com,3181"
    GSMDW_DB = "gscdw"

    debug_msg = "[Intel Top Secret] Using the gscdw database on the HTZ STAGING PREPROD server."


parser = argparse.ArgumentParser()
parser.add_argument('-prod', '--production', action="store_true", default=False, help="Switch to load to the sql1717-fm1-in.amr.corp.intel.com server instead of the default sql1944-fm1-in.amr.corp.intel.com server")
parser.add_argument('--scbi-preprod', action="store_true", default=False, help="Switch to load to the sql2943-fm1-in.amr.corp.intel.com server instead of the default sql1944-fm1-in.amr.corp.intel.com server")
parser.add_argument('--scbi-prod', action="store_true", default=False, help="Switch to load to the sql3266-fm1-in.amr.corp.intel.com server instead of the default sql1944-fm1-in.amr.corp.intel.com server")
parser.add_argument('--scbi-htz-preprod', action="store_true", default=False, help="Switch to load to the sql2592-fm1s-in.amr.corp.intel.com server instead of the default sql1944-fm1-in.amr.corp.intel.com server")
parser.add_argument('--scbi-htz-prod', action="store_true", default=False, help="Switch to load to the sql2652-fm1s-in.amr.corp.intel.com server instead of the default sql1944-fm1-in.amr.corp.intel.com server")
args, unknown = parser.parse_known_args()  # this allows argparse to handle unrecognized argument errors

if args.production:
    config = ProductionConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = True  # this is used to control when error email are sent
elif args.scbi_prod:  # Note: dash characters in ArgumentParser definition are converted to underscores in variable names
    config = StagingProductionConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = True
elif args.scbi_preprod:
    config = StagingPreprodConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = False
elif args.scbi_htz_prod:
    config = HTZStagingProductionConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = True
elif args.scbi_htz_preprod:
    config = HTZStagingPreprodConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = False
else:  # if no arguments are entered by the user
    config = DevelopmentConfig()
    params['EMAIL_ERROR_NOTIFICATIONS'] = False
params['GSMDW_SERVER'] = config.GSMDW_SERVER
params['GSMDW_DB'] = config.GSMDW_DB

print(config.debug_msg)
