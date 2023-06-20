__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script loads Intel Top Secret (ITS) data into the GSCDW database on the sql2592-fm1s-in.amr.corp.intel.com,3181 server"""

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from time import time
from Helper_Functions import uploadDFtoSQL, querySQL, getLastRefresh, map_columns
from Logging import log


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == '__main__':
    start_time = time()

    # initialize variables
    project_name = 'SCBI Core Data'
    data_area = 'FRISCO'
    table = 'Base.FRISCOInterfacePowerBIEquipCapacity'

    if os.environ['COMPUTERNAME'] == 'VMSGSCODASH100':  # Production VM
        server = 'vmtmefelcos100.amr.corp.intel.com,3180'
    elif os.environ['COMPUTERNAME'] == 'VMSGSCODASH200':  # Pre-production VM
        server = 'vmtmefelcos200.amr.corp.intel.com,3180'
    else:
        raise EnvironmentError("Unable to connect to FELCO database. Please confirm you are running this script on the Operational Analytics team's python virtual machines. "
                               "These are the only servers that have ports opened to the FELCO/FRISCO HTZ environment.")

    query = """SELECT 
                   [DataSetID]
                  ,[SnapshotDate]
                  ,[NeedId]
                  ,[PoId]
                  ,[Site]
                  ,[Entity]
                  ,[Ceid]
                  ,[Process]
                  ,[Manufacturing Type]
                  ,[PlanId]
                  ,[PlanName]
                  ,[PlanType]
                  ,[Ueid]
                  ,[Building]
                  ,[EventType]
                  ,[Source]
                  ,[Functional Area]
                  ,[Budget Area]
                  ,[Wafer Tie]
                  ,[InExecutionRange]
                  ,[Cnd]
                  ,[Std]
                  ,[Trdd]
                  ,[Atd]
                  ,[Prefac Start]
                  ,[Prefac Finish]
                  ,[Set Start]
                  ,[Set Finish]
                  ,[Convert Start]
                  ,[Convert Finish]
                  ,[SL1 Finish]
                  ,[SL2 Finish]
                  ,[P-MRCL]
                  ,[Mrcl]
                  ,[Late MRCL]
                  ,[Up To Prod]
                  ,[MRCL Float]
                  ,[Pid]
                  ,[Supplier Name]
                  ,[Supplier ID]
                  ,[Model]
                  ,[Description]
                  ,[Category]
                  ,[IsNeeded]
                  ,[PR Number]
                  ,[PO Number]
                  ,[PO Line]
                  ,[TrackingNumber]
                  ,[PoType]
                  ,[InitialRdd]
                  ,[Rdd]
                  ,[DdRdd]
                  ,[InitialSdd]
                  ,[Sdd]
                  ,[RequestedDate]
                  ,[Impact]
                  ,[EmsComment]
                  ,[SIRFIS ID]
                  ,[State]
                  ,[Ctd]
                  ,[Transit Origin]
                  ,[Transit Mode]
                  ,[Transit Time]
                  ,[Tags]
                  ,[SIRFIS Internal Comment]
                  ,[SIRFIS Supplier Comment]
                  ,[CS Acceleration Comment]
                  ,[CS Acceleration Comment Auto]
                  ,[Vendor Acceleration Comment]
                  ,[Delivery Comment]
                  ,[IsDesignConstraint]
                  ,[IsConstructionBbConstraint]
                  ,[IsSupplierConstraint]
                  ,[IsIntelConstraint]
                  ,[OtherConstraint]
                  ,[SupplierIqRiskLevel]
                  ,[SupplierIqRiskLastUpdateBy]
                  ,[SupplierIqRiskLastUpdateDate]
                  ,[AT Comment]
                  ,[AT Factory Comment]
                  ,[AT Funding Cycle]
                  ,[AtPriorityCategory]
                  ,[AtFactoryRisk]
                  ,[IsAtCriticalPathTool]
                  ,[IsAtHelpNeeded]
                  ,[AT Product Driver]
                  ,[Platform]
                  ,[LastUpdateBy]
                  ,[LastUpdateDt]
                  ,[Cache_LastUpdateDt]
                  ,[Ramp]
                  ,[IsShipped]
                  ,[OrgUnitDescr]
                  ,[Public]
                  ,[Rtd]
                  ,[LastReleasedRtd]
                  ,[BuildTimeType]
                  ,[ExpectedForecastedBuildTime]
                  ,[ExpectedUnforecastedBuildTime]
                  ,[ForecastedBuildTime]
                  ,[UnforecastedBuildTime]
                  ,[ForecastNoticeTime]
                  ,[SupplierPullInProbability]
                  ,[SupplierGapReason]
                  ,[CrossPlantStatus]
               FROM dbo.vInterface_PowerBI_EquipCapacity
               WHERE [DataSetID] = 0"""
    query_succeeded, df, error_msg = querySQL(query, server=server, database='FELCO')
    if not query_succeeded:
        log(False, project_name=project_name, data_area=data_area, error_msg='Unable to connect to the FELCO database on {}.'.format(server))
        print(error_msg)
    else:
        df['LoadDtm'] = pd.to_datetime('today')
        df['LoadBy'] = 'AMR\\' + os.getlogin().upper()

        map_columns(table, df)

        insert_succeeded, error_msg = uploadDFtoSQL(table, df, truncate=True)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
        if insert_succeeded:
            print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))

    print("--- %s seconds ---" % (time() - start_time))
