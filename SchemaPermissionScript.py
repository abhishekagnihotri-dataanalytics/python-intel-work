__author__ = "Dan K."
__email__ = "William.D.Kniseley@intel.com"
__description__ = "This script run the Schema Permissions for Consumption View users"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Logging import log
from Helper_Functions import executeStoredProcedure


sp_name = 'dbo.spSchemaPermission'
SP_succeeded, error_msg = executeStoredProcedure(sp_name)
log(SP_succeeded, project_name='ADMIN', data_area='Restore Permission-Prod', row_count=1, error_msg=error_msg)
SP_succeeded, error_msg = executeStoredProcedure(sp_name, server='sql2943-fm1-in.amr.corp.intel.com,3180', database='GSCDW')
log(SP_succeeded, project_name='ADMIN', data_area='Restore Permission-PreProd', row_count=1, error_msg=error_msg)



