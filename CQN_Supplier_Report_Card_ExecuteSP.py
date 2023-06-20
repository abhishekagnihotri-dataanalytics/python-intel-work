__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script executes a stored procedure on the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 4:15 AM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import executeStoredProcedure
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":

    # initialize variables
    project_name = 'SRC_ExecuteSP.py'
    data_area = 'Quality SRC'
    insert_succeeded, error_msg = executeStoredProcedure('src.OnlyCorpQuarter')
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=0, error_msg=error_msg)
