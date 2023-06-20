__author__ = "Prathakini Balakrishnan"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Learning data from SCBI Preprod to Prod since we are unable to get sys_scdata permission to this DB" \
                  "This will run until we switch the source to LearningODS API"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path

from Helper_Functions import uploadDFtoSQL, querySQL
from Logging import log
from Project_params import params


#remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'DataFluency'
    data_area = 'SABA Learning'
    tables = ['[Base].[LearningCourses]','[Base].[LearningInstructors]','[Base].[LearningStudents]']
    sourceserver = 'sql2943-fm1-in.amr.corp.intel.com,3181'
    sourceDB = 'GSCDW'
    params['EMAIL_ERROR_RECEIVER'].append(['prathakini.balakrishnan@intel.com'])

    for table in tables:

        query = """SELECT * from {copy_from}""".format(copy_from=table)

        query_succeeded, df, error_msg = querySQL(query,server=sourceserver, database=sourceDB)

        if query_succeeded:
            insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True,
                                                        driver="{ODBC Driver 17 for SQL Server}")
            log(insert_succeeded, project_name=project_name, data_area='PlannerTasks', row_count=df.shape[0],
                error_msg=error_msg)
            if insert_succeeded:
                print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
            else:
                print(error_msg)

