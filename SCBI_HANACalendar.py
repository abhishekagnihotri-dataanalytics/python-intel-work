__author__ = "Pratha Bala"
__email__ = "prathakini.balakrishnan@intel.com"
__description__ = "This script loads Worker Organization data from HANA to GSCDW DB"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
from Helper_Functions import queryHANA, uploadDFtoSQL, executeStoredProcedure,map_columns
import pandas as pd
from Logging import log
from Project_params import params

#remove the current file's parent directory from sys.path since it was only needed for imports above
try:
   sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # initialize variables
    project_name = 'SCBI Core Data'
    data_area = 'HANACalendar'
    base_table = 'Base.FinancialCalendarFiscalDayView'
    load_by = 'HANA'
    params['EMAIL_ERROR_RECEIVER'].append('prathakini.balakrishnan@intel.com')

    query = """SELECT
	 "Date",
	 "DateSQL",
	 "FiscalYear",
	 "YearInt",
	 "YearQtrVarchar",
	 "YearQtr",
	 "YearMonthVarchar",
	 "YearMonth",
	 "FiscalQuarter",
	 "FiscalQuarterNm",
	 "FiscalYearQuarterNm",
	 "FiscalPeriod",
	 "PeriodInt",
	 "PeriodDesc",
	 "FiscalMonthNm",
	 "FiscalYearWorkWeekVarchar",
	 "FiscalWorkWeek",
	 "FiscalWW",
	 "WorkWeek",
	 "WorkWeekNm",
	 "DayofWeekNbr",
	 "DeltaMonths",
	 "DeltaQuarters",
	 "DeltaYears",
	 "DeltaWeeks",
	 "DeltaDays" 
FROM "_SYS_BIC"."intel.masterdata.calendar/FinanceCalendarFiscalDayView" 
            """
    df = queryHANA(query, environment='Production')

    df['LoadDtm'] = pd.to_datetime('today')
    df['LoadBy'] = load_by

   # map_columns(base_table,df)

    # upload dataframe to SQL
    insert_succeeded, error_msg = uploadDFtoSQL(base_table, df, truncate=True)
    if insert_succeeded:
        print('Successfully copied {0} records in {1} from {2} to {3}'.format(df.shape[0], base_table, load_by, params['GSMDW_DB']))

        sp_name = 'ETL.spLoadFinancialCalendar'
        sp_succeeded, error_msg = executeStoredProcedure(sp_name)
        if sp_succeeded:
            print("Successfully executed stored procedure that loads FinancialCalendarFiscalDayView Integration table")

        else:
            log(sp_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    else:
        print(error_msg)
        log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
