# python -m pip --proxy=http://proxy-chain.intel.com:911 install teradatasql


import teradatasql

import pandas as pd
import logger
from datetime import datetime
import database
import os
import sys

for arg in sys.argv:
    print(arg)

# Passing the arguments to the Job name. The Job name is 'BlockedInvoices_Qty_Price'
Job = arg

class TeradataExtract():
    def __init__(self) -> None:
        self.loggerInit = logger.Log()

    def extract_teradata(self):
        self.loggerInit.logWarning("CONNECTING TO TERADATA")    
        snapshotdt = datetime.now().strftime('%Y-%m-%d %H%M')
        print (snapshotdt)
        filepath = open(r'C:\\GSPO_Python\ETL\sql\\' + Job + '.txt' , 'r').read()

        with teradatasql.connect(host='tdprd1.intel.com', user=os.environ.get('TeradataUserName'), password=os.environ.get('TeradataPassword')) as connect:    
            df = pd.read_sql(filepath, connect)
            df.to_csv (r'C:\\GSPO_Python\teradata_csv\\' + Job + snapshotdt + '.csv',index=False)
            self.loggerInit.logWarning("TERADATA SAVED TO CSV")    
            data = pd.read_csv(r'C:\\GSPO_Python\teradata_csv\\' + Job + snapshotdt + '.csv',keep_default_na=False)
            self.loggerInit.logWarning("TERADATA FILE SUCCESSFULLY OPENED")
            dbInit = database.DBInsertion()
            dbInit.Insert(data,Job)



Tera = TeradataExtract();
Tera.extract_teradata();