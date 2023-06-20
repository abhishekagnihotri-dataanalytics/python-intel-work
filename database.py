import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import pandas as pd
import logger
import os



class DBInsertion():
    def __init__(self) -> None:
        # Development Server
        self.connection_config = {
            'host': 'maria3107-lb-pg-in.dbaas.intel.com',
            'port': 3307,
            'user': 'GSCO_GSPO_rw',
            'password': os.environ.get('GSCO_GSPO_DevPassword'),
            'database': 'GSCO_GSPO',
            # 'ssl_ca': '/usr/local/share/ca-certificates/IntelSHA256RootCA-base64.crt',
            'tls_versions': ['TLSv1.2', 'TLSv1.1']
        }
        # Production Server
        # self.connection_config = {
        #     'host': 'maria4113-lb-fm-in.iglb.intel.com',
        #     'port': 3307,
        #     'user': 'GSCO_GSPO_rw',
        #     'password': os.environ.get('GSCO_GSPO_Password'),
        #     'database': 'GSCO_GSPO',
        #     # 'ssl_ca': '/usr/local/share/ca-certificates/IntelSHA256RootCA-base64.crt',
        #     'tls_versions': ['TLSv1.2', 'TLSv1.1']
        # }
        try:
            self.loggerInit = logger.Log()
            self.loggerInit.logWarning('TRYING TO CONNECT IN MARIADB')
            self.connection = mysql.connector.connect(**self.connection_config)
            self.cursor = self.connection.cursor(dictionary=True)
            self.loggerInit.logInfo('CONNECTED SUCESSFULLY IN MARIADB')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.loggerInit.logError("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.loggerInit.logError("Database does not exist")
            else:
                self.loggerInit.logError(err)
    def Insert(self,dataframe, database):
        self.df = dataframe
        column_length = len(self.df.columns) 
        len_rows = len(self.df.index)
        s = ",%s"
        column = column_length* s
        snapshotdt = datetime.now().strftime('%Y-%m-%d %H:%M')
        self.cursor.fast_executemany = True
        insert_statement = "INSERT INTO GSCO_GSPO.{} VALUES('{}'{})".format(database,snapshotdt,column)
        try:
            self.loggerInit.logWarning('CREATING DATA BATCH')
            insert_params = list(self.df.itertuples(index=False, name='Pandas'))
            self.loggerInit.logInfo('CREATED')
            self.loggerInit.logWarning('INSERTING DATA IN DATABASE')
            self.cursor.executemany(insert_statement,insert_params)
            self.count = self.cursor.rowcount

            if (self.count - len_rows) == 0:
                self.connection.commit()
                self.loggerInit.logInfo('DATA INSERTED SUCESSFULLY IN DATABASE {}'.format(database))
                self.loggerInit.logInfo('DATA INSERTED: {}'.format(self.count))
            else:
                self.connection.rollback()
                self.loggerInit.logError('DATA INSERTION FAILED IN DATABASE {}'.format(database))
                self.loggerInit.logInfo('ROLLING BACK')
        except Exception as Er:
                self.loggerInit.logError(Er)