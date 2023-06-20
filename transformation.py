from sqlalchemy import true
import logger
import os
import pandas as pd 




class FileTransformation():
    def __init__(self) -> None:
        self.loggerInit = logger.Log()

    def csv_from_excel(self,excel_file, file):
        try:
            self.loggerInit.logWarning('CREATING TEMPORARY JAVA ENV')
            from asposecells.api import Workbook
            self.loggerInit.logWarning('STARTING TO CONVERTING FILE')
            workbook = Workbook(excel_file)
            workbook.save(r'C:\\temp\\{}'.format(os.path.splitext(file)[0]) + '.csv')
            self.loggerInit.logInfo('SUCESSFULLY CONVERTED FILE')
            self.loggerInit.logInfo('RESULTED FILE: {}'.format(os.path.splitext(file)[0]) + '.csv')
            self.loggerInit.logWarning('ENDING TEMPORARY JAVA ENV')

            self.filepath = (r'C:\\temp\\{}'.format(os.path.splitext(file)[0]) + '.csv')
        except Exception as Er:
            self.loggerInit.logError('CONVERSION FAILED BECAUSE: {}'.format(Er))
        
        return self.data_cleaning()


    def data_cleaning(self):
        try:
            self.loggerInit.logWarning('OPENING CONVERTED FILE')
            df = pd.read_csv(self.filepath, skiprows=5, header=0, na_values='')
            self.loggerInit.logInfo('FILE SUCESSFULLY OPENED')
            df.drop(df.tail(1).index,inplace=True)
            df.fillna('',inplace=True)
            df = df.replace('Unclassified', '')
            self.loggerInit.logInfo('LAST ROW REMOVED')
            return df 
        except Exception as Er:
            self.loggerInit.logError('OPENING FAILED BECAUSE: {}'.format(Er))




