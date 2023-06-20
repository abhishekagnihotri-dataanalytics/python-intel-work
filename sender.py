import win32com.client
import logger
class EmailSender():
    def __init__(self):
        try:
            self.loggerInit = logger.Log()
            self.loggerInit.logWarning("MISSING EMAIL")
            self.loggerInit.logInfo("TRYING TO CONNECT IN OUTLOOK")
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.loggerInit.logInfo("SUCESSFULLY CONNECTED IN OUTLOOK")
        except Exception as Er:
            self.loggerInit.logError("CONNECTION FAILED BECAUSE: " + str(Er))
    def send_email(self,missing):
        try:
            self.loggerInit.logInfo("SENDING WARNING EMAIL")
            mail = self.outlook.CreateItem(0)
            mail.To = 'abhishek.agnihotri@intel.com'
            mail.Subject = 'Script failed in collect {}'.format(missing)
            mail.Body = "Script couldn't collect {}, please, check manually if emails arrive in Inbox.".format(missing)
            mail.Send()
        except Exception as Er:
            self.loggerInit.logError("EMAIL FAILED BECAUSE: {}".format(Er))
            