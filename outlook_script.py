# python -m pip --proxy=http://proxy-chain.intel.com:911 install mysql-connector-python
# python -m pip --proxy=http://proxy-chain.intel.com:911 install schedule
# python -m pip --proxy=http://proxy-chain.intel.com:911 install pyinstaller
# python -m pip --proxy=http://proxy-chain.intel.com:911 install pandas
# python -m pip --proxy=http://proxy-chain.intel.com:911 install xlrd
# python -m pip --proxy=http://proxy-chain.intel.com:911 install openpyxl
# python -m pip --proxy=http://proxy-chain.intel.com:911 install aspose-cells
# On the VM, after this file is updated, run this command to generate a .exe file:  pyinstaller scan_execute_dev.py --onefile


#This script is scheduled to run daily.  The Ariba Exception Report is schedule send an attachment of the data to gspo.sys.account@intel.com account.
#This script will first extract the attachment from the email, convert it to csv file.  Then it will load the data on the GSPO Maria DB.



from turtle import goto
import logger
import sender
import jpype
# import database
import delete_attachments
import unziper
import transformation
import win32com.client
import os
from datetime import datetime, timedelta

# Open outlook
class Outlook():
    def __init__(self):
        try:
            loggerInit.logWarning("TRYING TO CONNECT IN OUTLOOK")
            self.outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
            loggerInit.logInfo("SUCESSFULLY CONNECTED IN OUTLOOK")
        except Exception as Er:
            loggerInit.logError("CONNECTION FAILED BECAUSE: " + str(Er))
# Open email from the gspo.sys.account mail box and retrieve the email with Recoiling and Solved as Subject lines that was sent today.
    def get_emails(self):
        try:
            loggerInit.logWarning("TRYING TO CONNECT IN my ACCOUNT")
            self.outlook_inbox  = self.outlook.Folders("abhishek.agnihotri@intel.com").Folders("Inbox").Items
            loggerInit.logInfo("SUCESSFULLY CONNECTED IN my ACCOUNT")
        except Exception as Er:
            loggerInit.logError("CONNECTION FAILED BECAUSE: " + str(Er))  
        try:
            loggerInit.logWarning("TRYING TO COLLECT TODAY EMAILS")
            inbox_today = self.outlook_inbox.Restrict("[ReceivedTime] > '{} 00:00 AM'".format(datetime.today().strftime('%Y-%m-%d')))
            for msg in list(inbox_today):
                if msg.ConversationTopic.find('bleh2') != -1:
                    excp_email = msg
                if msg.ConversationTopic.find('bleh3') != -1:
                    sld_email = msg
                if not('excp_email' in locals()):
                    excp_email = 'bleh2'
                elif not('sld_email' in locals()):
                    sld_email = 'bleh3'
            loggerInit.logInfo("QUANTITY EMAILS LOCATED: {}".format(len(list(inbox_today))))
        except Exception as Er:
            loggerInit.logError("EMAILS COLLECTION FAILED BECAUSE: " + str(Er))
        
        return excp_email, sld_email

# This definition will get the email attachments and save it to the folder.  It will delete the previous version and placed the latest version in the folder.
# It will also unzip the file.
def attachment(email):
    try:
        loggerInit.logWarning('TYING TO EXTRACT EMAIL ATTACHMENT')
        attachments = email.Attachments
        attachment = attachments.Item(1)
        if attachment != None:
            path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\Sustainability\Ariba"
            loggerInit.logInfo('ATTACHMENT LOCATTED')
            delete_attachments.delete(path)
            try:
                for attachment in email.Attachments:
                    loggerInit.logWarning('TYING TO SAVE EMAIL ATTACHMENT IN DESTINATION')
                    attachment.SaveAsFile(os.path.join(path, str(attachment)))
                    filename = unziper.unzip(os.path.join(path, str(attachment)), path)
                    loggerInit.logInfo('SUCESSFULLY SAVED')
            except Exception as Er:
                loggerInit.logError("ATTACHMENT COULDN'T BE SAVED BECAUSE: " + str(Er))   
    except Exception as Er:
            loggerInit.logError("ATTACHMENT COLLECTION FAILED BECAUSE: " + str(Er))
  
    return (os.path.join(path, filename), filename)


#INITIALIZE SCRIPT
# This step includes data cleansing and covert the xls file to csv files.
loggerInit = logger.Log()
loggerInit.logInfo("SCRIPT STARTED")
# jpype.startJVM()
emailsOutlook = Outlook()
a = [excp_mail, sld_email] = emailsOutlook.get_emails()

for i in a:
    if not(isinstance(i,str)):
        path, file = attachment(i)
        # data_cleaning = transformation.FileTransformation()
        # data = data_cleaning.csv_from_excel(path, file)
        # if str(i.ConversationTopic).find('Solved') != -1:
        #     dbInit = database.DBInsertion()
        #     dbInit.Insert(data,'AribaHistoricalInvoiceExceptionSolved')
        # elif str(i.ConversationTopic).find('Reconciling') != -1:
        #     dbInit = database.DBInsertion()
        #     dbInit.Insert(data,'AribaHistoricalInvoiceExceptionReconciling')
    else:
        pass
      # emailInit = sender.EmailSender()
      # emailInit.send_email(i)

# jpype.shutdownJVM()



