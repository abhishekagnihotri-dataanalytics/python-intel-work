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
import logging
from datetime import datetime
from zipfile import ZipFile
import win32com.client
import logger


class Log():
    def __init__(self):
        self.logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
        self.dt = datetime.today().strftime('%Y-%m-%d')
        logging.basicConfig(format=self.logFormatter, level=logging.DEBUG, datefmt="%m/%d/%Y %I:%M:%S %p", filename=r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\Sustainability\Ariba\\automation_{}.log".format(self.dt), filemode="a")
        self.logger = logging.getLogger(__name__)

    def logInfo(self,message):
        self.logger.info(message)

    def logWarning(self,message):
        self.logger.warning(message)

    def logError(self,message):
        self.logger.error(message)


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

    def send_email(self, missing):
        try:
            self.loggerInit.logInfo("SENDING WARNING EMAIL")
            mail = self.outlook.CreateItem(0)
            mail.To = 'abhishek.agnihotri@intel.com'
            mail.Subject = 'Script failed in collect {}'.format(missing)
            mail.Body = "Script couldn't collect {}, please, check manually if emails arrive in Inbox.".format(missing)
            mail.Send()
        except Exception as Er:
            self.loggerInit.logError("EMAIL FAILED BECAUSE: {}".format(Er))

def unzip(path,folder):
    try:
        loggerInit = logger.Log()
        loggerInit.logWarning('STARTING TO UNZIP FILE')
        with ZipFile(path, 'r') as zipObj:
            zipObj.extractall(folder)
            loggerInit.logInfo('UNZIPING CONCLUDED')
            loggerInit.logInfo('RESULTED FILE: {}'.format(str(zipObj.filelist[0].filename)))
    except Exception as Er:
        loggerInit.logError('UNZIPING FAILED BECAUSE: {}'.format(Er))
    return str(zipObj.filelist[0].filename)



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
            self.outlook_inbox = self.outlook.Folders("khushboo.saboo@intel.com").Folders("Inbox").Items
            loggerInit.logInfo("SUCESSFULLY CONNECTED IN my ACCOUNT")
        except Exception as Er:
            loggerInit.logError("CONNECTION FAILED BECAUSE: " + str(Er))
        try:
            loggerInit.logWarning("TRYING TO COLLECT TODAY EMAILS")
            inbox_today = self.outlook_inbox.Restrict(
                "[ReceivedTime] > '{} 00:00 AM'".format(datetime.today().strftime('%Y-%m-%d')))
            for msg in list(inbox_today):
                if msg.ConversationTopic.find('Background Report All Contracts- Operational Analytics has completed') != -1:
                    excp_email = msg
                # if msg.ConversationTopic.find('bleh3') != -1:
                #     sld_email = msg
                if not ('excp_email' in locals()):
                    excp_email = 'Background Report All Contracts- Operational Analytics has completed'
                # elif not ('sld_email' in locals()):
                #     sld_email = 'bleh3'
            loggerInit.logInfo("QUANTITY EMAILS LOCATED: {}".format(len(list(inbox_today))))
        except Exception as Er:
            loggerInit.logError("EMAILS COLLECTION FAILED BECAUSE: " + str(Er))

        return  excp_email
            # sld_email




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

def delete(path):
    loggerInit = logger.Log()
    try:
        dir = path
        loggerInit.logWarning('LOOKING FOR FOLDER TO CLEAR TO START ATTACHMENT COLLECTION')
        if len(list(os.scandir(dir))) > 0:
            for file in os.scandir(dir):
                loggerInit.logWarning('DELETING....')
                os.remove(file.path)
                loggerInit.logInfo('DELETED FILE {}'.format(file.name))
            loggerInit.logInfo('FOLDER CLEANED')
        else:
            loggerInit.logInfo('FOLDER WAS ALREADY CLEANED')
    except Exception as Er:
        loggerInit.logError('DELETING FAILED BECAUSE {}'.format(Er))

# INITIALIZE SCRIPT
# This step includes data cleansing and covert the xls file to csv files.
loggerInit = logger.Log()
loggerInit.logInfo("SCRIPT STARTED")
# jpype.startJVM()
emailsOutlook = Outlook()
print(emailsOutlook.get_emails())
a = emailsOutlook.get_emails()

for i in a:
    if not (isinstance(i, str)):
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



