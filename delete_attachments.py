import os
import logger

 
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