from zipfile import ZipFile
import logger


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

