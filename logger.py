import logging
from datetime import datetime


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
