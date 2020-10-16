
import os

import logging

# LOGGING
log_id = "FILE_HANDLER"
extra = {'app_name': log_id}
# Log to file
logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), "Automation_Engine.log"),
                    format='%(asctime)s [%(levelname)s] [%(app_name)s] - %(message)s')
logger = logging.getLogger(__name__)
syslog = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(app_name)s] : %(message)s')
syslog.setFormatter(formatter)
logger.setLevel(logging.DEBUG)

logger.addHandler(syslog)
logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
logger = logging.LoggerAdapter(logger, extra)

class Write_File:
    def __init__(self, file_to_write):
        self.file_to_write = file_to_write

    def write_file(self, file_content):
        # logger.info(file_content)
        with open(self.file_to_write, "w") as fout:
            fout.write(file_content.rstrip())
            # logger.info(file_content)
            # fout.write(file_content)


class Read_File:
    def __init__(self, file_to_read):
        self.file_to_read = file_to_read

    def read_file(self):
        data = []
        logger.info(f"Reading file: {self.file_to_read}")
        with open(self.file_to_read) as reader:
            for read in reader.readlines():
                data.append(read.rstrip())
                # logger.info(read)
                # data.append(read)

        return data
    def read_line(self):
        data = []
        logger.info(f"Reading file: {self.file_to_read}")
        with open(self.file_to_read) as reader:
            data=reader.readline()

        return data
