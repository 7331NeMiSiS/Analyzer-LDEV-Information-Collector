from file_handler import Read_File, Write_File
import os
import json
import logging

# LOGGING
log_id = "CONFIG_HANDLER"
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


def read_config(config_file_name):
  """
  New Json config
  :param config_file_name:
  :return:
  """
  no_config_file = False
  file_json = None

  try:
    file_json = load_json_from_config_file(config_file_name)

    return file_json

  except Exception as ce:
    logger.info(f"No config.cfg file. Creating one")

    no_config_file = True
  # TODO: REMEMBER TO DELTE BEFORE SENDING TO GIT
  if no_config_file == True:
    logger.info(f"Populating a new {config_file_name} file.")

    # DATA TO POPULATE IF NONE EXISTS

    file_json = {'username': "system",
                 'password': 'manager',
                 'analyzer_host': '192.168.1.1',
                 'analyzer_port': '22016',
                 'analyzer_query': 'raidStorage[=name rx .*]/raidLdev[=ldevNaming rx .*]&[=usedCapacityGB rx .*]&[=ldevCapacityGB rx .*]&[=name rx .*]/raidOwner[=name rx .*]',
                 'time_difference_in_hours': 24,
                 'blank_host_group_value': 'Not Mapped',
                 'analyzer_protocol': 'https'}

    data_writer = Write_File(config_file_name)
    logger.info(f"Trying to access: {config_file_name}")
    data_writer.write_file(json.dumps(file_json, indent=4))

    file_json = load_json_from_config_file(config_file_name)

  return file_json


def load_json_from_config_file(config_file_name):
  logger.info(f"Reading config.json file.")
  data_reader = Read_File(os.path.join(os.path.abspath(os.path.dirname(__file__)), config_file_name))
  file_data = data_reader.read_file()
  # logger.info(f"Loading data: {json.dumps(file_data, indent=4)}")
  file_string = ""
  for text in file_data:
    file_string += text
  file_json = json.loads(file_string)

  return file_json
