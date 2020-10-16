import requests
import json
import datetime
import os
import logging
import traceback
import csv
from base64 import b64encode
from config_file_handler import *
import urllib3

urllib3.disable_warnings()

debug = False


def set_logging():
  global logger
  log_id = "AnalyzerLogger"
  extra = {'app_name': log_id}
  # Log to file
  logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), "application_ouput.log"),
                      format='%(asctime)s [%(levelname)s] [%(app_name)s] - %(message)s')
  logger = logging.getLogger(__name__)
  syslog = logging.StreamHandler()
  formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(app_name)s] : %(message)s')
  syslog.setFormatter(formatter)
  logger.setLevel(logging.DEBUG)
  logger.addHandler(syslog)
  logger = logging.LoggerAdapter(logger, extra)


def main():
  logger.info("===================================")
  logger.info("=  Get Volume Data from Analyzer  =")
  logger.info("===================================")
  logger.info("")

  config = read_config("config.json")

  userAndPass = encoded_username_and_password(config)

  analyzer_data = get_volume_information(config, userAndPass)

  my_report = build_report_from_analyzer_detail(analyzer_data, config)

  logger.info(f"My Report: {my_report}")

  csv_file = "report.csv"
  write_report_to_file(csv_file, my_report)


def write_report_to_file(csv_file, my_report):
  csv_columns = my_report[0].keys()
  dict_data = my_report
  try:
    with open(csv_file, 'w', newline='') as csvfile:
      csvfile.write('sep=,\n')
      writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
      writer.writeheader()
      for data in dict_data:
        writer.writerow(data)
  except IOError:
    logger.error("I/O error writing csv")


def build_report_from_analyzer_detail(analyzer_data, config):
  my_report = []
  for each_array in analyzer_data['result'][0]['data']:

    array = each_array['name']['data']
    for each_volume in each_array['related']:
      if debug:
        logger.info(f"Volume recieved: {each_volume}")
      # print(each_volume.keys())

      # TODO: print signature
      volume_name = each_volume['signature'].replace('^', ':')
      my_new_volume_details = {'array': array,
                               'name': volume_name}
      # TODO: IF KEY has DATA print it
      for each_volume_key in each_volume.keys():
        try:
          if 'data' in each_volume[each_volume_key].keys():
            my_new_volume_details[each_volume_key] = each_volume[each_volume_key]['data']

        except Exception as ce:
          pass
      if 'related' in each_volume.keys():
        my_new_volume_details['host_group'] = each_volume['related'][0]['name']['data']
      else:
        my_new_volume_details['host_group'] = config['blank_host_group_value']
      if debug:
        logger.info(f"I got the following line: {each_volume}, "
                    f"I processed this to: {json.dumps(my_new_volume_details)}")
      my_report.append(my_new_volume_details)
  return my_report


def get_volume_information(config, userAndPass):
  url = f"{config['analyzer_protocol']}://{config['analyzer_host']}:{config['analyzer_port']}/Analytics/v1/services/DatabaseQuery/actions/invokeQuery/invoke"
  logger.info(f"URL GET - {url}")
  start_time = datetime.datetime.strftime(datetime.datetime.now() -
                                          datetime.timedelta(hours=config['time_difference_in_hours']),
                                          "%Y%m%d_%H%M%S")
  # start_time = datetime.datetime.now() - datetime.timedelta(hours=config['time_difference_in_hours'])
  end_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d_%H%M%S")
  payload = {"name": "invokeQuery", "href": "v1/services/DatabaseQuery/actions/invokeQuery/invoke", "method": "POST",
             "parameters": [{"dcaRequestUrlParameter": {"dataset": "defaultDs", "processSync": "true"},
                             "dcaRequestBody": {
                               "query": config['analyzer_query'],
                               "startTime": start_time, "endTime": end_time,
                               "utcOffset": "UTC+00:00"},
                             "page": 1, "pageSize": 0}]}
  headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Authorization': "Basic " + str(userAndPass),
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/json; charset=UTF-8',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty'
  }
  response = requests.request("POST", url, headers=headers, data=json.dumps(payload), verify=False)
  logger.info(f"URL Response: {response.status_code} : {url}")
  if response.status_code != 200:
    logger.error(f"Error while accessing url {url} - {response.text}")
    exit(1)
  elif response.status_code == 401:
    logger.error(f"Error while accessing url {url} - {response.text}")
    logger.error(f"BE SURE TO UPDATE USERNAME AND PASSWORD in config.json file!")
    exit(1)

  analyzer_data = response.json()
  return analyzer_data


def encoded_username_and_password(config):
  username_password_combo = str(config['username']) + ":" + str(config['password'])
  userAndPass = b64encode(username_password_combo.encode('ascii')).decode("ascii")
  return userAndPass


if __name__ == '__main__':
  set_logging()
  main()
