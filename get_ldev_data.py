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

debug = True


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
  # input()
  current_report_date_stamp = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d_%H%M%S")
  csv_file = f"report_{current_report_date_stamp}.csv"
  write_report_to_file(csv_file, my_report)


def write_report_to_file(csv_file, my_report):
  # TODO: Get longest dict and set that as the Column names in the CSV file
  longest_dict_in_report_bookmark = {'numer_of_keys': 0, 'index': 0}
  for idx, iteration in enumerate(my_report):
    if len(iteration.keys()) > longest_dict_in_report_bookmark['numer_of_keys']:
      longest_dict_in_report_bookmark['numer_of_keys'] = len(iteration.keys())
      longest_dict_in_report_bookmark['index'] = idx
  logger.info(f"Longest number of keys, columns for excel: {longest_dict_in_report_bookmark['numer_of_keys']}, "
              f"index in array: {longest_dict_in_report_bookmark['index']}")
  csv_columns = my_report[longest_dict_in_report_bookmark['index']].keys()

  # TODO: Write this data to the csv file
  dict_data = my_report
  try:
    with open(csv_file, 'w', newline='') as csvfile:
      csvfile.write('sep=,\n')
      # writer = csv.DictWriter(csvfile)
      writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
      writer.writeheader()
      for data in dict_data:
        writer.writerow(data)
  except IOError:
    logger.error("I/O error writing csv")


def build_report_from_analyzer_detail(analyzer_data, config):
  my_report = []

  for each_array in analyzer_data['result'][0]['data']:
    if debug:
      logger.info(f"Volume recieved: {each_array}")
    my_new_volume_details = {}

    try:
      unpack_nexted_dictionary_n_analyzer_response(each_array, my_new_volume_details,my_report)
    except Exception as ce:
      if debug:
        logger.error(f"ISSUES UNPACKING THIS TOP HEADING in {each_array}")
        traceback.print_exc()
        input("press any key to continue...")

    if debug:
      logger.info(f"I processed this to: {json.dumps(my_report)}")


  return my_report


def unpack_nexted_dictionary_n_analyzer_response(nested_dictionary, my_new_volume_details, my_report):
  # logger.info(f"Nested Dict: {nested_dictionary}")

  if 'related' in nested_dictionary.keys():
    for each_heading in nested_dictionary.keys():
      if 'signature' in each_heading:
        # print(f"My Signature key: {nested_dictionary}")
        my_new_name = nested_dictionary['signature'].split("#")[0]
        my_new_volume_details[my_new_name] = nested_dictionary['signature'].split("#")[1].replace("^", ":")
      elif 'related' in each_heading:
        pass
      else:
        my_new_volume_details[nested_dictionary[each_heading]['name']] = nested_dictionary[each_heading]['data']
    for each_deeper_nested_dict in nested_dictionary['related']:
      unpack_nexted_dictionary_n_analyzer_response(each_deeper_nested_dict, my_new_volume_details,my_report)


  else:
    for each_heading in nested_dictionary.keys():
      if 'signature' in each_heading:
        # print(f"My Signature key: {nested_dictionary}")
        my_new_name = nested_dictionary['signature'].split("#")[0]
        my_new_volume_details[my_new_name] = nested_dictionary['signature'].split("#")[1].replace("^", ":")
      else:
        my_new_volume_details[nested_dictionary[each_heading]['name']] = nested_dictionary[each_heading]['data']
    logger.info(f"Adding this to my report: {my_new_volume_details}")

    my_report.append(my_new_volume_details.copy())
    my_new_volume_details.clear()




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
