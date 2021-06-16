import configparser
import json


def write_config_to_file(config_dict, ini_fpath):
  """ Writes a configuration to an ini file.

  :param config_dict: (Dict) config to write
  :param ini_fpath: (str) fpath to ini file
  :return: (str) ini_file written to
  """
  config = configparser.ConfigParser()
  config["DEFAULT"] = {key: json.dumps(value) for key, value in config_dict.items()}
  with open(ini_fpath, "w") as ini:
    config.write(ini)
  return ini_fpath


def read_config_from_file(ini_fpath):
  """
  Reads a config file

  :param ini_fpath:
  :return: a dictionary of config parameters
  """
  config = configparser.ConfigParser()
  config.read(ini_fpath)

  result = {}
  for key in config["DEFAULT"]:
    result[key] = json.loads(config["DEFAULT"][key])
  return result
