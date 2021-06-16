import config_io


def generate_config_files_and_add_fields(config_objects):
  config_file_list = []
  for co in config_objects:
    config_file_list += co.generate_all_config_files()

  return config_file_list


def generate_configs_from_files_and_add_fields(file):
  cfg = config_io.read_config_from_file(file)
  cfg["config_fpath"] = file

  return cfg


def generate_configs_from_files_and_add_fields_list(config_file_list):
  return [generate_configs_from_files_and_add_fields(file) for file in config_file_list]
