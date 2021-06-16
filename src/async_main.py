import datetime
import os
import sys

import async_config_object  # implement this interface
import async_server  # implement this interface
import config_io
import constants
import csv_utils
import generate_configs
import async_latency_throughput as latency_throughput
import sqlite_helper_object
import system_utils

############ CONFIGURING THIS MAIN FILE ############
CONFIG_OBJ_LIST = [
    (async_config_object.ConfigObject(), os.path.join(os.getcwd(), "config", "async_lt.ini"))
]

# location of the entire database run
# this defaults to thermopylae_tests/scratch/db_{whatever} directory
DB_DIR = os.path.join(os.getcwd(), "scratch", "db_{0}".format(
    datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")))


def run_single_trial(server_node, server_commit_branch, server_concurrency,
                     client_nodes, client_commit_branch, client_concurrency,
                     raw_logs_dir, duration, batch, read_percent):
    """ Runs a single trial i.e. cleans out all traces of remaining experiments,
    runs a server, runs the clients, aggregates the data from clients,
    and returns that data.

    Args:
        Whatever you need them to be.

    Returns:
        A single dictionary of the aggregated data.
        """

    # cleanup
    for node in client_nodes + [server_node]:
        async_server.kill(node)

    # start server
    async_server.build_server(server_node,
                              server_commit_branch)
    _ = async_server.run_server(server_node,
                                server_concurrency)

    # run clients
    if not os.path.exists(raw_logs_dir):
        os.mkdir(raw_logs_dir)
    for client in client_nodes:
        async_server.build_client(client, client_commit_branch)

    logfiles = async_server.run_clients(client_nodes,
                                        server_node,
                                        duration,
                                        client_concurrency,
                                        batch,
                                        read_percent,
                                        raw_logs_dir)

    # cleanup again
    for node in client_nodes + [server_node]:
        async_server.kill(node)

    # aggregate logfiles
    aggregate_data, has_data = async_server.aggregate_raw_logs(logfiles)
    if not has_data:
        raise RuntimeError("Failed to produce results")

    return aggregate_data


def run_single_trial_wrapper(config, trial_logs_location):
    """ Wraps run_single_trial function so that the input and output
    parameters match the following interface.

    Args:
        config (dict): a dictionary
        trial_logs_location (str): location that the trial will log.

    Returns:
        (str) absolute filepath of the results.
        """
    trial_data = run_single_trial(config["server_node"],
                                  config["server_commit_branch"],
                                  config["server_concurrency"],
                                  config["workload_nodes"],
                                  config["client_commit_branch"],
                                  config["concurrency"],
                                  trial_logs_location,
                                  config["duration"],
                                  config["batch"],
                                  config["read_percent"])

    results_fpath = os.path.join(trial_logs_location, "results.csv")
    _ = csv_utils.write_out_data([trial_data], results_fpath)

    return results_fpath


def adjust_cfg(config, **kwargs):
    """ Adjusts the config dictionary to include any paramters needed
    when it's passed into run_single_trial_wrapper.

    Args:
        config (dict): original config
        kwargs: any other paramters that need to be added. May be none.

    Returns:
        Adjusted config.
        """

    return config


############ END OF CONFIGURING MAIN ######################


############ Some helper functions that main needs ###########
# You probably don't need to change them

def generate_dir_name(config_fpath, db_dir):
    config_file = os.path.basename(config_fpath)
    config_name = config_file.split('.')[0]
    dir_name = os.path.join(db_dir, config_name)

    return dir_name


def setup_sqlite_db(sqlite_db_dir):
    # connect to db
    if not os.path.exists(sqlite_db_dir):
        os.makedirs(sqlite_db_dir)

    db = sqlite_helper_object.SQLiteHelperObject(
        os.path.join(sqlite_db_dir, "trials.db"))
    db.connect()

    return db


def insert_into_sqlite_db(db_connector, results_fpath_csv, logs_dir, cfg):
    # insert into sqlite db
    cfg.update({"logs_dir": logs_dir})
    db_connector.insert_csv_data_into_sqlite_table("trials_table", results_fpath_csv, cfg)


def process_and_setup_configs(sqlite_db_dir, config_obj_list):
    # populate configs to process
    if not os.path.exists(sqlite_db_dir):
        os.makedirs(sqlite_db_dir)

    files_to_process = os.path.join(sqlite_db_dir, "configs_to_process.csv")

    for cfg_obj, lt_fpath in config_obj_list:
        cfg_fpath_list = cfg_obj.generate_all_config_files()
        data = [{constants.CONFIG_FPATH_KEY: cfg_fpath,
                 "lt_fpath": lt_fpath} for cfg_fpath in cfg_fpath_list]
        csv_utils.append_data_to_file(data, files_to_process)

    _, cfg_lt_tuples = csv_utils.read_in_data_as_tuples(files_to_process, has_header=False)

    return cfg_lt_tuples


############## End of helper functions ########################


def main():
    cfg_lt_pairs = process_and_setup_configs(DB_DIR, CONFIG_OBJ_LIST)
    # db_connector = setup_sqlite_db(DB_DIR)

    for cfg_fpath, lt_fpath in cfg_lt_pairs:

        # generate config object
        cfg = generate_configs.generate_configs_from_files_and_add_fields(cfg_fpath)

        # generate lt_config objects that match those config objects
        lt_cfg = config_io.read_config_from_file(lt_fpath)

        # make directory in which trial will be run
        logs_dir = generate_dir_name(cfg[constants.CONFIG_FPATH_KEY], DB_DIR)
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        # copy over config into directory
        system_utils.call("cp {0} {1}".format(cfg[constants.CONFIG_FPATH_KEY], logs_dir))

        # generate latency throughput trials
        cfg = adjust_cfg(cfg)
        lt_fpath_csv = latency_throughput.run(cfg,
                                              lt_cfg,
                                              logs_dir,
                                              run_func=run_single_trial_wrapper)

        # run trial
        cfg["concurrency"] = latency_throughput.find_optimal_concurrency(lt_fpath_csv)
        cfg = adjust_cfg(cfg)
        results_fpath_csv = run_single_trial_wrapper(cfg, logs_dir)

        # insert results in sqlite db # THIS INSERTION METHOD ISN'T CORRECT
        # insert_into_sqlite_db(db_connector,
        #                       results_fpath_csv,
        #                       logs_dir,
        #                       cfg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
