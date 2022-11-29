#!/usr/bin/env python3

import argparse
import datetime
import os

import trial_billion as trial_config_object_1
# import trial_CRDB_lt_graphs_2 as trial_config_object_2

import config_io
import constants
import csv_utils
import generate_configs
import latency_throughput
import mail
import run_single_data_point
import sqlite_helper_object
import system_utils

######## configuring the main file ###########

# configuration object generators matched to the latency throughput files
CONFIG_OBJ_LIST = [(trial_config_object_1.ConfigObject(),
                    os.path.join(constants.TEST_CONFIG_PATH, "lt.ini")),

                   # (trial_config_object_2.ConfigObject(), os.path.join(
                   # constants.TEST_CONFIG_PATH, "lt2.ini")),
                   ]

# location of the entire database run
unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
DB_DIR = os.path.join(
    os.getcwd(),
    "scratch/tpcc_crdb_baseline_large_wh_{0}".format(unique_suffix)
)


######## end of configs #############


def generate_dir_name(db_dir, **kwargs):
    config_name = "_".join(
        ["{1}{0}".format(value, key) for key, value in kwargs.items()]
    )
    dir_name = os.path.join(db_dir, config_name)

    return dir_name


def assert_args_are_correct(args):
    if args.recovery_mode and args.recovery_file is not None and args.db_dir \
            is not None:
        return True
    elif args.recovery_mode and args.recovery_file is not None and \
            args.db_dir is None:
        raise RuntimeError(
            "Must specify --db_dir with --recovery_mode and --recovery_file"
        )
    elif args.recovery_mode and args.recovery_file is None and args.db_dir is \
            not None:
        raise RuntimeError(
            "Must specify --recovery_file with --recovery_mode and --db_dir"
        )
    elif args.recovery_mode and args.recovery_file is None and args.db_dir is \
            None:
        raise RuntimeError(
            "Must specify --recovery_file and --db_dir with --recovery_mode"
        )
    elif args.recovery_mode is False and args.recovery_file is not None:
        raise RuntimeError("Must specify --recovery_mode to use any arguments")
    elif args.recovery_mode is False and args.db_dir is not None:
        raise RuntimeError("Must specify --recovery_mode to use any arguments")


def main():
    # takes arguments in the case of recovery
    parser = argparse.ArgumentParser()
    parser.add_argument("--recovery_mode", action="store_true")
    parser.add_argument("--recovery_file")
    parser.add_argument("--db_dir")
    parser.add_argument(
        "--continue_on_failure", action="store_true", default=False,
        help="main halts entire trial on throwing exception"
    )
    args = parser.parse_args()
    assert_args_are_correct(args)

    db_dir = args.db_dir if args.recovery_mode else DB_DIR
    files_to_process = args.recovery_file if args.recovery_mode else \
        os.path.join(
            db_dir, "configs_to_process.csv"
        )

    if not args.recovery_mode:
        # create the database and table
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # populate configs to process
        for cfg_obj, lt_fpath in CONFIG_OBJ_LIST:
            cfg_fpath_list = cfg_obj.generate_all_config_files()
            data = [{
                constants.CONFIG_FPATH_KEY: cfg_fpath, "lt_fpath": lt_fpath
            } for cfg_fpath in cfg_fpath_list]
            csv_utils.append_data_to_file(data, files_to_process)

        try:
            # file of failed configs
            failed_configs_csv = os.path.join(db_dir, "failed_configs.csv")
            f = open(
                failed_configs_csv, "w"
            )  # make sure it's only the failures from this round
            f.close()

            # connect to db
            db = sqlite_helper_object.SQLiteHelperObject(
                os.path.join(db_dir, "trials.db")
            )
            db.connect()
            _, cfg_lt_tuples = csv_utils.read_in_data_as_tuples(
                files_to_process, has_header=False
            )

            for cfg_fpath, lt_fpath in cfg_lt_tuples:

                # generate config object
                cfg = generate_configs.generate_configs_from_files_and_add_fields(
                    cfg_fpath
                )

                # generate lt_config objects that match those config objects
                lt_cfg = config_io.read_config_from_file(lt_fpath)

                try:
                    # make directory in which trial will be run
                    logs_dir = generate_dir_name(
                        db_dir, keys=cfg["n_keys_per_statement"],
                        nodes=cfg["num_warm_nodes"], skew=cfg["skews"]
                    )
                    # logs_dir = generate_dir_name(
                    #    db_dir, warehouses=cfg["warehouses"]
                    # )
                    if not os.path.exists(logs_dir):
                        os.makedirs(logs_dir)

                    # copy over config into directory
                    system_utils.call(
                        "cp {0} {1}".format(
                            cfg[constants.CONFIG_FPATH_KEY], logs_dir
                        )
                    )

                    if cfg["generate_latency_throughput"]:
                        # generate latency throughput trials
                        lt_fpath_csv = latency_throughput.run(cfg, lt_cfg,
                                                              logs_dir)

                        # run trial
                        cfg[
                            "concurrency"] = \
                            latency_throughput.find_optimal_concurrency(
                                lt_fpath_csv
                            )

                    results_fpath_csv = run_single_data_point.run(cfg, logs_dir)

                    # insert into sqlite db
                    # TODO get the actual commit hash, not the branch
                    if cfg["name"] == "kv":
                        db.insert_csv_data_into_sqlite_table(
                            "trials_table", results_fpath_csv, None,
                            logs_dir=logs_dir,
                            cockroach_commit=cfg["cockroach_commit"],
                            server_nodes=cfg["num_warm_nodes"],
                            disabled_cores=cfg["disable_cores"],
                            keyspace=cfg["keyspace"],
                            read_percent=cfg["read_percent"],
                            n_keys_per_statement=cfg["n_keys_per_statement"],
                            skews=cfg["skews"],
                            prepromote_max=cfg["prepromote_max"]
                        )
                    elif cfg["name"] == "tpcc":
                        db.insert_csv_data_into_sqlite_table(
                            "trials_table", results_fpath_csv, None,
                            logs_dir=logs_dir,
                            cockroach_commit=cfg["cockroach_commit"],
                            server_nodes=cfg["num_warm_nodes"],
                            warehouses=cfg["warehouses"],
                            mix=cfg["mix"],
                            wait=cfg["wait"]
                        )

                except BaseException as e:
                    print(
                        "Config {0} failed to run, continue with other"
                        "configs.e:[{1}]".format(
                            cfg[constants.CONFIG_FPATH_KEY], e
                        )
                    )

                    csv_utils.append_data_to_file(
                        [{
                            constants.CONFIG_FPATH_KEY: cfg[
                                constants.CONFIG_FPATH_KEY],
                            "lt_fpath": lt_fpath
                        }], failed_configs_csv
                    )
                    if not args.continue_on_failure:
                        exit(-1)

            db.close()
            mail.email_success()

        except BaseException as e:
            mail.email_failure()
            raise e


if __name__ == "__main__":
    main()
