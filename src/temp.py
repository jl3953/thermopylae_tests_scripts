import argparse
import os
import sys

import config_io
import sqlite_helper_object


def main():
    """ Adds trials to the db file again when the normal script messes up. """

    parser = argparse.ArgumentParser()
    parser.add_argument("--db_file", type=str, required=True)
    parser.add_argument("--trial_dir", type=str, required=True)
    args = parser.parse_args()

    db = sqlite_helper_object.SQLiteHelperObject(args.db_file)
    db.connect()
    for dir in os.walk(args.trial_dir):
        dirname = dir[0]
        subdirs = dir[1]
        files = dir[2]
        if "latency_throughput" in subdirs and "logs" in subdirs:
            csv = os.path.join(dirname, "results.csv")
            ini_file = files[0] if ".ini" in files[0] else files[1]

            params = config_io.read_config_from_file(os.path.join(dirname, ini_file))

            db.insert_csv_data_into_sqlite_table("trials_table", csv, None,
                                                 logs_dir="jennlost",
                                                 cockroach_commit=params["cockroach_commit"],
                                                 server_nodes=params["num_warm_nodes"],
                                                 disabled_cores=params["disable_cores"],
                                                 keyspace=params["keyspace"],
                                                 read_percent=params["read_percent"],
                                                 n_keys_per_statement=params["n_keys_per_statement"],
                                                 skews=params["skews"])
    db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())

