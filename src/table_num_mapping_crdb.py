import datetime
import subprocess
import sys

import constants
import system_utils


def query_table_num(table_name, host, database, output_file_handle):
    """Queries CRDB for the number matching the table name. Writes that
    number to output_file_handle

    @param table_name name of table i.e. "warehouse"
    @param host host on which CRDB server resides
    @param database database from which to query i.e. "tpcc"
    @param output_file_handle file to write to
    """

    query = "SELECT '{0}'::regclass::oid;".format(table_name)

    sql_command_args = " ".join(
        ["--host {}".format(host), "--database {}".format(database),
         "--execute \"{}\"".format(query)]
    )
    crdb_command = "{0} sql --insecure {1}".format(
        constants.EXE, sql_command_args
    )
    system_utils.call(
        crdb_command, stdout=output_file_handle, stderr=output_file_handle
    )


def map_table_to_num_per_database(table_names, host, database):
    unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_file = "{0}_{1}.txt".format(database, unique_suffix)
    with open(output_file, "w") as output_file_handle:
        for table_name in table_names:
            query_table_num(table_name, host, database, output_file_handle)

    mapping = {}
    with open(output_file, "r") as f:
        for table_name in table_names:
            _ = f.readline()  # "oid"
            table_num = f.readline()
            mapping[int(table_num)] = table_name

    system_utils.call("rm {0}".format(output_file))

    return mapping


def main():
    # with open("hey.txt", "w") as f:
    #     query_table_num("warehouse", "localhost", "tpcc", f)
    #     query_table_num("district", "localhost", "tpcc", f)
    #     query_table_num("customer", "localhost", "tpcc", f)

    print(
        map_table_to_num_per_database(
            ["warehouse", "district", "customer"], "localhost", "tpcc"
        )
    )


if __name__ == "__main__":
    sys.exit(main())
