import argparse
import csv
import gzip
import math
import os
import shlex
import subprocess
import sys
import time
import system_utils

from constants import EXE

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
WRITE_KEYS_EXE = os.path.join(CURRENT_DIR, "write_keyspace_to_file.py")
MAX_DATA_ROWS_PER_FILE = 1000000


def append_server_num_to_filename(original_filename, server_num):
    original_fname_components = original_filename.split(".")
    fname_components = original_fname_components[:-1] + ["_" + str(
        server_num
    )] + original_fname_components[-1:] + ["gz"]
    fname = ".".join(fname_components)
    return fname


def transform_row(i):
    return i + 256 ** 5


def write_keyspace_to_file(fname, range_max, range_min, payload_size,
                           enable_fixed_sized_encoding=True):
    # forming payload
    payload = ""
    for i in range(0, payload_size, len("jennifer")):
        payload += "jennifer"
    for i in range(len(payload), payload_size):
        payload += "l"

    with gzip.open(fname, "wt") as f:
        writer = csv.writer(f)

        for i in range(range_min, range_max):
            key = i
            if enable_fixed_sized_encoding:
                key = transform_row(i)
            writer.writerow((key, payload))
    print("done with", fname)


def populate(filename, range_max, range_min=0, servers=1, payload_size=512,
             enable_fixed_sized_encoding=True):

    # number of files to be written
    num_data_files = int((range_max - range_min) / MAX_DATA_ROWS_PER_FILE)
    data_per_file = MAX_DATA_ROWS_PER_FILE

    # # keyspace per server, in case keyspace is smaller
    # keyspace_per_server = int((range_max - range_min) / servers)
    # data_per_file = min(keyspace_per_server, MAX_DATA_ROWS_PER_FILE)
    # num_files = max(servers, num_data_files)

    num_files = num_data_files

    # num_files = servers
    bookmark = range_min
    processes = []
    for i in range(0, num_files):
        fname = append_server_num_to_filename(filename, i)

        if i == num_files - 1:
            # last file to be written
            cmd = "python3 {0} --location_of_file {1} --range_max {2} " \
                  "--range_min {3} " \
                  "--payload_size {4} --enable_fixed_sized_encoding {5}"\
                .format(WRITE_KEYS_EXE, fname, range_max + 1, bookmark,
                payload_size, enable_fixed_sized_encoding)
            process = subprocess.Popen(shlex.split(cmd))
            processes.append(process)
        else:
            cmd = "python3 {0} --location_of_file {1} --range_max {2} " \
                  "--range_min {3} " \
                  "--payload_size {4} --enable_fixed_sized_encoding {5}"\
                .format(WRITE_KEYS_EXE, fname, bookmark + data_per_file,
                bookmark, payload_size, enable_fixed_sized_encoding)
            process = subprocess.Popen(shlex.split(cmd))
            processes.append(process)

        bookmark += data_per_file

        if len(processes) >= 16:
            for p in processes:
                p.wait()
            processes = []


def upload_nodelocal(
    location_of_file, nfs_location, hostport="localhost:26257"
):
    upload_cmd = "{0} nodelocal upload --insecure --host {1} {2} " \
                 "{3}".format(
        EXE, hostport, location_of_file, nfs_location
    )
    system_utils.call(upload_cmd)


def import_into_crdb(server, nfs_locations):
    import_cmd = 'echo "IMPORT INTO kv (k, v) CSV DATA('

    for i in range(len(nfs_locations)):
        csv = nfs_locations[i]
        if i == len(nfs_locations) - 1:
            import_cmd += '\\\"nodelocal://1/{0}\\\"'.format(csv)
        else:
            import_cmd += '\\\"nodelocal://1/{0}\\\",'.format(csv)

    import_cmd += ');" | {0} sql --insecure --database=kv'.format(EXE)
    system_utils.call_remote(server, import_cmd)


def snapshot(server, snapshot_name, database):
    snapshot_cmd = 'echo "BACKUP TO \\\"nodelocal://1/{1}\\\";"' \
                   ' | {0} sql --insecure --database={2}'\
        .format(EXE, snapshot_name, database)
    system_utils.call_remote(server, snapshot_cmd)


def restore(ipaddr, snapshot_name, database):

    restore_cmd = 'echo "RESTORE DATABASE {2} FROM \\\"nodelocal://1/{1}\\\";"'\
                  ' | {0} sql --insecure'.format(EXE, snapshot_name, database)
    system_utils.call_remote(ipaddr, restore_cmd)


def main():
    parser = argparse.ArgumentParser()
    filename = "/proj/cops-PG0/workspaces/jl87/populate1B.csv"
    range_max = 300000000
    tic = time.perf_counter()
    populate(filename, range_max, range_min=0, servers=3)
    toc = time.perf_counter()
    print(f"elapsed {toc - tic:0.4f} seconds")

    return 0


if __name__ == "__main__":
    sys.exit(main())
