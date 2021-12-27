import csv
import gzip
import os
import shlex
import subprocess
import sys
import time
import system_utils

from constants import EXE

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
WRITE_KEYS_EXE = os.path.join(CURRENT_DIR, "write_keyspace_to_file.py")


def append_server_num_to_filename(original_filename, server_num):
    original_fname_components = original_filename.split(".")
    fname_components = original_fname_components[:-1] + ["_" + str(
        server_num
    )] + original_fname_components[-1:] + ["gz"]
    fname = ".".join(fname_components)
    return fname


def write_keyspace_to_file(fname, range_max, range_min):
    with gzip.open(fname, "wt") as f:
        writer = csv.writer(f)

        for i in range(range_min, range_max):
            writer.writerow((i, i))


def populate(filename, range_max, range_min=0, servers=1):
    # files_per_1m = int((range_max - range_min) / 1000000)
    keyspace_per_server = int((range_max - range_min) / servers)
    # data_per_file = min(keyspace_per_server, 1000000)
    data_per_file = keyspace_per_server
    # num_files = max(servers, files_per_1m)
    num_files = servers
    bookmark = range_min
    processes = []
    for i in range(0, num_files):
        fname = append_server_num_to_filename(filename, i)

        if i == num_files - 1:
            # last file to be written
            cmd = "python3 {0} --location_of_file {1} --range_max {2} " \
                  "--range_min {3}"\
                .format(WRITE_KEYS_EXE, fname, range_max + 1, bookmark)
            process = subprocess.Popen(shlex.split(cmd))
            processes.append(process)
        else:
            cmd = "python3 {0} --location_of_file {1} --range_max {2} " \
                  "--range_min {3}"\
                .format(WRITE_KEYS_EXE, fname, bookmark + data_per_file, bookmark)
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

        import_cmd += '\\\"nodelocal://1/{0}\\\",'.format(csv)

    import_cmd += ');" | {0} sql --insecure --database=kv'.format(EXE)
    system_utils.call_remote(server, import_cmd)


def main():
    filename = "/home/jennifer/thermopylae_tests_scripts/scratch/populate.csv"
    range_max = 10000000
    tic = time.perf_counter()
    populate(filename, range_max, range_min=0, servers=1)
    toc = time.perf_counter()
    print(f"elapsed {toc - tic:0.4f} seconds")

    return 0


if __name__ == "__main__":
    sys.exit(main())
