import csv
import gzip
import sys
import time
import system_utils

from constants import EXE


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
    files_per_1m = int((range_max - range_min) / 1000000)
    keyspace_per_server = int((range_max - range_min) / servers)
    data_per_file = min(keyspace_per_server, 1000000)
    num_files = max(servers, files_per_1m)
    bookmark = range_min
    for i in range(0, num_files):
        fname = append_server_num_to_filename(filename, i)
        if i == num_files - 1:
            # last file to be written
            write_keyspace_to_file(fname, range_max + 1, bookmark)
        else:
            write_keyspace_to_file(
                fname, bookmark + data_per_file,
                bookmark
            )

        bookmark += data_per_file


def upload_nodelocal(
    location_of_file, nfs_location,
    hostport="localhost:26257"
):
    upload_cmd = "{0} nodelocal upload --insecure --host {1} {2} " \
                 "{3}".format(
        EXE, hostport, location_of_file,
        nfs_location
    )
    system_utils.call(upload_cmd)


def import_into_crdb(serverport, nfs_locations):
    import_cmd = 'echo "IMPORT INTO kv (k, v) CSV_DATA('

    for csv in nfs_locations:
        import_cmd += '\\\"nodelocal://1/{0}\\\",'.format(csv)

    import_cmd += ');" | {0} sql --insecure --database=kv'.format(EXE)
    system_utils.call_remote(serverport, import_cmd)


def prepopulate(server_nodes, csvfiles):

    for csvfile in csvfiles:
        upload_nodelocal(csvfile, )


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
