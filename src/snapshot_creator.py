import argparse
import sys
import time

import populate_crdb_data
import run_single_data_point
import system_utils
from constants import EXE


def initialize_crdb(config):
    server_nodes = config["warm_nodes"]
    commit_hash = config["commit_hash"]
    should_restore_data = True
    run_single_data_point.cleanup_previous_experiments(should_restore_data, server_nodes, [], None)
    run_single_data_point.build_cockroachdb_commit(server_nodes, commit_hash)
    run_single_data_point.start_cluster(server_nodes, nodelocal_dir="/mydata")

    server_node = server_nodes[0]
    run_single_data_point.set_cluster_settings([server_node],
                                               enable_crdb_replication=True)

    server_urls = ["postgresql://root@{0}:26257?sslmode=disable".format(n["ip"])
                   for n in server_nodes]

    if config["name"] == "kv":
        init_cmd = "{0} workload init kv {1}".format(EXE, server_urls[0])
        system_utils.call_remote(server_node["ip"], init_cmd)
    elif config["name"] == "tpcc":
        warehouses = config["warehouses"]
        init_cmd = "{0} workload fixtures import tpcc --warehouses {1} " \
                   "postgres://root@{2}:26257?sslmode=disable" \
            .format(EXE, warehouses, server_node["ip"])
        system_utils.call_remote(server_node["ip"], init_cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start", type=int, required=True,
        help="which of the import files to start importing from"
    )
    parser.add_argument(
        "--last", type=int, required=True,
        help="which of the import files to end at"
    )
    parser.add_argument(
        "--last_snapshot", type=str, required=True,
        help="name of last snapshot, type None for initial"
    )
    parser.add_argument(
        "--num_nodes", type=int, required=True,
        help="number of nodes that we're using"
    )
    parser.add_argument(
        "--exclude_nodes", nargs="*", default=[],
        help="nodes to exclude"
    )

    args = parser.parse_args()

    warm_nodes = []
    for node in range(args.num_nodes):
        if node not in [int(excluded) for excluded in args.exclude_nodes]:
            warm_nodes.append(node)

    print("using warm_nodes", warm_nodes)

    config = {
        "warm_nodes": [
            {
                "ip": "192.168.1.{}".format(i),
                "store": "/data",
                "region": "singapore",
            }
            for i in [j for j in range(1, 11)] + [j for j in range(15, 40)]],
        "commit_hash": "new-cloudlab",
        "name": "kv"
    }

    if args.start % 10 != 0:
        print("args.start must start at multiple of 10")
        return -1
    elif args.last % 10 != 0:
        print("args.last must start at multiple of 10")
        return -1
    elif (args.last - args.start) % 50 != 0:
        print("args.last - args.start must be a multiple of 50")
        return -1

    last_snapshot = args.last_snapshot
    server_nodes = config["warm_nodes"]
    a_server_node = server_nodes[0]
    data_files = ["populate1B._{0}.csv.gz".format(i) for i in
                  range(args.start, args.last)]

    for i in range(args.start, args.last, 50):

        # somehow restore the cluster here
        initialize_crdb(config)

        if last_snapshot != "None":
            tic = time.perf_counter()
            run_single_data_point.restore_rows(
                a_server_node["ip"], last_snapshot
            )
            toc = time.perf_counter()
            print(f"elapsed {toc - tic:0.4f} seconds", a_server_node["ip"],
                  last_snapshot)

        # import in batches of 10
        first_file = i
        last_file = first_file + 50
        for j in range(first_file + 10, last_file + 1, 10):
            tic = time.perf_counter()
            idx = j - args.start
            print("j", j, "first_file", first_file, "args.start", args.start,
                  "j-first_file", j - first_file, "j-args.start",
                  j - args.start)
            print(a_server_node["ip"], data_files[idx - 10:idx])
            populate_crdb_data.import_into_crdb(
                a_server_node["ip"], data_files[idx - 10:idx]
            )
            toc = time.perf_counter()
            print(f"elapsed {toc - tic:0.4f} seconds, imported", j - 10, j)

        # snapshot the database
        cur_snapshot = "snapshots/" + str(last_file) + "M"
        populate_crdb_data.snapshot(a_server_node["ip"], cur_snapshot, "kv")
        last_snapshot = cur_snapshot
        print(cur_snapshot)

    return 0


if __name__ == "__main__":
    sys.exit(main())
