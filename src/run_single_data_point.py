import logging
import math
import os
import shlex
import subprocess
import sys

import enum
import threading

import grpc
import psycopg2

import cicada_server
import constants
from constants import EXE
import csv_utils
import gather
import populate_crdb_data
import system_utils
import time

import smdbrpc_pb2
import smdbrpc_pb2_grpc

PREPROMOTION_EXE = os.path.join(
    "hotshard_gateway_client", "manual_promotion.go"
)
SNAPSHOT_THRESHOLD = 100000000

M = 1000000
LARGEST_SNAPSHOT = 400  # 400M keys


class RunMode(enum.Enum):
    WARMUP_ONLY = 1
    TRIAL_RUN_ONLY = 2
    WARMUP_AND_TRIAL_RUN = 3


def set_cluster_settings_on_single_node(node, enable_crdb_replication):
    cmd = ('echo "'
           # 'set cluster setting kv.range_merge.queue_enabled = false;'
           # 'set cluster setting kv.range_split.by_load_enabled = false;'
           'set cluster setting kv.raft_log.disable_synchronization_unsafe = '
           'true;')

    if not enable_crdb_replication:
        cmd += 'alter range default configure zone using num_replicas = 1;'

    cmd += ('" | {0} sql --insecure '
            '--url="postgresql://root@{1}?sslmode=disable"').format(EXE,
                                                                    node["ip"])
    system_utils.call_remote(node["ip"], cmd)


def build_cockroachdb_commit_on_single_node(node, commit_hash):
    cmd = ("ssh {0} 'export GOPATH={3}/go "
           "&& set -x && cd {1} && git fetch origin {2} && git stash && git "
           "checkout {2} && git pull origin {2} && "
           "git submodule "
           "update --init "
           "&& (export PATH=$PATH:/usr/local/go/bin && echo $PATH && make "
           "build ||"
           " (make clean && make build)) && set +x'").format(
        node["ip"], constants.COCKROACHDB_DIR, commit_hash, constants.ROOT
    )

    return subprocess.Popen(shlex.split(cmd))


def build_cockroachdb_commit(nodes, commit_hash):
    processes = [build_cockroachdb_commit_on_single_node(node, commit_hash) for
                 node in nodes]
    for process in processes:
        process.wait()


def start_cockroach_node(node, nodelocal_dir, other_urls=[]):
    ip = node["ip"]
    store = node["store"]
    region = node["region"]

    if len(other_urls) > 1:
        cmd = ("{0} start --insecure "
               "--advertise-addr={1} "
               "--store={2} "
               "--locality=region={3} "
               "--cache=.5 "
               "--max-sql-memory=.25 "
               "--log-file-verbosity=2 "
               "--join={4} "
               "--external-io-dir={5} "
               "--background").format(
            EXE, ip, store, region, ",".join(n["ip"] for n in other_urls),
            nodelocal_dir
        )
    else:
        cmd = ("{0} start-single-node --insecure "
               "--advertise-addr={1} "
               "--store={2} "
               "--locality=region={3} "
               "--cache=.25 "
               "--max-sql-memory=.25 "
               "--log-file-verbosity=2 "
               "--http-addr=localhost:8080 "
               "--external-io-dir={4} "
               "--background").format(EXE, ip, store, region, nodelocal_dir)

    cmd = "ssh -tt {0} '{1}' && stty sane".format(ip, cmd)
    print(cmd)
    return subprocess.Popen(cmd, shell=True)


def start_cluster(nodes, nodelocal_dir):
    # first = nodes[0]
    # start_cockroach_node(first).wait()

    processes = []
    for i in range(len(nodes)):
        # for node in nodes:
        # start_cockroach_node(node, join=first["ip"]).wait()
        # set_cluster_settings_on_single_node(first)
        node = nodes[i]

        processes.append(
            start_cockroach_node(
                node, nodelocal_dir, other_urls=nodes
            )
        )

    for process in processes:
        process.wait()

    if len(nodes) > 1:
        system_utils.call(
            "/root/go/src/github.com/cockroachdb/cockroach/cockroach init "
            "--insecure "
            "--host={0}".format(nodes[0]["ip"])
        )


def set_cluster_settings(nodes, enable_crdb_replication):
    for node in nodes:
        set_cluster_settings_on_single_node(node, enable_crdb_replication)


def setup_hotnode(
        node, commit_branch, concurrency, num_rows_in_db,
        write_log=True, log_dir="/root/cicada/build/log",
        enable_replication=False, tail_nodes=[], log_threshold=0,
        replay_interval=0, test_mode=False
):
    """ Kills node (if running) and (re-)starts it.

    Args:
        node (dict of Node object)
        commit_branch (str): commit of hotnode
        concurrency (int): concurrency with which to start the hotnode
        num_rows_in_db (int): deprecated
        write_log (bool): whether to write Cicada's failures to a log
        log_dir (str): directory of cicada's log, if written to
        enable_replication (bool): whether to enable chain rep on Cicada
        tail_nodes (list[Node]): Cicada's tail nodes for chain rep
        log_threshold (int): how many transactions should Cicada buffer during
            replication
        replay_interval (int): deprecated
        test_mode (bool): if true, tail nodes do not replay transactions

    Returns:
        None.
    """
    cicada_server.kill(node)
    for tail_node in tail_nodes:
        cicada_server.kill(tail_node)

    threads = []
    for cr_node in tail_nodes + [node]:
        t = threading.Thread(target=cicada_server.build_server,
                             args=(cr_node, commit_branch))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if enable_replication:
        cicada_server.run_chain_rep(node, tail_nodes, concurrency,
                                    log_threshold, replay_interval, test_mode,
                                    write_log=write_log, log_dir=log_dir)
    else:
        cicada_server.run_server(node, concurrency, num_rows_in_db,
                                 write_log=write_log, log_dir=log_dir,
                                 enable_replication=False)


def kill_hotnode(node):
    cicada_server.kill(node)


def disable_cores(nodes, cores):
    modify_cores(nodes, cores, is_enable_cores=False)


def enable_cores(nodes, cores):
    modify_cores(nodes, cores, is_enable_cores=True)


def modify_cores(nodes, cores, is_enable_cores=False):
    processes = []
    for node in nodes:
        for i in range(1, cores + 1):
            processes.append(
                system_utils.modify_core(node["ip"], i, is_enable_cores)
            )

    for p in processes:
        p.wait()


def kill_cockroachdb_node(node):
    ip = node["ip"]

    if "store" in node:
        store = node["store"]
    else:
        store = None

    cmd = ("PID=$(! pgrep cockroach) "
           "|| (sudo pkill -9 cockroach; while ps -p $PID;do sleep 1;done;)")

    if store:
        cmd = "({0}) && {1}".format(
            cmd, "sudo rm -rf {0}".format(os.path.join(store, "*"))
        )

    cmd = "ssh {0} '{1}'".format(ip, cmd)
    print(cmd)
    return subprocess.Popen(shlex.split(cmd))


def prepromote_keys(
        hot_node, hot_node_port, server_nodes, server_nodes_port, key_min,
        key_max,
        keyspace, hash_randomize_keyspace, enable_fixed_sized_encoding,
        batch=5000
):
    cicadaAddr = ":".join([hot_node["ip"], str(hot_node_port)])
    # cicadaAddr = "node-11:50051"
    crdbAddrs = ",".join(
        [":".join(
            [server_node["ip"], str(server_nodes_port)]
        ) for server_node in server_nodes]
    )

    update_smdbrpc_repo = "cd /root/smdbrpc && git stash && git pull origin " \
                          "demotehotkeys && " \
                          "/root/smdbrpc/generate_new_protos.sh"

    cmd = "cd /root/smdbrpc/go; /usr/local/go/bin/go run {0} --batch={1} " \
          "--cicadaAddr={2} " \
          "--crdbAddrs={3} " \
          "--keyMin={4} " \
          "--keyMax={5} " \
          "--keyspace={6} " \
          "--hash_randomize_keyspace={7} " \
          "--enable_fixed_sized_encoding={8} ".format(
        PREPROMOTION_EXE, batch, cicadaAddr, crdbAddrs, key_min, key_max,
        keyspace, hash_randomize_keyspace, enable_fixed_sized_encoding
    )
    with open("/root/hey", "w") as f:
        system_utils.call(update_smdbrpc_repo, f)
        system_utils.call(cmd, f)

    time.sleep(5)


def cleanup_previous_experiments(server_nodes, client_nodes, hot_node):
    # kill all client nodes
    client_processes = [kill_cockroachdb_node(node) for node in client_nodes]
    for cp in client_processes:
        cp.wait()

    # kill at server nodes
    server_processes = [kill_cockroachdb_node(node) for node in server_nodes]
    for sp in server_processes:
        sp.wait()

    # kill the hot node
    if hot_node:
        kill_hotnode(hot_node)
        enable_cores([hot_node], 15)

    # re-enable ALL cores again, regardless of whether they were previously
    # disabled
    enable_cores(server_nodes, 15)


def restore_rows(server_node, snapshot_name):
    drop_table = "DROP TABLE kv;"
    restore_table = "RESTORE kv.kv FROM \\\"nodelocal://1/{0}\\\";".format(
        snapshot_name
    )

    drop_table_crdb_cmd = 'echo "{0}" | {1} sql --insecure --database ' \
                          'kv'.format(drop_table, EXE)
    restore_crdb_cmd = 'echo "{0}" | {1} sql --insecure --database ' \
                       'kv'.format(restore_table, EXE)

    system_utils.call_remote(server_node, drop_table_crdb_cmd)
    system_utils.call_remote(server_node, restore_crdb_cmd)


def run_kv_workload(
        client_nodes, server_nodes, concurrency, keyspace, warm_up_duration,
        duration, read_percent, n_keys_per_statement, skew, log_dir,
        prepromote_min,
        prepromote_max, hot_node, hot_node_port, crdb_grpc_port, nodelocal_dir,
        discrete_warmup_and_trial, enable_crdb_replication, keyspace_min=0,
        mode=RunMode.WARMUP_AND_TRIAL_RUN, hash_randomize_keyspace=True,
        enable_fixed_sized_encoding=True):
    server_urls = ["postgresql://root@{0}:26257?sslmode=disable".format(n["ip"])
                   for n in server_nodes]

    # warmup and trial run commands are the same
    args = ["--concurrency {}".format(int(concurrency)),
            "--read-percent={}".format(read_percent),
            "--batch={}".format(n_keys_per_statement),
            "--zipfian --s={}".format(skew), "--keyspace={}".format(keyspace),
            "--hash_randomize_keyspace={}".format(hash_randomize_keyspace),
            "--enable_fixed_sized_encoding={}".format(
                enable_fixed_sized_encoding
            ), "--ramp={}s".format(
            0 if discrete_warmup_and_trial else warm_up_duration)]
    # cmd = "{0} workload run kv {1} {2} --useOriginal=False".format(EXE,
    # " ".join(server_urls), " ".join(args))

    # initialize the workload from driver node
    # for url in server_urls:
    init_cmd = "{0} workload init kv {1}".format(EXE, server_urls[0])
    # init_cmd = "{0} workload init kv {1}".format(EXE, url)
    driver_node = client_nodes[0]
    system_utils.call_remote(driver_node["ip"], init_cmd)

    # set database settings
    a_server_node = server_nodes[0]
    if not enable_crdb_replication:
        print("No CRDB replication")
        settings_cmd = 'echo "alter range default configure zone using ' \
                       'num_replicas = 1;" | ' \
                       '{0} sql --insecure --database=kv ' \
                       '--url="postgresql://root@{1}?sslmode=disable"'.format(
            EXE, a_server_node["ip"]
        )
        system_utils.call_remote(driver_node["ip"], settings_cmd)

    # prepopulate data the old way
    # if keyspace - keyspace_min == populate_crdb_data.MAX_DATA_ROWS_PER_FILE-1:
    #    restore_rows(a_server_node["ip"], "data/1M")

    assert (keyspace_min == 0)
    if keyspace - keyspace_min < populate_crdb_data.MAX_DATA_ROWS_PER_FILE:
        data_csv_leaf = "init_data.csv.gz"
        data_csv = os.path.join(nodelocal_dir, "data", data_csv_leaf)
        populate_crdb_data.write_keyspace_to_file(
            data_csv, keyspace + 1, range_min=keyspace_min, payload_size=512,
            enable_fixed_sized_encoding=enable_fixed_sized_encoding
        )
        nfs_location = "data/{0}".format(data_csv_leaf)
        import_cmd = 'echo "IMPORT INTO kv (k, v) CSV DATA(' \
                     '\\\"nodelocal://1/{1}\\\");" | ' \
                     "{0} sql --insecure --database=kv".format(
            EXE, nfs_location
        )
        system_utils.call_remote(a_server_node["ip"], import_cmd)

    elif keyspace < (LARGEST_SNAPSHOT + 50) * M:
        predecessor_snapshot = 0
        successor_snapshot = 50
        while keyspace > successor_snapshot * M and successor_snapshot <= LARGEST_SNAPSHOT:
            predecessor_snapshot = successor_snapshot
            successor_snapshot += 50

        # restore the nearest snapshot for speed
        if predecessor_snapshot > 0:
            tic = time.perf_counter()
            restore_rows(a_server_node["ip"],
                         "snapshots/{0}M".format(predecessor_snapshot))
            toc = time.perf_counter()
            print(f"elapsed {toc - tic:0.4f} seconds, restored snapshot")

        # import the remaining rows
        if keyspace > predecessor_snapshot * M:
            num_files = math.ceil(
                keyspace / populate_crdb_data.MAX_DATA_ROWS_PER_FILE
            )
            data_files = ["populate1B._{0}.csv.gz".format(i) for i in
                          range(num_files + 1)]
            print("number of files to import:", num_files - predecessor_snapshot)

            if num_files >= 10:
                for i in range(predecessor_snapshot + 10, num_files + 1, 10):
                    tic = time.perf_counter()
                    populate_crdb_data.import_into_crdb(
                        a_server_node["ip"], data_files[i - 10: i]
                    )
                    toc = time.perf_counter()
                    print(f"elapsed {toc - tic:0.4f} seconds, imported", i - 10,
                          i)

            remaining_files = num_files % 10
            if remaining_files > 0:
                tic = time.perf_counter()
                populate_crdb_data.import_into_crdb(
                    a_server_node["ip"], data_files[-remaining_files:]
                )
                toc = time.perf_counter()
                print(
                    f"elapsed {toc - tic:0.4f} seconds, imported",
                    num_files - remaining_files, num_files
                )
    else:
        print("keyspace larger than", LARGEST_SNAPSHOT + 50, "unsupported")
        sys.exit(-1)
    #
    # if keyspace - keyspace_min < populate_crdb_data.MAX_DATA_ROWS_PER_FILE:
    #     data_csv_leaf = "init_data.csv.gz"
    #     data_csv = os.path.join(nodelocal_dir, "data", data_csv_leaf)
    #     populate_crdb_data.write_keyspace_to_file(
    #         data_csv, keyspace + 1, range_min=keyspace_min, payload_size=512,
    #         enable_fixed_sized_encoding=enable_fixed_sized_encoding
    #     )
    #     nfs_location = "data/{0}".format(data_csv_leaf)
    #     # upload_cmd = "{0} nodelocal upload {1} {2} --host={3}
    #     # --insecure".format(
    #     #     EXE, data_csv, nfs_location, a_server_node["ip"])
    #     # system_utils.call(upload_cmd)
    #     import_cmd = 'echo "IMPORT INTO kv (k, v) CSV DATA(' \
    #                  '\\\"nodelocal://1/{1}\\\");" | ' \
    #                  "{0} sql --insecure --database=kv".format(
    #         EXE, nfs_location
    #     )
    #     system_utils.call_remote(a_server_node["ip"], import_cmd)
    #
    # elif keyspace < SNAPSHOT_THRESHOLD:
    #     if enable_fixed_sized_encoding is False:
    #         print(
    #             "don't have preset files for "
    #             "enable_fixed_sized_encoding=false"
    #         )
    #         sys.exit(-1)
    #
    #     # prepopulate data
    #     num_files = math.ceil(
    #         keyspace / populate_crdb_data.MAX_DATA_ROWS_PER_FILE
    #     )
    #     data_files = ["populate1B._{0}.csv.gz".format(i) for i in
    #                   range(num_files + 1)]
    #     print("number of files to import:", num_files)
    #
    #     if num_files >= 10:
    #         for i in range(10, num_files + 1, 10):
    #             tic = time.perf_counter()
    #             populate_crdb_data.import_into_crdb(
    #                 a_server_node["ip"], data_files[i - 10: i]
    #             )
    #             toc = time.perf_counter()
    #             print(f"elapsed {toc - tic:0.4f} seconds, imported", i - 10, i)
    #
    #     remaining_files = num_files % 10
    #     if remaining_files > 0:
    #         tic = time.perf_counter()
    #         populate_crdb_data.import_into_crdb(
    #             a_server_node["ip"], data_files[-remaining_files:]
    #         )
    #         toc = time.perf_counter()
    #         print(
    #             f"elapsed {toc - tic:0.4f} seconds, imported",
    #             num_files - remaining_files, num_files
    #         )
    #
    # elif keyspace == SNAPSHOT_THRESHOLD:
    #     if enable_fixed_sized_encoding is False:
    #         print(
    #             "don't have preset population files for "
    #             "enable_fixed_sized_encoding=false"
    #         )
    #         sys.exit(-1)
    #
    #     restore_rows(a_server_node["ip"], "snapshots/100M")
    #
    # else:
    #     print("keyspace larger than", SNAPSHOT_THRESHOLD, "unsupported")
    #     sys.exit(-1)

    # prepromote keys, if necessary
    if hot_node and prepromote_max - prepromote_min > 0:
        time.sleep(5)
        prepromote_keys(
            hot_node, hot_node_port, server_nodes, crdb_grpc_port,
            prepromote_min, prepromote_max, keyspace, hash_randomize_keyspace,
            enable_fixed_sized_encoding
        )

    if (
            mode == RunMode.WARMUP_ONLY or mode == RunMode.WARMUP_AND_TRIAL_RUN) \
            and discrete_warmup_and_trial:

        # run warmup
        # warmup_cmd = cmd + " --duration={}s".format(warm_up_duration)
        warmup_processes = []
        for i in range(len(client_nodes)):
            node = client_nodes[i]
            cmd = "{0} workload run kv {1} {2} --useOriginal=False".format(
                EXE, server_urls[i % len(server_nodes)], " ".join(args)
            )
            warmup_cmd = cmd + " --duration={}s".format(warm_up_duration)
            # for node in client_nodes:
            individual_node_cmd = "sudo ssh {0} '{1}'".format(
                node["ip"], warmup_cmd
            )
            print(individual_node_cmd)
            warmup_processes.append(
                subprocess.Popen(shlex.split(individual_node_cmd))
            )

        for wp in warmup_processes:
            wp.wait()

    if mode == RunMode.TRIAL_RUN_ONLY or mode == RunMode.WARMUP_AND_TRIAL_RUN:

        # making the logs directory, if it doesn't already exist
        log_fpath = os.path.join(log_dir, "logs")
        if not os.path.exists(log_fpath):
            os.makedirs(log_fpath)

        # run trial
        # trial_cmd = cmd + " --duration={}s".format(duration)
        trial_processes = []
        bench_log_files = []
        for i in range(len(client_nodes)):
            node = client_nodes[i]
            cmd = "{0} workload run kv {1} {2} --useOriginal=False".format(
                EXE, server_urls[i % len(server_nodes)], " ".join(args)
            )
            trial_cmd = cmd + " --duration={}s".format(duration)
            # for node in client_nodes:
            # logging output for each node
            individual_log_fpath = os.path.join(
                log_fpath, "bench_{}.txt".format(node["ip"])
            )
            bench_log_files.append(individual_log_fpath)

            # run command
            individual_node_cmd = "sudo ssh {0} '{1}'".format(
                node["ip"], trial_cmd
            )
            print(individual_node_cmd)
            with open(individual_log_fpath, "w") as f:
                trial_processes.append(
                    subprocess.Popen(shlex.split(individual_node_cmd), stdout=f)
                )

        for tp in trial_processes:
            tp.wait()

        return bench_log_files


def init_tpcc(server_node, driver_node, init_with_fixture, warehouses):
    start = time.time()
    snapshot_name = "snapshot/tpcc{}".format(warehouses)
    populate_crdb_data.restore(server_node["ip"], snapshot_name, "tpcc")
    # init_cmd = "{0} workload init tpcc " \
    #            "--warehouses={1} " \
    #            "{2}".format(EXE, warehouses, server_node)
    # if init_with_fixture:
    #     init_cmd = "{0} workload fixtures import tpcc " \
    #                "--warehouses {1} " \
    #                "{2}".format(EXE, warehouses, server_node)
    # system_utils.call_remote(driver_node["ip"], init_cmd)

    end = time.time()
    print(end - start)


def query_table_num_from_names(names, host="localhost"):
    db_url = "postgresql://root@{}:26257?sslmode=disable".format(host)

    conn = psycopg2.connect(db_url, database="tpcc")

    mapping = {}
    with conn.cursor() as cur:
        for table_name in names:

            query = "SELECT '\"{}\"'::regclass::oid;".format(table_name)
            print(query)

            cur.execute(query)
            logging.debug("status message %s", cur.statusmessage)

            rows = cur.fetchall()
            if len(rows) > 1:
                print("fetchall should only have one row")
                sys.exit(-1)

            mapping[table_name] = rows[0][0]

        conn.commit()

    return mapping


def promote_keys_in_tpcc(crdb_node, num_warehouses):
    # populate CRDB table numbers
    tableNames = ["warehouse", "stock", "item", "history", "new_order",
                  "order_line", "district", "customer", "order"]
    mapping = query_table_num_from_names(tableNames, crdb_node["ip"])

    req = smdbrpc_pb2.PopulateCRDBTableNumMappingReq(
        tableNumMappings=[], )
    for tableName, tableNum in mapping.items():
        req.tableNumMappings.append(
            smdbrpc_pb2.TableNumMapping(
                tableNum=tableNum, tableName=tableName, )
        )

    # populate CRDB table numbers
    channel = grpc.insecure_channel("{}:50055".format(crdb_node["ip"]))
    stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

    _ = stub.PopulateCRDBTableNumMapping(req)

    # promote warehouse
    # this call broadcasts to all CRDB nodes
    promotion_req = smdbrpc_pb2.TestPromoteTPCCTablesReq(
        num_warehouses=num_warehouses, warehouse=True, district=False,
        customer=False,
        order=False, neworder=False, orderline=False, stock=False,
        item=False, history=False
    )
    _ = stub.TestPromoteTPCCTables(promotion_req)


def run_tpcc_workload(
        client_nodes, server_nodes, concurrency, log_dir, warm_up_duration,
        duration, mix, discrete_warmup_and_trial, init_with_fixture, warehouses,
        wait, promote_keys, enable_crdb_replication,
        mode=RunMode.WARMUP_AND_TRIAL_RUN
):
    server_urls = ["postgresql://root@{0}:26257?sslmode=disable".format(n["ip"])
                   for n in server_nodes]

    # warmup and trial run commands are the same
    args = ["--concurrency {}".format(int(concurrency)),
            "--mix='{}'".format(mix), "--ramp={}s".format(
            0 if discrete_warmup_and_trial else warm_up_duration),
            "--wait={}".format(1 if wait else 0)]

    # init tpcc
    a_server_node = server_nodes[0]
    driver_node = client_nodes[0]
    init_tpcc(a_server_node, driver_node, init_with_fixture, warehouses)

    # set database settings
    if not enable_crdb_replication:
        settings_cmd = 'echo "alter range default configure zone using ' \
                       'num_replicas = 1;" | ' \
                       '{0} sql --insecure ' \
                       '--url="postgresql://root@{1}?sslmode=disable"'.format(
            EXE, a_server_node["ip"]
        )
        system_utils.call_remote(driver_node["ip"], settings_cmd)

    # promote keys
    if promote_keys:
        promote_keys_in_tpcc(a_server_node, warehouses)

    if (mode == RunMode.WARMUP_ONLY or mode == RunMode.WARMUP_AND_TRIAL_RUN) \
            and discrete_warmup_and_trial:

        # run warmup
        # warmup_cmd = cmd + " --duration={}s".format(warm_up_duration)
        warmup_processes = []
        for i in range(len(client_nodes)):
            node = client_nodes[i]
            cmd = "{0} workload run tpcc {1} {2} ".format(
                EXE, server_urls[i % len(server_nodes)], " ".join(args)
            )
            warmup_cmd = cmd + " --duration={}s".format(warm_up_duration)
            # for node in client_nodes:
            individual_node_cmd = "sudo ssh {0} '{1}'".format(
                node["ip"], warmup_cmd
            )
            print(individual_node_cmd)
            warmup_processes.append(
                subprocess.Popen(shlex.split(individual_node_cmd))
            )

        for wp in warmup_processes:
            wp.wait()

    if mode == RunMode.TRIAL_RUN_ONLY or mode == RunMode.WARMUP_AND_TRIAL_RUN:

        # making the logs directory, if it doesn't already exist
        log_fpath = os.path.join(log_dir, "logs")
        if not os.path.exists(log_fpath):
            os.makedirs(log_fpath)

        # run trial
        # trial_cmd = cmd + " --duration={}s".format(duration)
        trial_processes = []
        bench_log_files = []
        for i in range(len(client_nodes)):
            node = client_nodes[i]
            cmd = "{0} workload run tpcc {1} {2} ".format(
                EXE, server_urls[i % len(server_nodes)], " ".join(args)
            )
            trial_cmd = cmd + " --duration={}s".format(duration)
            # for node in client_nodes:
            # logging output for each node
            individual_log_fpath = os.path.join(
                log_fpath, "bench_{}.txt".format(node["ip"])
            )
            bench_log_files.append(individual_log_fpath)

            # run command
            individual_node_cmd = "sudo ssh {0} '{1}'".format(
                node["ip"], trial_cmd
            )
            print(individual_node_cmd)
            with open(individual_log_fpath, "w") as f:
                trial_processes.append(
                    subprocess.Popen(shlex.split(individual_node_cmd), stdout=f)
                )

        for tp in trial_processes:
            tp.wait()

        return bench_log_files


def run(config, log_dir, write_cicada_log=True):
    server_nodes = config["warm_nodes"]
    client_nodes = config["workload_nodes"]
    commit_hash = config["cockroach_commit"]
    discrete_warmup_and_trial = config["discrete_warmup_and_trial"]
    hot_node = config["hot_node"] if "hot_node" in config else None
    hot_node_port = config[
        "hot_node_port"] if "hot_node_port" in config else None
    prepromote_min = config[
        "prepromote_min"] if "prepromote_min" in config else 0
    prepromote_max = config[
        "prepromote_max"] if "prepromote_max" in config else 0
    crdb_grpc_port = config[
        "crdb_grpc_port"] if "crdb_grpc_port" in config else None
    hash_randomize_keyspace = config[
        "hash_randomize_keyspace"] if "hash_randomize_keyspace" in config else None
    enable_fixed_sized_encoding = config[
        "enable_fixed_sized_encoding"] if "enable_fixed_sized_encoding" in config else None
    keyspace = config["keyspace"] if "keyspace" in config else 0
    enable_crdb_replication = config["enable_crdb_replication"]

    # hotkeys = config["hotkeys"]

    # clear any remaining experiments
    cleanup_previous_experiments(server_nodes, client_nodes, hot_node)

    # disable cores, if need be
    cores_to_disable = config["disable_cores"]
    if cores_to_disable > 0:
        disable_cores(server_nodes, cores_to_disable)
        if hot_node:
            disable_cores([hot_node], cores_to_disable)

    # start hot node
    print("tail nodes", config["tail_nodes"], type(config["tail_nodes"]))
    min_key = 0
    if hot_node:
        num_rows_in_cicada = prepromote_max - prepromote_min
        if config["enable_replication"]:
            setup_hotnode(hot_node, config["hot_node_commit_branch"],
                          config["hot_node_concurrency"], num_rows_in_cicada,
                          write_log=write_cicada_log, log_dir=log_dir,
                          enable_replication=True,
                          tail_nodes=config["tail_nodes"],
                          log_threshold=config["log_threshold"],
                          replay_interval=config["replay_interval"],
                          test_mode=config["test_mode"])
        else:
            setup_hotnode(
                hot_node, config["hot_node_commit_branch"],
                config["hot_node_concurrency"], num_rows_in_cicada,
                write_log=write_cicada_log, log_dir=log_dir,
                enable_replication=False
            )

    # build and start crdb cluster
    build_cockroachdb_commit(server_nodes + client_nodes, commit_hash)
    nodelocal_dir = "/mydata"
    if config["name"] == "kv" and keyspace - min_key < populate_crdb_data.MAX_DATA_ROWS_PER_FILE:
        nodelocal_dir = "/proj/cops-PG0/workspaces/jl87/"
    start_cluster(server_nodes, nodelocal_dir)
    set_cluster_settings_on_single_node(server_nodes[0],
                                        enable_crdb_replication)

    # build and start client nodes
    results_fpath = ""
    warm_up_duration = config["warm_up_duration"]
    duration = config["duration"]
    concurrency = config["concurrency"]
    if config["name"] == "kv":

        read_percent = config["read_percent"]
        n_keys_per_statement = config["n_keys_per_statement"]
        skew = config["skews"]
        bench_log_files = run_kv_workload(
            client_nodes, server_nodes, concurrency, keyspace, warm_up_duration,
            duration, read_percent, n_keys_per_statement, skew, log_dir,
            prepromote_min, prepromote_max, hot_node, hot_node_port,
            crdb_grpc_port, nodelocal_dir, discrete_warmup_and_trial,
            enable_crdb_replication, keyspace_min=min_key,
            hash_randomize_keyspace=hash_randomize_keyspace,
            enable_fixed_sized_encoding=enable_fixed_sized_encoding,
        )

        # create csv file of gathered data
        data = {"concurrency": config["concurrency"]}
        more_data, has_data = gather.gather_data_from_raw_kv_logs(
            bench_log_files
        )
        if not has_data:
            raise RuntimeError(
                "Config {0} has failed to produce any results".format(
                    config[constants.CONFIG_FPATH_KEY]
                )
            )
        data.update(more_data)

        # write out csv file
        results_fpath = os.path.join(log_dir, "results.csv")
        _ = csv_utils.write_out_data([data], results_fpath)

    elif config["name"] == "tpcc":
        warehouses = config["warehouses"]
        mix = config["mix"]
        init_with_fixture = config["init_with_fixture"]
        wait = config["wait"]
        promote_keys = config["promote_keys"]
        bench_log_files = run_tpcc_workload(
            client_nodes, server_nodes, concurrency, log_dir, warm_up_duration,
            duration, mix, discrete_warmup_and_trial, init_with_fixture,
            warehouses, wait, promote_keys, enable_crdb_replication
        )

        # create csv file of gathered data
        data = {"concurrency": config["concurrency"]}
        more_data, has_data = gather.gather_data_from_raw_tpcc_logs(
            bench_log_files
        )

        if not has_data:
            raise RuntimeError(
                "Config {0} has failed to produce any results".format(
                    config[constants.CONFIG_FPATH_KEY]
                )
            )
        data.update(more_data)

        # write out csv file
        results_fpath = os.path.join(log_dir, "results.csv")
        _ = csv_utils.write_out_data([data], results_fpath)

    # re-enable cores
    cores_to_enable = cores_to_disable
    if cores_to_enable > 0:
        enable_cores(server_nodes, cores_to_enable)
        if hot_node:
            enable_cores([hot_node], cores_to_enable)

    return results_fpath


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ini_file")
    parser.add_argument("concurrency", type=int)
    parser.add_argument("--log_dir", type=str, default=constants.SCRATCH_DIR)
    args = parser.parse_args()

    import config_io
    config = config_io.read_config_from_file(args.ini_file)
    config["concurrency"] = args.concurrency
    import datetime
    unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_dir = os.path.join(
        args.log_dir, "run_single_trial_{0}".format(unique_suffix)
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    run(config, log_dir)


if __name__ == "__main__":
    main()
