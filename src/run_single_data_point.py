import math
import os
import shlex
import subprocess
import sys

import enum

import cicada_server
import constants
from constants import EXE
import csv_utils
import gather
import populate_crdb_data
import system_utils
import time

PREPROMOTION_EXE = os.path.join(
    "hotshard_gateway_client", "manual_promotion.go"
)


class RunMode(enum.Enum):
    WARMUP_ONLY = 1
    TRIAL_RUN_ONLY = 2
    WARMUP_AND_TRIAL_RUN = 3


def set_cluster_settings_on_single_node(node):
    cmd = ('echo "'
           # 'set cluster setting kv.range_merge.queue_enabled = false;'
           # 'set cluster setting kv.range_split.by_load_enabled = false;'
           'set cluster setting kv.raft_log.disable_synchronization_unsafe = '
           'true;'
           'alter range default configure zone using num_replicas = 1;'
           '" | {0} sql --insecure '
           '--url="postgresql://root@{1}?sslmode=disable"').format(
        EXE, node["ip"]
    )
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


def start_cockroach_node(node, other_urls=[]):
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
            "/proj/cops-PG0/workspaces/jl87/"
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
               "--background").format(EXE, ip, store, region)

    cmd = "ssh -tt {0} '{1}' && stty sane".format(ip, cmd)
    print(cmd)
    return subprocess.Popen(cmd, shell=True)


def start_cluster(nodes):
    # first = nodes[0]
    # start_cockroach_node(first).wait()

    processes = []
    for i in range(len(nodes)):
        # for node in nodes:
        # start_cockroach_node(node, join=first["ip"]).wait()
        # set_cluster_settings_on_single_node(first)
        node = nodes[i]

        processes.append(start_cockroach_node(node, other_urls=nodes))

    for process in processes:
        process.wait()

    if len(nodes) > 1:
        system_utils.call(
            "/root/go/src/github.com/cockroachdb/cockroach/cockroach init "
            "--insecure "
            "--host={0}".format(nodes[0]["ip"])
        )


def set_cluster_settings(nodes):
    for node in nodes:
        set_cluster_settings_on_single_node(node)


def setup_hotnode(
    node, commit_branch, concurrency, log_dir, threshold, write_log=True
):
    """ Kills node (if running) and (re-)starts it.

    Args:
        node (dict of Node object)
        commit_branch (str): commit of hotnode
        concurrency (int): concurrency with which to start the hotnode

    Returns:
        None.
    """
    cicada_server.kill(node)
    cicada_server.build_server(node, commit_branch)
    cicada_server.run_server(
        node, concurrency, log_dir, threshold, write_log=write_log
    )


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
    hot_node, hot_node_port, server_nodes, server_nodes_port, key_min, key_max,
    batch=500
):
    # cicadaAddr = ":".join([hot_node["ip"], str(hot_node_port)])
    cicadaAddr = "node-11:50051"
    crdbAddrs = ",".join(
        [":".join(
            [server_node["ip"], str(server_nodes_port)]
        ) for server_node in server_nodes]
    )

    cmd = "cd /root/smdbrpc/go; /usr/local/go/bin/go run {0} --batch {1} " \
          "--cicadaAddr {2} " \
          "--crdbAddrs {3} " \
          "--keyMin {4} --keyMax {5}".format(
        PREPROMOTION_EXE, batch, cicadaAddr, crdbAddrs, key_min, key_max
    )
    with open("/root/hey", "w") as f:
        system_utils.call(cmd, f)


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


def run_kv_workload(
    client_nodes, server_nodes, concurrency, keyspace, warm_up_duration,
    duration, read_percent, n_keys_per_statement, skew, log_dir, keyspace_min=0,
    mode=RunMode.WARMUP_AND_TRIAL_RUN
):
    server_urls = ["postgresql://root@{0}:26257?sslmode=disable".format(n["ip"])
                   for n in server_nodes]

    # warmup and trial run commands are the same
    args = ["--concurrency {}".format(int(concurrency)),
            "--read-percent={}".format(read_percent),
            "--batch={}".format(n_keys_per_statement),
            "--zipfian --s={}".format(skew), "--keyspace={}".format(keyspace)]
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
    settings_cmd = 'echo "alter range default configure zone using ' \
                   'num_replicas = 1;" | ' \
                   '{0} sql --insecure --database=kv ' \
                   '--url="postgresql://root@{1}?sslmode=disable"'.format(
        EXE, a_server_node["ip"]
    )
    system_utils.call_remote(driver_node["ip"], settings_cmd)

    # prepopulate data
    num_files = math.ceil(keyspace / 5000000)
    data_files = ["populate1B._{0}.csv.gz".format(i) for i in range(
        num_files
    )]
    print("number of files to import:", num_files)

    # nodelocal upload
    # tic = time.perf_counter()
    # for file in data_files:
    #    local_file_location = "/proj/cops-PG0/workspaces/jl87/{0}".format(file)
    #    crdb_file_location = file
    #    populate_crdb_data.upload_nodelocal(
    #        local_file_location, crdb_file_location,
    #        a_server_node["ip"] + ":26257")
    # toc = time.perf_counter()
    # print(f"nodelocal upload elapsed {toc - tic:0.4f} seconds")

    for i in range(0, num_files, 10):
        tic = time.perf_counter()
        populate_crdb_data.import_into_crdb(
            a_server_node["ip"], data_files[i - 10: i]
        )
        toc = time.perf_counter()
        print(f"elapsed {toc - tic:0.4f} seconds, imported", i-10, i)

    remaining_files = num_files % 10
    if remaining_files > 0:
        tic = time.perf_counter()
        populate_crdb_data.import_into_crdb(
            a_server_node["ip"], data_files[-remaining_files:]
        )
        toc = time.perf_counter()
        print(f"elapsed {toc - tic:0.4f} seconds, imported",
            num_files-remaining_files, num_files)

    sys.exit(0)

    if mode == RunMode.WARMUP_ONLY or mode == RunMode.WARMUP_AND_TRIAL_RUN:

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


def run(config, log_dir, write_cicada_log=True):
    server_nodes = config["warm_nodes"]
    client_nodes = config["workload_nodes"]
    commit_hash = config["cockroach_commit"]
    hot_node = config["hot_node"] if "hot_node" in config else None
    hot_node_port = config[
        "hot_node_port"] if "hot_node_port" in config else None
    prepromote_min = config[
        "prepromote_min"] if "prepromote_min" in config else None
    prepromote_max = config[
        "prepromote_max"] if "prepromote_max" in config else None
    crdb_grpc_port = config[
        "crdb_grpc_port"] if "crdb_grpc_port" in config else None

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
    min_key = 0
    if hot_node:
        min_key = config["hot_node_threshold"]
        setup_hotnode(
            hot_node, config["hot_node_commit_branch"],
            config["hot_node_concurrency"], log_dir, min_key,
            write_log=write_cicada_log
        )

    # build and start crdb cluster
    build_cockroachdb_commit(server_nodes + client_nodes, commit_hash)
    start_cluster(server_nodes)
    set_cluster_settings_on_single_node(server_nodes[0])

    # prepromote keys, if necessary
    if hot_node_port:
        time.sleep(5)
        prepromote_keys(
            hot_node, hot_node_port, server_nodes, crdb_grpc_port,
            prepromote_min, prepromote_max
        )

    # build and start client nodes
    results_fpath = ""
    if config["name"] == "kv":
        keyspace = config["keyspace"]
        warm_up_duration = config["warm_up_duration"]
        duration = config["duration"]
        read_percent = config["read_percent"]
        n_keys_per_statement = config["n_keys_per_statement"]
        skew = config["skews"]
        concurrency = config["concurrency"]
        bench_log_files = run_kv_workload(
            client_nodes, server_nodes, concurrency, keyspace, warm_up_duration,
            duration, read_percent, n_keys_per_statement, skew, log_dir,
            keyspace_min=min_key
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
