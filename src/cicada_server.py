import os
import shlex
import subprocess
import time

import async_server
import system_utils

STARTUP_SECS = 20


def build_server(server_node, commit_branch):
    print(type(server_node))
    server_url = server_node["ip"]

    cmd = "cd /root/cicada-engine; " \
          "git fetch origin {0}; " \
          "git stash; " \
          "git checkout {0}; " \
          "git pull origin {0}; ".format(commit_branch)
    print(system_utils.call_remote(server_url, cmd))

    cmd = "cd /root/cicada-engine/build; " \
          "../script/setup.sh 0; " \
          "rm -rf *; " \
          "export PATH=$PATH:/root/.local/bin; " \
          "cmake -DLTO=ON -DDEBUG=OFF ..; " \
          "make -j; " \
          "ln -s /root/cicada-engine/src/mica/test/test_tx.json " \
          "/root/cicada-engine/build; "
    print(system_utils.call_remote(server_url, cmd))


def build_client(client_node, commit_branch):
    async_server.build_client(client_node, commit_branch)


def run_chain_rep(head_node, tail_nodes, concurrency, log_threshold,
                  replay_interval, test_mode, write_log=True,
                  log_dir="/root/cicada/build/log"):
    is_end_node = True
    for i in range(len(tail_nodes), 0, -1):
        backup_node = tail_nodes[i - 1]
        if is_end_node:
            is_end_node = False
            run_end_node(backup_node["ip"], concurrency, backup_node["port"],
                         test_mode,
                         write_log=write_log, log_dir=log_dir)
        else:
            tail_node = tail_nodes[i]
            run_backup(backup_node["ip"], concurrency, backup_node["port"],
                       tail_node["ip"], tail_node["port"], test_mode,
                       write_log=write_log, log_dir=log_dir)

    tail_node = tail_nodes[0]
    run_server(head_node, concurrency, 100000,
               write_log=write_log, log_dir=log_dir, enable_replication=True,
               next_host=tail_node["ip"],
               next_port=tail_node["port"], log_threshold=log_threshold,
               replay_interval=replay_interval)


def run_server(server_node, concurrency, num_rows_in_dbs,
               write_log=True, log_dir="/root/cicada-engine/build/log",
               enable_replication=False, next_host="", next_port=-1,
               log_threshold=0,
               replay_interval=0):
    server_url = server_node["ip"]

    cmd = "/root/cicada-engine/script/setup.sh 32000; " \
          "/root/cicada-engine/build/hotshard_gateway_server {0}".format(
        concurrency)
    if enable_replication:
        cmd += " {0} {1} {2} {3}".format(next_host, next_port, log_threshold,
                                         replay_interval)
    else:
        cmd += " --endNode"

    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    log_fpath = os.path.join(log_dir, "logs")
    if not os.path.exists(log_fpath):
        os.makedirs(log_fpath)

    if write_log:
        cicada_log = os.path.join(log_fpath, "cicada_log.txt")
        with open(cicada_log, "w") as f:
            process = subprocess.Popen(shlex.split(ssh_wrapped_cmd),
                                       stdout=f, stderr=f)
    else:
        process = subprocess.Popen(shlex.split(ssh_wrapped_cmd))

    time.sleep(STARTUP_SECS)

    return process


def run_backup(server_node_host, concurrency, base_port, next_host, next_port,
               test_mode, write_log=True,
               log_dir="/root/cicada-engine/build/logs"):
    server_url = server_node_host

    cmd = "/root/cicada-engine/build/backup {0} {1} {2} {3}".format(concurrency,
                                                                    base_port,
                                                                    next_host,
                                                                    next_port)
    if test_mode:
        cmd += " --testMode"
    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    if write_log:
        log_fpath = os.path.join(log_dir, "logs")
        if not os.path.exists(log_fpath):
            os.makedirs(log_fpath)
        cicada_log = os.path.join(log_fpath, "cicada_log.txt")
        with open(cicada_log, "w") as f:
            process = subprocess.Popen(shlex.split(ssh_wrapped_cmd),
                                       stdout=f, stderr=f)
    else:
        process = subprocess.Popen(shlex.split(ssh_wrapped_cmd))

    time.sleep(STARTUP_SECS)

    return process


def run_end_node(server_node_host, concurrency, base_port, test_mode,
                 write_log=True,
                 log_dir="/root/cicada-engine/build/logs"):
    server_url = server_node_host

    cmd = "/root/cicada-engine/build/backup {0} {1} --endNode".format(
        concurrency, base_port)
    if test_mode:
        cmd += " --testMode"
    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    if write_log:
        log_fpath = os.path.join(log_dir, "logs")
        if not os.path.exists(log_fpath):
            os.makedirs(log_fpath)
        cicada_log = os.path.join(log_fpath, "cicada_log.txt")
        with open(cicada_log, "w") as f:
            process = subprocess.Popen(shlex.split(ssh_wrapped_cmd),
                                       stdout=f, stderr=f)
    else:
        process = subprocess.Popen(shlex.split(ssh_wrapped_cmd))

    time.sleep(STARTUP_SECS)

    return process


def run_clients(client_nodes, server_node, duration, concurrency, batch,
                read_percent,
                location):
    return async_server.run_clients(client_nodes,
                                    server_node,
                                    duration,
                                    concurrency,
                                    batch,
                                    read_percent,
                                    location)


def kill(node):
    async_server.kill(node)


def aggregate_raw_logs(logfiles):
    return async_server.aggregate_raw_logs(logfiles)


def parse_raw_logfiles(input_logfiles, output_csvfile):
    return async_server.parse_raw_logfiles(input_logfiles, output_csvfile)


def graph(datafile, graph_location):
    return async_server.graph(datafile, graph_location)
