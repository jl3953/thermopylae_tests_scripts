import datetime
import operator
import os

import config_io
import constants
import csv_utils
import gather
import plot_utils
import run_single_data_point

LT_GNUPLOT_EXE = os.path.join(constants.TEST_SRC_PATH, "lt.gp")


def last_adjustments(max_throughput_concurrency):
    return max_throughput_concurrency - 1


def insert_csv_data(data, csv_fpath):
    if len(data) <= 0:
        return None

    existing_rows = csv_utils.read_in_data(csv_fpath)
    all_data = existing_rows + data
    all_data = sorted(all_data, key=lambda i: i["concurrency"])

    _ = csv_utils.write_out_data(all_data, csv_fpath)

    return csv_fpath


def run(config, lt_config, log_dir):
    # create latency throughput dir, if not running recovery
    lt_dir = os.path.join(log_dir, "latency_throughput")
    lt_logs_dir = os.path.join(lt_dir, "logs")
    checkpoint_csv_fpath = os.path.join(lt_dir, "lt.csv")
    if not os.path.exists(lt_logs_dir):
        # not running recovery
        os.makedirs(lt_logs_dir)

    # read lt config file
    start, end = lt_config["concurrency"]
    step_size = lt_config["step_size"]

    # honing in on increasingly smaller ranges
    data = []
    #while step_size > 0:
    print("start", type(start), "end", type(end), "step_size", type(step_size))
    concurrency_list = [i for i in range(8, 128, 8)]
    if config["skews"] == 0.01:
        if "hot_node" in config:
            if config["num_warm_nodes"] == 1:
                concurrency_list = [24, 32, 36, 40, 48, 64, 80, 96, 120]
            elif config["num_warm_nodes"] == 3:
                concurrency_list = [20, 28, 36, 48, 56, 64, 72, 80, 96, 104]
            elif config["num_warm_nodes"] == 7:
                concurrency_list = []
            elif config["num_warm_nodes"] == 15:
                concurrency_list = []
        else:
            if config["num_warm_nodes"] == 1:
                concurrency_list = [8, 16, 24, 32, 48, 56]
            elif config["num_warm_nodes"] == 2:
                concurrency_list = [8, 12, 16, 24, 32, 48, 56]
            elif config["num_warm_nodes"] == 4:
                concurrency_list = [8, 16, 24, 32, 48, 56, 64]
            elif config["num_warm_nodes"] == 8:
                concurrency_list = [8, 16, 32, 48, 64, 80]
            elif config["num_warm_nodes"] == 16:
                concurrency_list = [8, 16, 32, 48, 64, 80, 96]
    elif config["skews"] == 0.99:
        if "hot_node" in config:
            if config["num_warm_nodes"] == 1:
                concurrency_list = [32, 40, 48, 56, 64, 80, 96, 120]
            elif config["num_warm_nodes"] == 3:
                concurrency_list = [40, 48, 56, 60, 64, 68, 72, 80, 96, 120]
            elif config["num_warm_nodes"] == 7:
                concurrency_list = []
            elif config["num_warm_nodes"] == 15:
                concurrency_list = []
        else:
            if config["num_warm_nodes"] == 1:
                concurrency_list = [8, 16, 24, 32, 48, 56, 64]
            elif config["num_warm_nodes"] == 2:
                concurrency_list = [2, 4, 8, 16, 24, 36]
            elif config["num_warm_nodes"] == 4:
                concurrency_list = [2, 4, 6, 8, 10, 12]
            elif config["num_warm_nodes"] == 8:
                concurrency_list = [1, 2, 3, 4, 5, 6]
            elif config["num_warm_nodes"] == 16:
                concurrency_list = [1, 2, 3, 4, 5, 6]
    elif config["skews"] == 1.2:
        if "hot_node" in config: # Thermopylae
            if config["num_warm_nodes"] == 1:
                concurrency_list = [40, 48, 56, 64, 72, 80, 88, 96, 120]
            elif config["num_warm_nodes"] == 3:
                concurrency_list = [40, 48, 56, 64, 72, 80, 88, 96, 120]
            elif config["num_warm_nodes"] == 7:
                concurrency_list = []
            elif config["num_warm_nodes"] == 15:
                concurrency_list = []
        else: # CRDB
            if config["num_warm_nodes"] == 1:
                concurrency_list = [2, 4, 6, 7, 8, 10, 12]
            elif config["num_warm_nodes"] == 2:
                concurrency_list = [2, 4, 6, 8, 10, 12]
            elif config["num_warm_nodes"] == 4:
                concurrency_list = [2, 4, 6, 8, 10, 12]
            elif config["num_warm_nodes"] == 8:
                # one fourth the number of clients
                concurrency_list = [1, 2, 3, 4, 5, 6]
            elif config["num_warm_nodes"] == 16:
                # one fourth the number of clients
                concurrency_list = [1, 2, 3, 4, 5, 6]

    else:
        print("skew or num machines not set")
        raise Exception("skew or num machines not set")
    for concurrency in concurrency_list:
        # run trial for this concurrency
        config["concurrency"] = concurrency

        # make directory for this specific concurrency, unique by timestamp
        specific_logs_dir = os.path.join(lt_logs_dir, "{0}_{1}".format(
            str(concurrency), datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")))

        # run trial
        os.makedirs(specific_logs_dir)
        results_fpath_csv = run_single_data_point.run(config,
            specific_logs_dir, write_cicada_log=False)

        # gather data from this run
        datum = {"concurrency": concurrency}
        more_data = csv_utils.read_in_data(results_fpath_csv)
        datum.update(*more_data)
        data.append(datum)

    # find max throughput and hone in on it
    max_throughput_concurrency = max(data, key=operator.itemgetter("ops/sec(cum)"))["concurrency"]
    concurrency = last_adjustments(max_throughput_concurrency)
    start = int(concurrency - step_size)
    end = int(concurrency + step_size)
    step_size = int(step_size / 2)

    # checkpoint_csv_fpath, and also write out csv values every round of honing in
    insert_csv_data(data, checkpoint_csv_fpath)

    # plot the latency throughput graphs
    plot_utils.gnuplot(LT_GNUPLOT_EXE, checkpoint_csv_fpath,
                       os.path.join(lt_dir, "p50_lt.png"),
                       os.path.join(lt_dir, "p95_lt.png"),
                       os.path.join(lt_dir, "p99_lt.png"))

    return checkpoint_csv_fpath


def find_optimal_concurrency(lt_fpath_csv):
    data = csv_utils.read_in_data(lt_fpath_csv)
    max_throughput_concurrency = max(data, key=operator.itemgetter("ops/sec(cum)"))["concurrency"]
    return int(last_adjustments(max_throughput_concurrency))


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ini_file")
    parser.add_argument("lt_ini_file")
    import constants
    parser.add_argument("--log_dir", type=str, default=constants.SCRATCH_DIR)
    args = parser.parse_args()

    config = config_io.read_config_from_file(args.ini_file)
    lt_config = config_io.read_config_from_file(args.lt_ini_file)

    unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_dir = os.path.join(args.log_dir, "lt_{}".format(unique_suffix))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    run(config, lt_config, log_dir)


if __name__ == "__main__":
    main()
