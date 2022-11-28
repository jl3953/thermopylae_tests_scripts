import datetime
import operator
import os

import config_io
import constants
import csv_utils
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


def run(should_restore_data, config, lt_config, log_dir):
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
    restore_data = should_restore_data
    while step_size > 0:
        print("start", type(start), "end", type(end), "step_size",
              type(step_size))
        concurrency_list = [i for i in range(start, end, step_size)]
        if step_size == 1:
            concurrency_list = [1, 2, 4, 8, 16, 32] + concurrency_list
        for concurrency in concurrency_list:
            try:
                # run trial for this concurrency
                config["concurrency"] = concurrency

                # make directory for this specific concurrency, unique by
                # timestamp
                specific_logs_dir = os.path.join(lt_logs_dir, "{0}_{1}".format(
                    str(concurrency),
                    datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")))

                # run trial
                os.makedirs(specific_logs_dir)
                results_fpath_csv = run_single_data_point.run(
                    restore_data, config,
                    specific_logs_dir,
                    write_cicada_log=False)
                restore_data = False

                # gather data from this run
                datum = {"concurrency": concurrency}
                more_data = csv_utils.read_in_data(results_fpath_csv)
                datum.update(*more_data)
                data.append(datum)
            except BaseException as err:
                print("jenndebug latency throughput move on", err)

        # find max throughput and hone in on it
        max_throughput_concurrency = \
            max(data, key=operator.itemgetter("ops/sec(cum)"))["concurrency"]
        concurrency = last_adjustments(max_throughput_concurrency)
        start = int(concurrency - step_size)
        end = int(concurrency + step_size)
        step_size = int(step_size / 2)

    # checkpoint_csv_fpath, and also write out csv values every round of
    # honing in
    insert_csv_data(data, checkpoint_csv_fpath)

    # plot the latency throughput graphs
    plot_utils.gnuplot(LT_GNUPLOT_EXE, checkpoint_csv_fpath,
                       os.path.join(lt_dir, "p50_lt.png"),
                       os.path.join(lt_dir, "p95_lt.png"),
                       os.path.join(lt_dir, "p99_lt.png"))

    return checkpoint_csv_fpath


def find_optimal_concurrency(lt_fpath_csv):
    data = csv_utils.read_in_data(lt_fpath_csv)
    max_throughput_concurrency = \
        max(data, key=operator.itemgetter("ops/sec(cum)"))["concurrency"]
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
