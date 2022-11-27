import argparse
import csv
import os
import sys

import latency_throughput
import plot_utils


def recover_lt_from_logs(ltcsv_dir, dirs):
    """ Reads separate latency throughput logs and re-generates the graph.

    Args:
        ltcsv_dir location of newly generated lt.csv
        *args list of directories

    Returns:
        None.
    """

    result_files = deduce_result_files(dirs)
    print("result files: {0}".format(result_files))

    ltcsv = extract_data_points_into_ltcsv(ltcsv_dir, result_files)
    print("lt.csv: {0}".format(ltcsv))

    plot_utils.gnuplot(latency_throughput.LT_GNUPLOT_EXE, ltcsv,
                       os.path.join(ltcsv_dir, "p50_lt.png"),
                       os.path.join(ltcsv_dir, "p95_lt.png"),
                       os.path.join(ltcsv_dir, "p99_lt.png"))
    print("plotted")


def extract_data_points_into_ltcsv(ltcsv_dir, result_files):
    """Generates a lt.csv from list of result files.

    Args:
        ltcsv_dir location of newly generated lt.csv
        *args a list of result files.

    Returns:
        A list of dictionaries of all relevant data points.
    """

    data_points = []

    for result_file in result_files:
        with open(result_file) as rf:
            reader = csv.DictReader(result_file)
            for row in reader:
                # of which there is only one
                print(row)
                data_points.append(row)

    ltcsv = os.path.join(ltcsv_dir, "lt.csv")
    # print(data_points)
    with open(ltcsv, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data_points[0].keys())

        writer.writeheader()
        writer.writerows(data_points)

    return ltcsv


def deduce_result_files(dirs):
    """ Reconstructs a list of all relevant result files.

    Args:
        *args a list of directories that contain result files

    Returns:
        A list of all relevant result files.
    """
    all_result_files = []
    for directory in dirs:
        result_files = deduce_result_files_from_single_dir(directory)
        all_result_files += result_files

    return all_result_files


def deduce_result_files_from_single_dir(directory):
    """ Deduces what the result files from the latency throughput runs were.

    Args:
    directory: keys10_skew0.99_3nodes/latency_throughput/logs

    Returns:
         A list of result files.
    """

    result_files = []

    # iterate over files in that directory
    for lt_point_dirs in os.listdir(directory):

        # lt_point_dirs = 112_datetime, 3_datetime
        lt_point_dir = os.path.join(directory, lt_point_dirs)

        # checking if it is a directory
        if os.path.isdir(lt_point_dir):
            result = os.path.join(lt_point_dir, "results.csv")
            if os.path.isfile(result):
                result_files.append(result)

    return result_files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dirs", nargs="+", default=[], required=True,
                        help="A list of directories containing log files")
    parser.add_argument("--ltdir", type=str, required=True,
                        help="New location of lt.csv")

    args = parser.parse_args()
    print("ltdir: {0}, dirs: {1}", args.ltdir, args.dirs)

    recover_lt_from_logs(args.ltdir, args.dirs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
