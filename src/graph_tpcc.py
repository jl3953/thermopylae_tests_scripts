import argparse
import os
import sys

import csv_utils
import plot_utils
import sqlite_helper_object


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_file", type=str, required=True, help="trials.db")
    parser.add_argument("--csv_dir", type=str, required=True)
    parser.add_argument("--graph_dir", type=str, required=True)
    parser.add_argument("--suffix", type=str, default="", help="add suffix to all generated files")
    args = parser.parse_args()

    # Query data
    db = sqlite_helper_object.SQLiteHelperObject(args.db_file)
    db.connect()
    data = []
    c = db.c
    for row in c.execute("SELECT ops_per_sec_cum, p50_ms, p99_ms, warehouses "
                         "FROM trials_table "
                         "ORDER BY warehouses ASC "):
        data.append({
            "ops/sec(cum)": row[0],
            "p50(ms)": row[1],
            "p99(ms)": row[2],
            "warehouses": row[3],
        })
    db.close()

    # sort and write out data
    csv_file = csv_utils.write_out_data(data,
                                        os.path.join(args.csv_dir, "dat_{}.csv".format(args.suffix)))

    # graph data
    plot_utils.gnuplot("src/tpcc.gp", csv_file,
                       os.path.join(args.graph_dir, "p50_v_skew_{}.png".format(args.suffix)),
                       os.path.join(args.graph_dir, "tp_v_skew_{}.png".format(args.suffix)),
                       os.path.join(args.graph_dir, "p99_v_skew_{}.png".format(args.suffix)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
