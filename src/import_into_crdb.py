import argparse
import sys

import populate_crdb_data as populate


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--server", type=str, required=True, help="server on which to execute"
    )
    parser.add_argument(
        "--range_max", type=int, required=True, help="max number to write"
    )
    parser.add_argument(
        "--range_min", type=int, required=True, help="min number to write"
    )
    args = parser.parse_args()

    nfs_locations = [
        "populate1B._{0}.csv.gz".format(i) for i
        in range(
            args.range_min, args.range_max
        )]

    populate.import_into_crdb(args.server, nfs_locations)

    return 0


if __name__ == "__main__":
    sys.exit(main())
