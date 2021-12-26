import argparse
import sys

import populate_crdb_data as populate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--location_of_file", type=str, required=True,
        help="location to write file"
    )
    parser.add_argument(
        "--range_max", type=int, required=True, help="max number to write"
    )
    parser.add_argument(
        "--range_min", type=int, required=True, help="min number to write"
    )
    args = parser.parse_args()

    populate.write_keyspace_to_file(
        args.location_of_file, args.range_max, args.range_min
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
